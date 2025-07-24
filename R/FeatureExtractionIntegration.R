# R/FeatureExtractionIntegration.R

#' Construct Cost and Utilization Covariates
#'
#' @description
#' This is an S3 method for the `FeatureExtraction::getDbCovariateData` generic.
#' It is called when the `covariateSettings` object is of type `costCovariateSettings`.
#' This function constructs aggregated or person-level cost and healthcare utilization covariates.
#'
#' @details
#' This function is not intended to be called directly, but is automatically dispatched
#' by `FeatureExtraction::getDbCovariateData`. It generates covariates for the specified
#' cohort based on settings created by `createCostCovariateSettings`.
#' When `aggregated = TRUE`, it produces summary statistics.
#' When `aggregated = FALSE`, it produces person-level data, which requires a unique
#' `rowIdField` in the cohort table to distinguish cohort entries.
#'
#' @param connection              A connection to the server containing the schema.
#' @param oracleTempSchema        DEPRECATED. Use `tempEmulationSchema` instead.
#' @param cdmDatabaseSchema       The name of the database schema that contains the OMOP CDM instance.
#' @param cohortTable             Name of the table holding the cohort.
#' @param cohortId                The ID of the cohort for which to construct covariates.
#' @param cdmVersion              The version of the OMOP CDM. Currently, only "5" is supported.
#' @param rowIdField              The name of the field in the cohort table that is to be used as the
#'                                `row_id` field. Crucial for person-level data.
#' @param covariateSettings       An S3 object of type `costCovariateSettings`.
#' @param aggregated              A logical flag indicating whether to compute population-level aggregate statistics
#'                                (`TRUE`) or person-level data (`FALSE`).
#' @param tempEmulationSchema     A schema with write privileges for emulating temp tables.
#'
#' @return
#' A `CovariateData` object containing the constructed covariates.
#' If `aggregated = TRUE`, results are in `covariatesContinuous`.
#' If `aggregated = FALSE`, results are in `covariates`.
#'
#' @export
getDbCovariateData.costCovariateSettings <- function(connection,
                                                     oracleTempSchema = NULL,
                                                     cdmDatabaseSchema,
                                                     cohortTable,
                                                     cohortId,
                                                     cdmVersion = "5",
                                                     rowIdField,
                                                     covariateSettings,
                                                     aggregated = FALSE,
                                                     tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  # --- Input Validation ---
  checkmate::assertClass(connection, "DBIConnection")
  checkmate::assertString(cdmDatabaseSchema, min.chars = 1)
  checkmate::assertString(cohortTable, min.chars = 1)
  checkmate::assertInt(cohortId, lower = 1)
  checkmate::assertChoice(cdmVersion, choices = "5", .var.name = "cdmVersion (for CostUtilization)")
  checkmate::assertString(rowIdField)
  checkmate::assertClass(covariateSettings, "costCovariateSettings")
  checkmate::assertFlag(aggregated)
  checkmate::assertString(tempEmulationSchema, null.ok = TRUE)

  if (!is.null(oracleTempSchema)) {
    ParallelLogger::logWarn("The 'oracleTempSchema' argument is deprecated. Please use 'tempEmulationSchema' instead.")
  }

  # --- Concept Set Resolution ---
  hasIncludedConcepts <- length(covariateSettings$filters$includedCovariateConceptIds) > 0
  if (hasIncludedConcepts) {
    resolvedIncludes <- resolveConceptSet(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      conceptIds = covariateSettings$filters$includedCovariateConceptIds,
      addDescendants = covariateSettings$filters$addDescendantsToInclude
    )
    DatabaseConnector::insertTable(connection, "#included_concepts", data.frame(concept_id = resolvedIncludes), tempTable = TRUE)
  }

  hasExcludedConcepts <- length(covariateSettings$filters$excludedCovariateConceptIds) > 0
  if (hasExcludedConcepts) {
    resolvedExcludes <- resolveConceptSet(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      conceptIds = covariateSettings$filters$excludedCovariateConceptIds,
      addDescendants = covariateSettings$filters$addDescendantsToExclude
    )
    DatabaseConnector::insertTable(connection, "#excluded_concepts", data.frame(concept_id = resolvedExcludes), tempTable = TRUE)
  }

  # --- Data Extraction ---
  references <- generateReferenceTables(covariateSettings)
  DatabaseConnector::insertTable(
    connection = connection, tableName = "#temporal_windows", data = references$temporalWindows,
    dropTableIfExists = TRUE, createTable = TRUE, tempTable = TRUE, camelCaseToSnakeCase = TRUE
  )

  # Choose SQL script and parameters based on aggregation mode
  if (aggregated) {
    sqlFilename <- "GetAggregatedCostCovariates.sql"
    sqlParameters <- list(row_id_field = rowIdField) # Aggregated script needs this for its temp table
  } else {
    sqlFilename <- "GetCostCovariates.sql"
    sqlParameters <- list(row_id_field = rowIdField)
  }

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = sqlFilename,
    packageName = "CostUtilization",
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cdmDatabaseSchema,
    cohort_table = cohortTable,
    cohort_id = cohortId,
    cost_type_concept_ids = covariateSettings$filters$costTypeConceptIds,
    cost_domain_ids = covariateSettings$filters$costDomains,
    currency_concept_ids = covariateSettings$filters$currencyConceptIds,
    use_total_cost = covariateSettings$analyses$totalCost,
    use_cost_by_domain = covariateSettings$analyses$costByDomain,
    use_cost_by_type = covariateSettings$analyses$costByType,
    use_utilization = covariateSettings$analyses$utilization,
    has_included_concepts = hasIncludedConcepts,
    has_excluded_concepts = hasExcludedConcepts,
    !!!sqlParameters
  )

  ParallelLogger::logInfo(sprintf(
    "Extracting %s cost and utilization data from database",
    ifelse(aggregated, "aggregated", "person-level")
  ))
  covariates <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)

  # --- Cleanup temp concept tables ---
  cleanupSql <- ""
  if (hasIncludedConcepts) {
    cleanupSql <- paste(cleanupSql, "TRUNCATE TABLE #included_concepts; DROP TABLE #included_concepts;")
  }
  if (hasExcludedConcepts) {
    cleanupSql <- paste(cleanupSql, "TRUNCATE TABLE #excluded_concepts; DROP TABLE #excluded_concepts;")
  }
  if (nchar(cleanupSql) > 0) {
    DatabaseConnector::renderTranslateExecuteSql(connection, cleanupSql)
  }


  # --- Format Output ---
  covariateData <- Andromeda::andromeda()

  if (aggregated) {
    if (nrow(covariates) > 0) {
      covariateData$covariatesContinuous <- covariates %>%
        dplyr::mutate(cohortDefinitionId = cohortId) %>%
        dplyr::rename(
          countValue = .data$n,
          minValue = .data$minValue,
          maxValue = .data$maxValue,
          averageValue = .data$meanValue,
          standardDeviation = .data$sdValue
        ) %>%
        dplyr::select(
          .data$cohortDefinitionId, .data$covariateId, .data$countValue,
          .data$minValue, .data$maxValue, .data$averageValue, .data$standardDeviation
        )
    }
  } else {
    if (nrow(covariates) > 0) {
      covariateData$covariates <- covariates %>%
        dplyr::select(.data$rowId, .data$covariateId, .data$covariateValue)
    }
  }

  covariateData$analysisRef <- references$analysisRef
  covariateData$covariateRef <- references$covariateRef

  metaData <- list(
    call = match.call(),
    sql = sql,
    cohortId = cohortId,
    aggregated = aggregated
  )
  attr(covariateData, "metaData") <- metaData
  class(covariateData) <- "CovariateData"

  return(covariateData)
}

#' Internal helper to generate reference tables from settings
#' @keywords internal
generateReferenceTables <- function(costCovariateSettings) {
  domains <- dplyr::tibble(
    domainIdValue = 1:7,
    costDomainId = c("Drug", "Visit", "Procedure", "Device", "Measurement", "Observation", "Specimen")
  )

  temporalWindows <- costCovariateSettings$temporalWindows %>%
    dplyr::mutate(windowId = dplyr::row_number())

  analysisRef <- dplyr::tibble()
  covariateRef <- dplyr::tibble()

  # Analysis 1: Total Cost
  if (costCovariateSettings$analyses$totalCost) {
    analysisId <- 1
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Total cost", domainId = "Cost", isBinary = "N", missingMeansZero = "Y")
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      temporalWindows %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 1000 + .data$windowId,
          covariateName = paste0("Total cost during day ", .data$startDay, " to ", .data$endDay),
          conceptId = 0
        )
    )
  }

  # Analysis 2: Cost By Domain
  if (costCovariateSettings$analyses$costByDomain) {
    analysisId <- 2
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Cost by domain", domainId = "Cost", isBinary = "N", missingMeansZero = "Y")
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      tidyr::crossing(temporalWindows, domains) %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 2000 + (.data$windowId * 10) + .data$domainIdValue,
          covariateName = paste0(.data$costDomainId, " cost during day ", .data$startDay, " to ", .data$endDay),
          conceptId = 0
        )
    )
  }

  # Analysis 3: Cost By Type
  if (costCovariateSettings$analyses$costByType) {
    analysisId <- 3
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Cost by type", domainId = "Cost", isBinary = "N", missingMeansZero = "Y")
    # Covariate names would be like: "Cost type X during day Y to Z". Dynamic creation is complex
    # without DB access here to get concept names. The SQL generates IDs based on concept_id % 100.
    # The user can join with the concept table to get full names if needed.
  }

  # Analysis 4: Utilization
  if (costCovariateSettings$analyses$utilization) {
    analysisId <- 4
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Utilization", domainId = "Cost", isBinary = "N", missingMeansZero = "Y")
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      temporalWindows %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 4000 + .data$windowId,
          covariateName = paste0("Utilization days during day ", .data$startDay, " to ", .data$endDay),
          conceptId = 0
        )
    )
  }

  return(list(
    analysisRef = analysisRef,
    covariateRef = covariateRef %>% dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId),
    temporalWindows = temporalWindows %>% dplyr::select(.data$windowId, .data$startDay, .data$endDay)
  ))
}


#' Internal helper to resolve concept sets with descendants
#' @keywords internal
resolveConceptSet <- function(connection, cdmDatabaseSchema, conceptIds, addDescendants) {
  if (!addDescendants) {
    return(as.numeric(conceptIds))
  }
  sql <- "
    SELECT DISTINCT ancestor_concept_id
    FROM @cdm_database_schema.concept_ancestor
    WHERE descendant_concept_id IN (@concept_ids);
  "
  resolved <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    cdm_database_schema = cdmDatabaseSchema,
    concept_ids = conceptIds,
    snakeCaseToCamelCase = TRUE
  )
  return(as.numeric(resolved$ancestorConceptId))
}
