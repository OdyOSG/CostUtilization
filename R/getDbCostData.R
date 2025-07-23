#' Get cost data from database
#'
#' @description
#' Extracts cost covariate data from a CDM database based on the provided settings.
#' This function uses the Andromeda package to store data on disk, making it memory-efficient.
#'
#' @param connectionDetails An S3 object of type `connectionDetails` as created by the
#'   `DatabaseConnector::createConnectionDetails()` function.
#' @param cdmDatabaseSchema A character string specifying the schema where the OMOP CDM data resides.
#' @param cohortTable A character string specifying the name of the cohort table.
#' @param cohortDatabaseSchema A character string specifying the schema where the cohort table resides.
#'   If not provided, it defaults to the `cdmDatabaseSchema`.
#' @param cohortId An integer specifying the cohort definition ID to use.
#' @param costCovariateSettings An S3 object of type `costCovariateSettings` as created by one of the
#'   settings functions in this package.
#' @param aggregated A logical flag indicating whether to compute population-level aggregate statistics
#'   (`TRUE`) or return person-level data (`FALSE`).
#' @param tempEmulationSchema (Optional) A character string for a schema where temp tables can be created
#'   for platforms that do not truly support temp tables (e.g., Oracle, Impala).
#'
#' @return
#' A `CostCovariateData` object. This object is a pointer to data stored on disk.
#' Remember to close it using `Andromeda::close()` when you are finished.
#'
#' @export
getDbCostData <- function(connectionDetails,
                          cdmDatabaseSchema,
                          cohortTable = "cohort",
                          cohortDatabaseSchema = cdmDatabaseSchema,
                          cohortId,
                          costCovariateSettings,
                          aggregated = FALSE,
                          tempEmulationSchema = NULL) {
  
  checkmate::assertClass(connectionDetails, "connectionDetails")
  checkmate::assertString(cdmDatabaseSchema)
  checkmate::assertString(cohortTable)
  checkmate::assertString(cohortDatabaseSchema)
  checkmate::assertCount(cohortId)
  checkmate::assertClass(costCovariateSettings, "costCovariateSettings")
  checkmate::assertFlag(aggregated)
  checkmate::assertString(tempEmulationSchema, null.ok = TRUE)
  
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  startTime <- Sys.time()
  andromeda <- Andromeda::andromeda()
  on.exit(Andromeda::close(andromeda), add = TRUE, after = FALSE)
  
  references <- generateReferenceTables(costCovariateSettings)
  andromeda$analysisRef <- references$analysisRef
  andromeda$covariateRef <- references$covariateRef
  
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "#temporal_windows",
    data = references$temporalWindows,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )
  
  sql <- generateQuerySql(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortId = cohortId,
    costCovariateSettings = costCovariateSettings,
    aggregated = aggregated,
    tempEmulationSchema = tempEmulationSchema
  )
  
  ParallelLogger::logInfo("Extracting cost covariates into Andromeda...")
  resultTableName <- if (aggregated) "aggregatedCovariates" else "covariates"
  
  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = sql,
    andromeda = andromeda,
    andromedaTableName = resultTableName,
    snakeCaseToCamelCase = TRUE
  )
  
  metaData <- list(
    call = match.call(),
    cohortId = cohortId,
    costCovariateSettings = costCovariateSettings,
    aggregated = aggregated,
    startTime = startTime,
    endTime = Sys.time()
  )
  attr(andromeda, "metaData") <- metaData
  class(andromeda) <- "CostCovariateData"
  
  ParallelLogger::logInfo("Extraction complete.")
  return(andromeda)
}

#' Internal function to generate SQL query
#' @keywords internal
generateQuerySql <- function(connection, cdmDatabaseSchema, cohortTable, cohortDatabaseSchema, cohortId, costCovariateSettings, aggregated, tempEmulationSchema) {
  sqlFileName <- if (aggregated) {
    "GetAggregatedCostCovariates.sql"
  } else {
    "GetCostCovariates.sql"
  }
  
  SqlRender::loadRenderTranslateSql(
    sqlFilename = sqlFileName,
    packageName = "CostUtilization",
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_id = cohortId,
    cost_type_concept_ids = costCovariateSettings$filters$costTypeConceptIds,
    cost_domain_ids = costCovariateSettings$filters$costDomains,
    currency_concept_ids = costCovariateSettings$filters$currencyConceptIds,
    use_total_cost = costCovariateSettings$analyses$totalCost,
    use_cost_by_domain = costCovariateSettings$analyses$costByDomain,
    use_cost_by_type = costCovariateSettings$analyses$costByType,
    use_utilization = costCovariateSettings$analyses$utilization
  )
}

#' Internal function to generate reference tables
#' @keywords internal
generateReferenceTables <- function(costCovariateSettings) {
  domains <- dplyr::tibble(
    domainIdValue = 1:7,
    costDomainId = c('Drug', 'Visit', 'Procedure', 'Device', 'Measurement', 'Observation', 'Specimen')
  )
  
  temporalWindows <- costCovariateSettings$temporalWindows %>%
    dplyr::mutate(windowId = dplyr::row_number())
  
  analysisRef <- dplyr::tibble()
  covariateRef <- dplyr::tibble()
  
  # Analysis 1: Total Cost
  if (costCovariateSettings$analyses$totalCost) {
    analysisId <- 1
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Total cost", isBinary = "N", missingMeansZero = "Y")
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
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Cost by domain", isBinary = "N", missingMeansZero = "Y")
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
  # Note: This is more complex as cost_type_concept_ids are not known beforehand.
  # The SQL generates covariate_ids like 3000 + (window_id * 100) + (cost_type_concept_id %% 100)
  # A complete reference would require querying the concept table. For now, we generate a basic analysis ref.
  if (costCovariateSettings$analyses$costByType) {
    analysisId <- 3
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Cost by type", isBinary = "N", missingMeansZero = "Y")
    # Covariate names would be like: "Cost type X during day Y to Z". Dynamic creation is complex without DB access here.
  }
  
  # Analysis 4: Utilization
  if (costCovariateSettings$analyses$utilization) {
    analysisId <- 4
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Utilization", isBinary = "N", missingMeansZero = "Y")
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      temporalWindows %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 4000 + .data$windowId,
          covariateName = paste0("Utilization days during day ", .data$startDay, " to ", .data.endDay),
          conceptId = 0
        )
    )
  }
  
  return(list(
    analysisRef = analysisRef %>% dplyr::select(.data$analysisId, .data$analysisName, .data$isBinary, .data$missingMeansZero),
    covariateRef = covariateRef %>% dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId),
    temporalWindows = temporalWindows %>% dplyr::select(.data$windowId, .data$startDay, .data$endDay)
  ))
}