#' Get Cost Covariate Data
#'
#' Computes cost covariates. This function can calculate costs for a single
#' cohort or aggregate costs across multiple cohorts.
#'
#' @param costCovariateSettings A settings object from `createCostCovariateSettings`.
#' @param cohortId A numeric vector of one or more cohort IDs to analyze. IDs only in aggregated option
#' @param aggregateCohortId If providing multiple `cohortId`s, you must provide a
#'   single numeric ID to label the aggregated output.
#' @param cdm A cdm_reference object created by `CDMConnector::cdm_from_con()`.
#' @param connection A `DatabaseConnector` or `DBI` connection object.
#' @param connectionDetails A `DatabaseConnector` connectionDetails object.
#' @param cdmDatabaseSchema The schema holding the OMOP CDM data.
#' @param cohortDatabaseSchema The schema where the cohort table is located.
#' @param cohortTable The name of the cohort table.
#'
#' @return A FeatureExtraction-compatible data frame with the computed covariates.
#' @export
getCostCovariateData <- function(costCovariateSettings,
                                 cohortId,
                                 aggregateCohortId = NULL,
                                 cdm = NULL,
                                 connection = NULL,
                                 connectionDetails = NULL,
                                 cdmDatabaseSchema = NULL,
                                 cohortDatabaseSchema = NULL,
                                 cohortTable = "cohort") {
  
  # --- Input Validation and Connection Management ---
  checkmate::assertDataFrame(costCovariateSettings)
  checkmate::assertNumeric(cohortId, min.len = 1)
  checkmate::assertInt(aggregateCohortId, null.ok = TRUE)
  
  # Check for aggregation logic
  useAggregation <- length(cohortId) > 1
  if (useAggregation && is.null(aggregateCohortId)) {
    stop("When providing multiple cohortIds, you must also provide `aggregateCohortId`.")
  }
  outputCohortId <- if (useAggregation) aggregateCohortId else cohortId[1]
  
  # Establish connection and schemas from inputs
  if (!is.null(cdm)) {
    checkmate::assertClass(cdm, "cdm_reference")
    connection <- CDMConnector::cdmCon(cdm) 
    cdmDatabaseSchema <- CDMConnector::cdmSchema(cdm)
    cohortDatabaseSchema <- CDMConnector::cdmWriteSchema(cdm)
  } else if (!is.null(connectionDetails)) {
    checkmate::assertClass(connectionDetails, "connectionDetails")
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  } else if (!is.null(connection)) {
    checkmate::assertClass(connection, "DBIConnection")
  } else {
    stop("Must provide one of 'cdm', 'connectionDetails', or 'connection'.")
  }
  
  if (is.null(cdmDatabaseSchema) || is.null(cohortDatabaseSchema)) {
    stop("cdmDatabaseSchema and cohortDatabaseSchema must be specified if not using a cdm object.")
  }
  
  # --- Analysis Execution ---
  assertCdmConforms(connection = connection, cdmDatabaseSchema = cdmDatabaseSchema)
  renderedSql <- SqlRender::loadRenderTranslateSql(
    'getCostCovariates.sql',
    'CostUtilization',
    dbms = "sql server",
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortId, # Changed from cohort_id to cohort_ids
    output_cohort_id = outputCohortId, # New parameter for labeling
    covariate_id = costCovariateSettings$covariateId,
    cost_concept_id = costCovariateSettings$costConceptId[[1]],
    cost_type_concept_id = costCovariateSettings$costTypeConceptId[[1]],
    filter_by_event_concepts = !is.null(costCovariateSettings$conceptIds[[1]]) && length(costCovariateSettings$conceptIds[[1]]) > 0,
    event_concept_ids = costCovariateSettings$conceptIds[[1]],
    start_day = costCovariateSettings$startDay,
    end_day = costCovariateSettings$endDay
  )
  
  covariates <- DatabaseConnector::querySql(connection, renderedSql, snakeCaseToCamelCase = TRUE)
  
  # Format output
  featureExtractionCovs <- covariates %>%
    dplyr::select(
      cohortDefinitionId = .data$outputCohortId,
      subjectId = .data$subjectId,
      covariateId = .data.covariateId,
      covariateValue = .data$sumValue
    ) %>%
    dplyr::mutate(timeId = NA) %>%
    dplyr::as_tibble()
  
  return(featureExtractionCovs)
}