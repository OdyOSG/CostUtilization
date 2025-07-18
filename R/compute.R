#' Get Cost Covariate Data
#'
#' Computes the cost covariates for a specified cohort based on the provided settings.
#'
#' @param connection A connection object from the `DatabaseConnector` package.
#' @param cdmDatabaseSchema The schema holding the OMOP CDM data.
#' @param cohortDatabaseSchema The schema where the cohort table is located.
#' @param cohortTable The name of the cohort table.
#' @param cohortId The ID of the cohort to analyze.
#' @param costCovariateSettings A settings object created by `createCostCovariateSettings`.
#'
#' @return A FeatureExtraction-compatible data frame with the computed covariates.
#' @export
getCostCovariateData <- function(connection,
                                 cdmDatabaseSchema,
                                 cohortDatabaseSchema,
                                 cohortTable,
                                 cohortId,
                                 costCovariateSettings) {
  # Assertions and setup
  checkmate::assertClass(connection, "DatabaseConnectorConnection")
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1)
  checkmate::assertCharacter(cohortDatabaseSchema, len = 1) # New assertion
  checkmate::assertCharacter(cohortTable, len = 1)
  checkmate::assertInt(cohortId)
  checkmate::assertDataFrame(costCovariateSettings)
  
  # Run Data Quality Checks
  assertCdmConforms(connection = connection, cdmDatabaseSchema = cdmDatabaseSchema)
  
  # Render and execute SQL
  sql <- SqlRender::readSql(system.file("sql/getCostCovariates.sql", package = "CostUtilization"))
  
  # The logic described in the design document is implemented in the SQL query
  renderedSql <- SqlRender::render(
    sql,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema, # New parameter
    cohort_table = cohortTable,
    cohort_id = cohortId,
    covariate_id = costCovariateSettings$covariateId,
    cost_concept_id = costCovariateSettings$costConceptId[[1]],
    cost_type_concept_id = costCovariateSettings$costTypeConceptId[[1]],
    filter_by_event_concepts = !is.null(costCovariateSettings$conceptIds[[1]]) && length(costCovariateSettings$conceptIds[[1]]) > 0,
    event_concept_ids = costCovariateSettings$conceptIds[[1]],
    start_day = costCovariateSettings$startDay,
    end_day = costCovariateSettings$endDay
  )
  
  covariates <- DatabaseConnector::querySql(connection, renderedSql, snakeCaseToCamelCase = TRUE)
  
  # Format output to conform to FeatureExtraction model
  featureExtractionCovs <- covariates %>%
    select(
      cohortDefinitionId = .data$cohortId,
      subjectId = .data.subjectId,
      covariateId = .data$covariateId,
      covariateValue = .data$sumValue
    ) %>%
    mutate(timeId = NA) %>%
    as_tibble()
  
  return(featureExtractionCovs)
}