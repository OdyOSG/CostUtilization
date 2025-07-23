#' Standardize Costs for Inflation
#'
#' @description
#' Adjusts the cost values in a CostCovariateData object to a standard year's currency value
#' using an inflation adjustment table.
#'
#' @param costCovariateData A CostCovariateData object, requires person-level data.
#' @param inflationTable A data frame with 'year' and 'adjustment_factor' columns. The factor
#'                       should represent how to multiply a cost from that 'year' to get to the
#'                       standard year's value.
#' @param cdmDatabaseSchema Schema name where your patient-level data resides.
#' @param connection Database connection.
#'
#' @return A new CostCovariateData object with adjusted costs.
#' @export
standardizeCost <- function(costCovariateData, inflationTable, connection, cdmDatabaseSchema) {
  if (costCovariateData$metaData$aggregated) {
    stop("Cost standardization requires person-level data. Please run getDbCostData with aggregated = FALSE.")
  }
  
  # Get the year of the cohort start for each person
  sql <- "SELECT subject_id, YEAR(cohort_start_date) as cohort_year
          FROM @cohort_database_schema.@cohort_table
          WHERE cohort_definition_id = @cohort_id;"
  sql <- SqlRender::render(sql,
                           cohort_database_schema = costCovariateData$metaData$cohortDatabaseSchema,
                           cohort_table = costCovariateData$metaData$cohortTable,
                           cohort_id = costCovariateData$metaData$cohortId)
  personYears <- DatabaseConnector::querySql(connection, sql) %>%
    rename(personId = .data$SUBJECT_ID, year = .data$COHORT_YEAR)
  
  # Join with inflation data and adjust costs
  adjustedCovariates <- costCovariateData$covariates %>%
    dplyr::inner_join(personYears, by = "personId") %>%
    dplyr::inner_join(inflationTable, by = "year") %>%
    dplyr::mutate(covariateValue = .data$covariateValue * .data$adjustment_factor) %>%
    dplyr::select(names(costCovariateData$covariates))
  
  costCovariateData$covariates <- adjustedCovariates
  costCovariateData$metaData$isStandardized <- TRUE
  costCovariateData$metaData$standardizationDate <- Sys.time()
  
  return(costCovariateData)
}