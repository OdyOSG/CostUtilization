#' Create Cost Covariate Settings
#'
#' This function creates a settings object for calculating cost covariates.
#' It defines the parameters for the cost calculation, such as the target
#' cohort, cost concepts, and optional clinical event filters.
#'
#' @param covariateId A user-defined numeric ID for the resulting covariate[cite: 201, 220].
#' @param costConceptId A vector of OMOP concept IDs representing the cost components to sum (e.g., 'Allowed', 'Paid')[cite: 212].
#' @param costTypeConceptId A vector of OMOP concept IDs for the cost provenance (e.g., 'Claim', 'Charge Master')[cite: 83, 91, 212].
#' @param conceptIds An optional vector of concept IDs used to filter the visits to include in the cost calculation[cite: 210, 222]. If empty, costs for all visits are included[cite: 211, 224].
#' @param startDay The start day relative to the cohort start date.
#' @param endDay The end day relative to the cohort start date.
#'
#' @return A data frame with the specified settings for the cost covariate analysis.
#' @export
createCostCovariateSettings <- function(covariateId,
                                        costConceptId,
                                        costTypeConceptId,
                                        conceptIds = c(),
                                        startDay = 0,
                                        endDay = 365) {
  # Input validation
  checkmate::assertNumeric(covariateId, len = 1)
  checkmate::assertIntegerish(costConceptId, min.len = 1)
  checkmate::assertIntegerish(costTypeConceptId, min.len = 1)
  checkmate::assertIntegerish(conceptIds, null.ok = TRUE)
  checkmate::assertInt(startDay)
  checkmate::assertInt(endDay)
  
  settings <- data.frame(
    covariateId = covariateId,
    costConceptId = I(list(costConceptId)),
    costTypeConceptId = I(list(costTypeConceptId)),
    conceptIds = I(list(conceptIds)),
    startDay = startDay,
    endDay = endDay
  )
  return(settings)
}