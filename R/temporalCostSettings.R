#' Create temporal covariate settings for cost analysis
#'
#' @description
#' Creates settings for extracting cost-based temporal covariates. These settings
#' can be used to analyze costs within specific time windows relative to cohort
#' start dates.
#'
#' @param useCostInWindow Logical indicating whether to extract costs within time windows
#' @param temporalStartDays A vector of integers specifying the start of time windows
#'   (days relative to cohort start date). Negative values indicate days before.
#' @param temporalEndDays A vector of integers specifying the end of time windows
#'   (days relative to cohort start date). Must be same length as temporalStartDays.
#' @param costDomainIds A vector of strings specifying which cost domains to include
#'   (e.g., "Drug", "Procedure", "Visit"). If NULL, includes all domains.
#' @param costConceptIds A vector of concept IDs to filter specific cost concepts.
#'   If NULL, includes all cost concepts.
#' @param costTypeConceptIds A vector of concept IDs specifying cost types to include
#'   (e.g., allowed amount, paid amount). If NULL, includes all types.
#' @param currencyConceptIds A vector of concept IDs for specific currencies.
#'   If NULL, includes all currencies.
#' @param aggregateMethod Method for aggregating costs: "sum", "mean", "median", "min", "max"
#' @param includeCostConceptId Logical indicating whether to create separate covariates
#'   for each cost concept ID.
#' @param includeCostTypeConceptId Logical indicating whether to create separate covariates
#'   for each cost type concept ID.
#' @param includeCostDomainId Logical indicating whether to create separate covariates
#'   for each cost domain.
#' @param covariateIdOffset An integer offset for generating unique covariate IDs.
#'
#' @return
#' An object of type `costTemporalCovariateSettings`.
#'
#' @export
costTemporalCovariateSettings <- function(useCostInWindow = TRUE,
                                          temporalStartDays = c(-365, -180, -30, 0),
                                          temporalEndDays = c(-1, -1, -1, 0),
                                          costDomainIds = NULL,
                                          costConceptIds = NULL,
                                          costTypeConceptIds = NULL,
                                          currencyConceptIds = NULL,
                                          aggregateMethod = "sum",
                                          includeCostConceptId = FALSE,
                                          includeCostTypeConceptId = FALSE,
                                          includeCostDomainId = TRUE,
                                          covariateIdOffset = 1000) {
  
  # --- Input Validation (using checkmate) ---
  assertFlag(useCostInWindow)
  assertIntegerish(temporalStartDays)
  assertIntegerish(temporalEndDays)
  
  # Corrected function name: test_set_equal
  assert(test_set_equal(length(temporalStartDays), length(temporalEndDays)),
         "The 'temporalStartDays' and 'temporalEndDays' vectors must have the same length.",
         .var.name = "temporal window lengths")
  
  # Corrected function name: test_true
  assert(test_true(all(temporalStartDays <= temporalEndDays)),
         "All start days must be less than or equal to their corresponding end days.",
         .var.name = "temporal window validity")
  
  assertCharacter(costDomainIds, null.ok = TRUE)
  assertIntegerish(costConceptIds, null.ok = TRUE)
  assertIntegerish(costTypeConceptIds, null.ok = TRUE)
  assertIntegerish(currencyConceptIds, null.ok = TRUE)
  assertChoice(aggregateMethod, choices = c("sum", "mean", "median", "min", "max"))
  
  assertFlag(includeCostConceptId)
  assertFlag(includeCostTypeConceptId)
  assertFlag(includeCostDomainId)
  
  assertCount(covariateIdOffset)
  
  
  # --- Create the settings object ---
  settings <- list(
    useCostInWindow = useCostInWindow,
    temporalStartDays = temporalStartDays,
    temporalEndDays = temporalEndDays,
    costDomainIds = costDomainIds,
    costConceptIds = costConceptIds,
    costTypeConceptIds = costTypeConceptIds,
    currencyConceptIds = currencyConceptIds,
    aggregateMethod = aggregateMethod,
    includeCostConceptId = includeCostConceptId,
    includeCostTypeConceptId = includeCostTypeConceptId,
    includeCostDomainId = includeCostDomainId,
    covariateIdOffset = as.integer(covariateIdOffset)
  )
  
  class(settings) <- "costTemporalCovariateSettings"
  return(settings)
}