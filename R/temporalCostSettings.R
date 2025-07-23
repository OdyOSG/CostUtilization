# R/CostTemporalCovariateSettings.R

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
#'   for each cost concept ID
#' @param includeCostTypeConceptId Logical indicating whether to create separate covariates
#'   for each cost type concept ID
#' @param includeCostDomainId Logical indicating whether to create separate covariates
#'   for each cost domain
#' @param includeRevenueCodes Logical indicating whether to include revenue code analysis
#' @param includeDrgCodes Logical indicating whether to include DRG code analysis
#' @param covariateIdOffset An integer offset for generating unique covariate IDs
#'
#' @return
#' An object of type \code{costTemporalCovariateSettings}, to be used with other
#' functions in the CostUtilization package.
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
  
  # Input validation
  if (length(temporalStartDays) != length(temporalEndDays)) {
    stop("temporalStartDays and temporalEndDays must have the same length")
  }
  
  if (any(temporalStartDays > temporalEndDays)) {
    stop("temporalStartDays must be less than or equal to temporalEndDays")
  }
  
  if (!aggregateMethod %in% c("sum", "mean", "median", "min", "max")) {
    stop("aggregateMethod must be one of: sum, mean, median, min, max")
  }
  
  # Create the settings object
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
    includeRevenueCodes = includeRevenueCodes,
    includeDrgCodes = includeDrgCodes,
    covariateIdOffset = as.integer(covariateIdOffset)
  )
  
  class(settings) <- "costTemporalCovariateSettings"
  return(settings)
}
