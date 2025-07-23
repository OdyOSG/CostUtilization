

#' Create Settings for Cost and Utilization Covariates
#'
#' @description
#' Creates a settings object for the `getDbCostData` function, specifying how
#' cost and utilization covariates should be constructed.
#'
#' @param temporalStartDays         A numeric vector of integers specifying the start days of temporal windows
#'                                  relative to the cohort index date.
#' @param temporalEndDays           A numeric vector of integers specifying the end days of temporal windows
#'                                  relative to the cohort index date. Must be the same length as `temporalStartDays`.
#' @param totalCost                 A logical flag indicating whether to compute total cost across all domains.
#' @param costByDomain              A logical flag indicating whether to compute costs stratified by CDM domain (e.g., 'Drug', 'Visit').
#' @param costByType                A logical flag indicating whether to compute costs stratified by the cost_type_concept_id.
#' @param utilization               A logical flag indicating whether to compute healthcare utilization (count of distinct cost days).
#' @param costDomains               A character vector of specific cost domains to include (e.g., c("Drug", "Visit")).
#'                                  If NULL, all domains are included.
#' @param costTypeConceptIds        A numeric vector of `cost_type_concept_id`s to include. If NULL, all types are included.
#' @param currencyConceptIds        A numeric vector of `currency_concept_id`s to include. If NULL, all currencies are included.
#'
#' @return
#' An S3 object of class `costCovariateSettings`.
#'
#' @export
createCostCovariateSettings <- function(temporalStartDays = c(-365, -30, 0, 1, 31),
                                        temporalEndDays = c(-1, -1, 0, 30, 365),
                                        totalCost = TRUE,
                                        costByDomain = TRUE,
                                        costByType = FALSE,
                                        utilization = TRUE,
                                        costDomains = NULL,
                                        costTypeConceptIds = NULL,
                                        currencyConceptIds = NULL) {
  # --- Input validation
  checkmate::assertNumeric(temporalStartDays)
  checkmate::assertNumeric(temporalEndDays)
  if (length(temporalStartDays) != length(temporalEndDays)) {
    stop("Length of 'temporalStartDays' must equal length of 'temporalEndDays'")
  }
  if (any(temporalStartDays > temporalEndDays)) {
    stop("All start days must be less than or equal to their corresponding end days.")
  }
  checkmate::assertFlag(totalCost)
  checkmate::assertFlag(costByDomain)
  checkmate::assertFlag(costByType)
  checkmate::assertFlag(utilization)
  checkmate::assertCharacter(costDomains, null.ok = TRUE, unique = TRUE)
  checkmate::assertIntegerish(costTypeConceptIds, null.ok = TRUE, unique = TRUE)
  checkmate::assertIntegerish(currencyConceptIds, null.ok = TRUE, unique = TRUE)
  
  settings <- list(
    temporalWindows = data.frame(
      startDay = as.integer(temporalStartDays),
      endDay = as.integer(temporalEndDays)
    ),
    analyses = list(
      totalCost = totalCost,
      costByDomain = costByDomain,
      costByType = costByType,
      utilization = utilization
    ),
    filters = list(
      costDomains = costDomains,
      costTypeConceptIds = costTypeConceptIds,
      currencyConceptIds = currencyConceptIds
    )
  )
  
  class(settings) <- "costCovariateSettings"
  return(settings)
}