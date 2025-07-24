#' Create Settings for Cost and Utilization Covariates
#'
#' @description
#' Creates a settings object for use with the `FeatureExtraction::getDbCovariateData`
#' function. This object specifies how cost and utilization covariates should be constructed.
#'
#' @param temporalStartDays           A numeric vector of integers specifying the start days of temporal windows
#'                                    relative to the cohort index date.
#' @param temporalEndDays             A numeric vector of integers specifying the end days of temporal windows
#'                                    relative to the cohort index date. Must be the same length as `temporalStartDays`.
#' @param totalCost                   A logical flag indicating whether to compute total cost across all domains.
#' @param costByDomain                A logical flag indicating whether to compute costs stratified by CDM domain (e.g., 'Drug', 'Visit').
#' @param costByType                  A logical flag indicating whether to compute costs stratified by the cost_type_concept_id.
#' @param utilization                 A logical flag indicating whether to compute healthcare utilization (count of distinct cost days).
#' @param costDomains                 A character vector of specific cost domains to include (e.g., c("Drug", "Visit")).
#'                                    If NULL, all domains are included.
#' @param costTypeConceptIds          A numeric vector of `cost_type_concept_id`s to include. If NULL, all types are included.
#' @param currencyConceptIds          A numeric vector of `currency_concept_id`s to include. If NULL, all currencies are included.
#' @param includedCovariateConceptIds A vector of concept IDs for which to calculate costs. The costs will be for events
#'                                    where the associated clinical concept is in this set.
#' @param addDescendantsToInclude     Should descendants of the `includedCovariateConceptIds` be included?
#' @param excludedCovariateConceptIds A vector of concept IDs to exclude from cost calculations.
#' @param addDescendantsToExclude     Should descendants of the `excludedCovariateConceptIds` be excluded?
#'
#' @return
#' An S3 object of class `costCovariateSettings`.
#'
#' @export
createCostCovariateSettings <- function(temporalStartDays = c(-365, -1, 0, 30),
                                        temporalEndDays = c(-1, -1, 0, 365),
                                        totalCost = TRUE,
                                        costByDomain = TRUE,
                                        costByType = FALSE,
                                        utilization = TRUE,
                                        costDomains = NULL,
                                        costTypeConceptIds = NULL,
                                        currencyConceptIds = NULL,
                                        includedCovariateConceptIds = c(),
                                        addDescendantsToInclude = FALSE,
                                        excludedCovariateConceptIds = c(),
                                        addDescendantsToExclude = FALSE) {
  # --- Input Validation ---
  checkmate::assertIntegerish(temporalStartDays, any.missing = FALSE)
  checkmate::assertIntegerish(temporalEndDays, any.missing = FALSE)

  checkmate::assertFlag(totalCost)
  checkmate::assertFlag(costByDomain)
  checkmate::assertFlag(costByType)
  checkmate::assertFlag(utilization)

  checkmate::assertCharacter(costDomains, null.ok = TRUE, unique = TRUE, any.missing = FALSE)
  checkmate::assertIntegerish(costTypeConceptIds, null.ok = TRUE, unique = TRUE, any.missing = FALSE)
  checkmate::assertIntegerish(currencyConceptIds, null.ok = TRUE, unique = TRUE, any.missing = FALSE)

  checkmate::assertIntegerish(includedCovariateConceptIds, any.missing = FALSE, unique = TRUE)
  checkmate::assertFlag(addDescendantsToInclude)
  checkmate::assertIntegerish(excludedCovariateConceptIds, any.missing = FALSE, unique = TRUE)
  checkmate::assertFlag(addDescendantsToExclude)

  # Cross-parameter validation
  if (length(temporalStartDays) != length(temporalEndDays)) {
    stop("The 'temporalStartDays' and 'temporalEndDays' vectors must have the same length.")
  }
  if (any(temporalStartDays > temporalEndDays)) {
    stop("All start days must be less than or equal to their corresponding end days.")
  }

  settings <- list(
    temporalWindows = data.frame(
      startDay = as.integer(temporalStartDays),
      endDay = as.integer(temporalEndDays)
    ),
    analyses = list(totalCost = totalCost, costByDomain = costByDomain, costByType = costByType, utilization = utilization),
    filters = list(
      costDomains = costDomains,
      costTypeConceptIds = costTypeConceptIds,
      currencyConceptIds = currencyConceptIds,
      includedCovariateConceptIds = includedCovariateConceptIds,
      addDescendantsToInclude = addDescendantsToInclude,
      excludedCovariateConceptIds = excludedCovariateConceptIds,
      addDescendantsToExclude = addDescendantsToExclude
    )
  )

  class(settings) <- "costCovariateSettings"
  return(settings)
}


#' Print Cost Covariate Settings
#'
#' @param x A costCovariateSettings object.
#' @param ... Additional arguments (not used).
#'
#' @return Invisibly returns the input object.
#' @export
print.costCovariateSettings <- function(x, ...) {
  writeLines("Cost & Utilization Covariate Settings")
  writeLines(paste(rep("-", 40), collapse = ""))

  # --- Analyses ---
  writeLines("\nAnalyses to be generated:")
  writeLines(paste("  - Total Cost:", x$analyses$totalCost))
  writeLines(paste("  - Cost by Domain:", x$analyses$costByDomain))
  writeLines(paste("  - Cost by Type:", x$analyses$costByType))
  writeLines(paste("  - Utilization:", x$analyses$utilization))

  # --- Temporal Windows ---
  writeLines("\nTemporal Windows:")
  if (nrow(x$temporalWindows) > 0) {
    for (i in 1:nrow(x$temporalWindows)) {
      writeLines(paste0("  - Window ", i, ": [", x$temporalWindows$startDay[i], ", ", x$temporalWindows$endDay[i], "]"))
    }
  } else {
    writeLines("  - None specified")
  }

  # --- General Filters ---
  writeLines("\nGeneral Filters:")
  writeLines(paste(
    "  - Cost Domains:",
    if (is.null(x$filters$costDomains) || length(x$filters$costDomains) == 0) {
      "All"
    } else {
      paste(x$filters$costDomains, collapse = ", ")
    }
  ))
  writeLines(paste(
    "  - Cost Type Concepts:",
    if (is.null(x$filters$costTypeConceptIds) || length(x$filters$costTypeConceptIds) == 0) {
      "All"
    } else {
      paste(x$filters$costTypeConceptIds, collapse = ", ")
    }
  ))
  writeLines(paste(
    "  - Currency Concepts:",
    if (is.null(x$filters$currencyConceptIds) || length(x$filters$currencyConceptIds) == 0) {
      "All"
    } else {
      paste(x$filters$currencyConceptIds, collapse = ", ")
    }
  ))

  # --- Concept Set Filters ---
  writeLines("\nConcept Set Filters:")
  if (length(x$filters$includedCovariateConceptIds) > 0) {
    writeLines(paste0(
      "  - Included Concepts: ",
      length(x$filters$includedCovariateConceptIds),
      " IDs (Add Descendants: ", x$filters$addDescendantsToInclude, ")"
    ))
  } else {
    writeLines("  - Included Concepts: None")
  }

  if (length(x$filters$excludedCovariateConceptIds) > 0) {
    writeLines(paste0(
      "  - Excluded Concepts: ",
      length(x$filters$excludedCovariateConceptIds),
      " IDs (Add Descendants: ", x$filters$addDescendantsToExclude, ")"
    ))
  } else {
    writeLines("  - Excluded Concepts: None")
  }

  invisible(x)
}
