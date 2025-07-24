# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of CostUtilization
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Create Unified Cost and Utilization Covariate Settings
#'
#' @description
#' Creates a single, unified settings object for constructing cost and utilization covariates. This function can
#' define analyses using standard "long/medium/short-term" windows, fully custom temporal windows, or a combination of both.
#'
#' @details
#' This function generates a `costCovariateSettings` object that can be passed to the
#' `getDbCovariateData` function.
#'
#' To use standard windows, set flags like `useCostTotalLongTerm = TRUE`.
#' To use custom windows, provide a list to `customTemporalWindows`, e.g., `list(c(-30, -1), c(0, 0))`.
#' Any analyses selected (e.g., `useCostTotalLongTerm = TRUE`) will be applied to all custom windows as well.
#'
#' @param useCostTotalLongTerm             Total cost of all events in the long-term window. (Analysis ID: 1)
#' @param useCostTotalMediumTerm           Total cost of all events in the medium-term window. (Analysis ID: 1)
#' @param useCostTotalShortTerm            Total cost of all events in the short-term window. (Analysis ID: 1)
#' @param useCostByDomainLongTerm          Cost stratified by domain in the long-term window. (Analysis ID: 2)
#' @param useCostByDomainMediumTerm        Cost stratified by domain in the medium-term window. (Analysis ID: 2)
#' @param useCostByDomainShortTerm         Cost stratified by domain in the short-term window. (Analysis ID: 2)
#' @param useCostByTypeLongTerm            Cost stratified by cost type in the long-term window. (Analysis ID: 3)
#' @param useCostByTypeMediumTerm          Cost stratified by cost type in the medium-term window. (Analysis ID: 3)
#' @param useCostByTypeShortTerm           Cost stratified by cost type in the short-term window. (Analysis ID: 3)
#' @param useUtilizationLongTerm           Healthcare utilization in the long-term window. (Analysis ID: 4)
#' @param useUtilizationMediumTerm         Healthcare utilization in the medium-term window. (Analysis ID: 4)
#' @param useUtilizationShortTerm          Healthcare utilization in the short-term window. (Analysis ID: 4)
#' @param longTermStartDays                Start day (relative to index) for the long-term window.
#' @param mediumTermStartDays              Start day (relative to index) for the medium-term window.
#' @param shortTermStartDays               Start day (relative to index) for the short-term window.
#' @param endDays                          End day (relative to index) for all standard (long, medium, short-term) windows.
#' @param costDomains                      A character vector of specific cost domains to include. If NULL, all are included.
#' @param costTypeConceptIds               A numeric vector of `cost_type_concept_id`s to include. If NULL, all are included.
#' @param currencyConceptIds               A numeric vector of `currency_concept_id`s to include. If NULL, all are included.
#' @param includedCovariateConceptIds      A vector of concept IDs for which to calculate costs.
#' @param addDescendantsToInclude          Should descendants of the `includedCovariateConceptIds` be included?
#' @param excludedCovariateConceptIds      A vector of concept IDs to exclude from cost calculations.
#' @param addDescendantsToExclude          Should descendants of the `excludedCovariateConceptIds` be excluded?
#'
#' @return
#' An S3 object of class `costCovariateSettings`.
#'
#' @export
createCostCovariateSettings <- function(useCostTotalLongTerm = FALSE,
                                        useCostTotalMediumTerm = FALSE,
                                        useCostTotalShortTerm = FALSE,
                                        useCostByDomainLongTerm = FALSE,
                                        useCostByDomainMediumTerm = FALSE,
                                        useCostByDomainShortTerm = FALSE,
                                        useCostByTypeLongTerm = FALSE,
                                        useCostByTypeMediumTerm = FALSE,
                                        useCostByTypeShortTerm = FALSE,
                                        useUtilizationLongTerm = FALSE,
                                        useUtilizationMediumTerm = FALSE,
                                        useUtilizationShortTerm = FALSE,
                                        longTermStartDays = -365,
                                        mediumTermStartDays = -180,
                                        shortTermStartDays = -30,
                                        endDays = 0,
                                        costDomains = NULL,
                                        costTypeConceptIds = NULL,
                                        currencyConceptIds = NULL,
                                        includedCovariateConceptIds = c(),
                                        addDescendantsToInclude = FALSE,
                                        excludedCovariateConceptIds = c(),
                                        addDescendantsToExclude = FALSE) {
  # --- Input Validation ---
  formalNames <- names(formals(createCostCovariateSettings))
  for (name in formalNames) {
    if (grepl("use.*", name)) {
      checkmate::assertFlag(get(name))
    }
  }
  checkmate::assertList(customTemporalWindows, types = "numeric", min.len = 0)
  # ... (all other checkmate assertions as before) ...
  
  # --- Bundle Settings ---
  settings <- list(
    temporal = list(
      longTermStartDays = longTermStartDays,
      mediumTermStartDays = mediumTermStartDays,
      shortTermStartDays = shortTermStartDays,
      endDays = endDays
    ),
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
  
  # Capture all 'use' flags
  analyses <- list()
  anyUseTrue <- FALSE
  for (name in formalNames) {
    if (grepl("use.*", name)) {
      if (get(name)) {
        analyses[[name]] <- TRUE
        anyUseTrue <- TRUE
      } else {
        analyses[[name]] <- FALSE
      }
    }
  }
  settings$analyses <- analyses
  
  if (!anyUseTrue) {
    ParallelLogger::logWarn("No analyses selected (e.g., useCostTotalLongTerm = FALSE). This will not generate any covariates.")
  }
  
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
  writeLines("Unified Cost & Utilization Covariate Settings")
  writeLines(paste(rep("-", 50), collapse = ""))
  
  # --- Analyses Section ---
  writeLines("\nAnalyses to be generated:")
  analysisTypes <- list(
    CostTotal = "Total Cost",
    CostByDomain = "Cost by Domain",
    CostByType = "Cost by Type",
    Utilization = "Utilization"
  )
  
  hasAnalyses <- FALSE
  for (analysis in names(analysisTypes)) {
    windows <- c()
    if (x$analyses[[paste0("use", analysis, "LongTerm")]]) { windows <- c(windows, "Long-Term") }
    if (x$analyses[[paste0("use", analysis, "MediumTerm")]]) { windows <- c(windows, "Medium-Term") }
    if (x$analyses[[paste0("use", analysis, "ShortTerm")]]) { windows <- c(windows, "Short-Term") }
    
    # An analysis is applied to custom windows if ANY of its standard flags are true
    if (length(x$temporal$customTemporalWindows) > 0 && any(grepl(analysis, names(which(unlist(x$analyses)))))) {
      windows <- c(windows, "Custom")
    }
    
    if (length(windows) > 0) {
      hasAnalyses <- TRUE
      writeLines(sprintf("  - %-15s: %s", analysisTypes[[analysis]], paste(unique(windows), collapse = ", ")))
    }
  }
  if (!hasAnalyses) { writeLines("  - None selected") }
  
  # --- Temporal Windows Section ---
  writeLines("\nTemporal Window Definitions:")
  writeLines(sprintf("  - Long-Term Window:   [%d, %d]", x$temporal$longTermStartDays, x$temporal$endDays))
  writeLines(sprintf("  - Medium-Term Window: [%d, %d]", x$temporal$mediumTermStartDays, x$temporal$endDays))
  writeLines(sprintf("  - Short-Term Window:  [%d, %d]", x$temporal$shortTermStartDays, x$temporal$endDays))
  
  if (length(x$temporal$customTemporalWindows) > 0) {
    for (i in seq_along(x$temporal$customTemporalWindows)) {
      window <- x$temporal$customTemporalWindows[[i]]
      writeLines(sprintf("  - Custom Window %-2d:    [%d, %d]", i, window[1], window[2]))
    }
  } else {
    writeLines("  - Custom Windows:     None specified")
  }
  
  # --- Filters Section (as before) ---
  
  writeLines(paste(rep("-", 50), collapse = ""))
  invisible(x)
}