# Copyright 2025 OHDSI
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
#' Create Cost and Utilization Settings
#'
#' @description
#' Creates a settings object for configuring a cost and healthcare resource utilization analysis.
#' This object is used by the `computeCostUtilization` function.
#'
#' @section Precedence Rules:
#' Some arguments take precedence over others if both are provided:
#' \itemize{
#'   \item \strong{`useConceptSet` > `costDomains`}: If `useConceptSet` is specified, it provides a
#'   precise definition of the events to be included in the analysis. The broader `costDomains`
#'   argument will be ignored, and a warning will be issued.
#' }
#'
#' @param timeWindows             A list of numeric vectors, where each vector has two elements:
#'                                the start and end day relative to the cohort start date.
#'                                For example, `list(c(-365, 0))` defines a 1-year lookback window.
#' @param useInCohortWindow       A logical flag. If TRUE, the analysis will include the
#'                                time a person is inside the cohort (from `cohort_start_date` to
#'                                `cohort_end_date`) as a distinct analysis window.
#' @param costDomains             A character vector of cost domains to include ('Condition', 'Drug').
#'                                This argument is ignored if `useConceptSet` is provided. (case independent)
#' @param useConceptSet           An optional concept set to restrict the events used for cost and
#'                                utilization calculation. This provides fine-grained control over
#'                                which events contribute to the totals. The input can be a numeric
#'                                vector of concept IDs, a `Capr` object, or a standard list-based
#'                                concept set expression.
#' @param costTypeConceptIds      A numeric vector of `cost_type_concept_id`s to restrict the
#'                                analysis to. If NULL, all cost types are included.
#' @param currencyConceptId       The `currency_concept_id` to filter costs on. Defaults to
#'                                44818668 (US Dollar).
#' @param aggregate               A character vector specifying the aggregation level for per-patient
#'                                metrics. The calculation is based on the total cost/utilization
#'                                divided by the total observed person-days in a window, which is then
#'                                scaled to the desired time unit. Supported options are:
#'                                \itemize{
#'                                  \item{\code{'pppd'}: Per-Patient Per-Day. The base rate, calculated as total value / total observation days.}
#'                                  \item{\code{'pppm'}: Per-Patient Per-Month. The per-day rate multiplied by the average days in a month (30.44).}
#'                                  \item{\code{'pppq'}: Per-Patient Per-Quarter. The per-day rate multiplied by the average days in a quarter (91.31).}
#'                                  \item{\code{'pppy'}: Per-Patient Per-Year. The per-day rate multiplied by the average days in a year (365.25).}
#'                                }
#' @param standardizationData     A data frame used to standardize costs. If NULL, no
#'                                standardization is performed.
#'
#' @return
#' An object of class `costUtilizationSettings`.
#'
#' @export
createCostUtilizationSettings <- function(
    timeWindows = list(c(-365, 0)),
    useInCohortWindow = FALSE,
    costDomains = NULL,
    useConceptSet = NULL,
    costTypeConceptIds = NULL,
    currencyConceptId = 44818668, # US dollar
    aggregate = c("pppm", "pppy"),
    standardizationData = NULL) {
  # --- 1. Initial Argument Validation ---
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertList(timeWindows, types = "numeric", null.ok = TRUE, add = errorMessages)
  if (!is.null(timeWindows)) {
    purrr::walk(timeWindows, ~ checkmate::assertNumeric(.x, len = 2, add = errorMessages))
  }
  checkmate::assertFlag(useInCohortWindow, add = errorMessages)
  if (is.null(timeWindows) && !useInCohortWindow) {
    errorMessages$push("At least one windowing strategy must be used.")
  }
  checkmate::assertCharacter(costDomains, null.ok = TRUE, add = errorMessages)
  if (!is.null(useConceptSet)) {
    checkmate::assert(
      checkmate::checkIntegerish(useConceptSet),
      checkmate::checkList(useConceptSet),
      checkmate::checkClass(useConceptSet, "ConceptSet"),
      combine = "or",
      add = errorMessages,
      .var.name = "useConceptSet"
    )
  }
  checkmate::assertIntegerish(costTypeConceptIds, null.ok = TRUE, add = errorMessages)
  checkmate::assertInt(currencyConceptId, add = errorMessages)
  checkmate::assertCharacter(aggregate, min.len = 1, add = errorMessages)
  checkmate::assertSubset(aggregate, choices = c("pppd", "pppm", "pppq", "pppy"), add = errorMessages)
  checkmate::assertDataFrame(standardizationData, null.ok = TRUE, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  settingsEnvironment <- as.list(environment())
  cleanSettings <- .normalizeSettings(settingsEnvironment)

  conceptSetDefinition <- tryCatch(
    {
      .parseConceptSet(cleanSettings$useConceptSet)
    },
    error = function(e) {
      stop(e$message, call. = FALSE)
    }
  )
  structure(
    list(
      timeWindows = cleanSettings$timeWindows,
      useInCohortWindow = cleanSettings$useInCohortWindow,
      costDomains = cleanSettings$costDomains,
      conceptSetDefinition = conceptSetDefinition,
      costTypeConceptIds = cleanSettings$costTypeConceptIds,
      currencyConceptId = cleanSettings$currencyConceptId,
      aggregate = cleanSettings$aggregate,
      standardizationData = cleanSettings$standardizationData
    ),
    class = "costUtilizationSettings"
  )
}
