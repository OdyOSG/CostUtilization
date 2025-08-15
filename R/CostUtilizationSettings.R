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
#' Creates a settings object for configuring a cost and healthcare resource
#' utilization analysis. This object is used by analysis functions that compute
#' these metrics.
#'
#' @section Precedence Rules:
#' Some arguments take precedence over others if both are provided:
#' \itemize{
#'   \item \strong{`useConceptSet` > `costDomains`}: If `useConceptSet` is specified, it provides a
#'   precise definition of the events to be included. The broader `costDomains`
#'   argument will be ignored, and a warning will be issued.
#'   \item \strong{`aggregate = "none"` is exclusive}: If `"none"` is included in the `aggregate`
#'   vector, all other aggregation options will be ignored, and a warning will be issued.
#' }
#'
#' @param timeWindows A list of numeric vectors, where each vector has two
#'   elements: the start and end day relative to the cohort start date. For
#'   example, `list(c(-365, 0))` defines a 1-year lookback window.
#' @param useInCohortWindow A logical flag. If `TRUE`, the analysis will include
#'   the time a person is inside the cohort (from `cohort_start_date` to
#'   `cohort_end_date`) as a distinct analysis window.
#' @param costDomains A character vector of cost domains to include (e.g.,
#'   'Condition', 'Drug'). This argument is case-insensitive and is ignored if
#'   `useConceptSet` is provided.
#' @param useConceptSet An optional concept set to restrict the events used for
#'   cost and utilization calculation. This provides fine-grained control over
#'   which events contribute to the totals. The input can be a numeric vector
#'   of concept IDs, a `Capr` object, or a standard list-based concept set
#'   expression.
#' @param costTypeConceptIds A numeric vector of `cost_type_concept_id`s to
#'   restrict the analysis to. If `NULL`, all cost types are included.
#' @param costConceptId A single integer `concept_id` to filter the cost table on
#'   the `cost_concept_id` field. For example, use `31985` for 'Total Cost'.
#'   If `NULL`, all cost concepts are considered.
#' @param restrictVisitConceptIds A numeric vector of `visit_concept_id`s to
#'   restrict the analysis to costs incurred during specific visit types (e.g., `9201` for inpatient visits).
#'   If `NULL`, costs from all visit types are included.
#' @param currencyConceptId The `currency_concept_id` to filter costs on.
#'   Defaults to 44818668 (US Dollar).
#' @param aggregate A character vector specifying the aggregation level for
#'   per-patient metrics. The calculation is based on the total cost/utilization
#'   divided by the total observed person-days in a window, which is then
#'   scaled to the desired time unit.
#'   If `"none"` is specified, no aggregation is performed, and this option cannot
#'   be combined with others.
#'   Supported options are:
#'   \itemize{
#'     \item{\code{'pppd'}: Per-Patient Per-Day. The base rate.}
#'     \item{\code{'pppm'}: Per-Patient Per-Month (pppd * 30.44).}
#'     \item{\code{'pppq'}: Per-Patient Per-Quarter (pppd * 91.31).}
#'     \item{\code{'pppy'}: Per-Patient Per-Year (pppd * 365.25).}
#'     \item{\code{'cohort_level'}: Total cost/utilization across the entire cohort.}
#'     \item{\code{'none'}: Do not perform any aggregation.}
#'   }
#' @param standardizationData A data frame or a path to a file (e.g., csv or tsv or xlsx)
#'   containing data used to standardize costs. If `NULL`, no standardization
#'   is performed.
#'
#' @return
#' An object of class `costUtilizationSettings`.
#'
#' @export
#' @examples
#' # Simple settings for a 1-year lookback for drug costs
#' settings <- createCostUtilizationSettings(
#'   timeWindows = list(c(-365, 0)),
#'   costDomains = "Drug",
#'   aggregate = c("pppm", "pppy")
#' )
#'
#' # Settings with conflicting aggregation levels (will trigger a warning)
#' settings_with_warning <- createCostUtilizationSettings(
#'   aggregate = c("none", 'pppm'),
#'   useInCohortWindow = TRUE
#' )
createCostUtilizationSettings <- function(
    timeWindows = list(c(-365, 0)),
    useInCohortWindow = FALSE,
    costDomains = NULL,
    useConceptSet = NULL,
    costTypeConceptIds = NULL,
    costConceptId = NULL,
    restrictVisitConceptIds = NULL,
    currencyConceptId = 44818668, # US dollar
    aggregate = c("pppm", "pppy", "cohort_level"),
    standardizationData = NULL) {
  # --- 1. Initial Argument Validation ---
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertList(timeWindows, types = "numeric", null.ok = TRUE, add = errorMessages)
  if (!is.null(timeWindows)) {
    purrr::walk(timeWindows, ~ checkmate::assertNumeric(.x, len = 2, add = errorMessages))
  }
  checkmate::assertFlag(useInCohortWindow, add = errorMessages)
  if (is.null(timeWindows) && !useInCohortWindow) {
    errorMessages$push("At least one windowing strategy (`timeWindows` or `useInCohortWindow`) must be specified.")
  }
  checkmate::assertCharacter(costDomains, null.ok = TRUE, add = errorMessages)
  checkmate::assertIntegerish(restrictVisitConceptIds, null.ok = TRUE, add = errorMessages)
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
  checkmate::assertInt(costConceptId, add = errorMessages, null.ok = TRUE)
  checkmate::assertInt(currencyConceptId, add = errorMessages, null.ok = TRUE)
  checkmate::assertCharacter(aggregate, min.len = 1, add = errorMessages)
  checkmate::assertSubset(aggregate, choices = c("pppd", "pppm", "pppq", "pppy", "cohort_level", "none"), add = errorMessages)
  if (!is.null(standardizationData)) {
    checkmate::assert(
      checkmate::checkDataFrame(standardizationData),
      checkmate::checkFileExists(standardizationData),
      combine = "or",
      add = errorMessages
    )
  }
  checkmate::reportAssertions(collection = errorMessages)
  settingsEnvironment <- as.list(environment())
  cleanSettings <- .normalizeSettings(settingsEnvironment)
  
  conceptSetDefinition <- purrr::safely(.parseConceptSet)(cleanSettings$useConceptSet)
  structure(
    list(
      timeWindows = cleanSettings$timeWindows,
      useInCohortWindow = cleanSettings$useInCohortWindow,
      costDomains = cleanSettings$costDomains,
      conceptSetDefinition = conceptSetDefinition$result,
      costTypeConceptIds = cleanSettings$costTypeConceptIds,
      costConceptId = cleanSettings$costConceptId,
      restrictVisitConceptIds = cleanSettings$restrictVisitConceptIds,
      currencyConceptId = cleanSettings$currencyConceptId,
      aggregate = cleanSettings$aggregate,
      standardizationData = cleanSettings$standardizationData
    ),
    class = "costUtilizationSettings"
  )
}