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

#' Create Cost and Utilization Analysis Settings
#'
#' @description
#' Creates a settings object for defining a cost and healthcare utilization analysis.
#'
#' @param analysisName              A string name for the analysis.
#' @param timeWindows               A list of numeric vectors defining fixed time windows relative to cohort start.
#'                                  Can be `NULL` if `useInCohortWindow = TRUE`.
#' @param useInCohortWindow         A boolean. If `TRUE`, a window from `cohort_start_date` to `cohort_end_date` is included.
#' @param costDomains               A character vector of OMOP CDM domains to calculate costs for. If NULL, no cost-by-domain is computed.
#' @param utilizationDomains        A character vector of OMOP CDM domains for which to count occurrences. If NULL, no utilization is computed.
#' @param conceptIds                A numeric vector of concept IDs for which to calculate costs. If `NULL`, this analysis is skipped.
#' @param calculateTotalCost        Boolean. If TRUE, calculates the total cost across all specified domains.
#' @param calculateLengthOfStay     Boolean. If TRUE, calculates the length of stay for inpatient visits.
#' @param costTypeConceptIds        A numeric vector of `cost_type_concept_id`s to include. If NULL, all are included.
#' @param currencyConceptId         A single integer for the `currency_concept_id` to use. Defaults to 44818668 (USD).
#' @param costStandardizationYear   Integer. The year to which all costs will be standardized. If NULL, no standardization is performed.
#' @param cpiData                   A data frame with 'year' and 'cpi' columns for cost standardization.
#'
#' @return
#' An object of class `costUtilSettings`.
#'
#' @export
createCostUtilSettings <- function(analysisName = "Cost and Utilization Analysis",
                                   timeWindows = list(c(-365, -1), c(0, 365)),
                                   useInCohortWindow = FALSE,
                                   costDomains = c("Drug", "Procedure", "Visit"),
                                   utilizationDomains = c("Visit", "Drug", "Procedure"),
                                   conceptIds = NULL,
                                   calculateTotalCost = TRUE,
                                   calculateLengthOfStay = TRUE,
                                   costTypeConceptIds = NULL,
                                   currencyConceptId = 44818668,
                                   costStandardizationYear = NULL,
                                   cpiData = NULL) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertString(analysisName, add = errorMessages)
  checkmate::assertList(timeWindows, types = "numeric", null.ok = TRUE, add = errorMessages)
  checkmate::assertFlag(useInCohortWindow, add = errorMessages)
  checkmate::assertCharacter(costDomains, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(utilizationDomains, null.ok = TRUE, add = errorMessages)
  checkmate::assertIntegerish(conceptIds, null.ok = TRUE, unique = TRUE, add = errorMessages)
  checkmate::assertFlag(calculateTotalCost, add = errorMessages)
  checkmate::assertFlag(calculateLengthOfStay, add = errorMessages)
  checkmate::assertIntegerish(costTypeConceptIds, null.ok = TRUE, add = errorMessages)
  checkmate::assertInt(currencyConceptId, add = errorMessages)
  checkmate::assertInt(costStandardizationYear, null.ok = TRUE, add = errorMessages)
  checkmate::assertDataFrame(cpiData, null.ok = TRUE, add = errorMessages)
  if (!is.null(cpiData)) {
    checkmate::assertNames(colnames(cpiData), must.include = c("year", "cpi"), add = errorMessages)
  }
  if (is.null(timeWindows) && !useInCohortWindow) {
    errorMessages$push("At least one windowing strategy must be used. Set 'timeWindows' or set 'useInCohortWindow = TRUE'.")
  }
  checkmate::reportAssertions(collection = errorMessages)
  
  structure(
    list(
      analysisName = analysisName,
      timeWindows = timeWindows,
      useInCohortWindow = useInCohortWindow,
      costDomains = costDomains,
      utilizationDomains = utilizationDomains,
      conceptIds = conceptIds,
      calculateTotalCost = calculateTotalCost,
      calculateLengthOfStay = calculateLengthOfStay,
      costTypeConceptIds = costTypeConceptIds,
      currencyConceptId = currencyConceptId,
      costStandardizationYear = costStandardizationYear,
      cpiData = cpiData
    ),
    class = "costUtilSettings"
  )
}


#' Get default Consumer Price Index (CPI) data for Medical Care
#'
#' @description
#' Provides a default data frame of the Consumer Price Index for All Urban Consumers: Medical Care in U.S. City Average.
#' Data is sourced from the U.S. Bureau of Labor Statistics, retrieved via FRED (Series ID: `CPIAUCNS.CPIMEDSL`).
#'
#' @return A data frame with `year` and `cpi` columns.
#' @export
getDefaultCpiTable <- function() {
  # Data as of early 2024.
  utils::read.csv(system.file("csv/cpi_data.csv", package = "CostUtilization"))
}

