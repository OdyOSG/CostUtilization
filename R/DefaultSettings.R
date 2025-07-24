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

#' Get Default Cost Covariate Settings
#'
#' @description
#' Provides pre-configured settings for common cost analysis scenarios. These templates
#' call `createCostCovariateSettings` with sensible defaults.
#'
#' @param analysisType  The type of default analysis. Options are:
#'                      - `"simple"`: Total cost and cost by domain across standard time windows.
#'                      - `"medical_only"`: Costs limited to medical domains (e.g., Visit, Procedure).
#'                      - `"pharmacy_only"`: Costs limited to the 'Drug' domain.
#'                      - `"utilization_only"`: Only computes utilization metrics across standard windows.
#' @param ...           Additional arguments to be passed to `createCostCovariateSettings`,
#'                      allowing for customization of the template (e.g., overriding
#'                      `longTermStartDays` or providing `currencyConceptIds`).
#'
#' @return
#' An S3 object of class `costCovariateSettings`.
#'
#' @examples
#' # Get settings for a simple, standard analysis
#' simpleSettings <- getDefaultCostSettings("simple")
#'
#' # Get settings for medical costs only, but with different standard windows
#' medicalSettings <- getDefaultCostSettings(
#'   analysisType = "medical_only",
#'   longTermStartDays = -180,
#'   shortTermStartDays = 0,
#'   endDays = 0
#' )
#'
#' @export
getDefaultCostSettings <- function(analysisType = "simple", ...) {
  # --- Input Validation ---
  checkmate::assertChoice(analysisType,
                          choices = c("simple", "medical_only", "pharmacy_only", "utilization_only")
  )
  
  # Capture user-provided arguments to override defaults
  args <- list(...)
  
  # --- Set template-specific defaults ---
  defaults <- list()
  if (analysisType == "simple") {
    defaults <- list(
      useCostTotalLongTerm = TRUE,
      useCostByDomainLongTerm = TRUE,
      useCostTotalShortTerm = TRUE,
      useCostByDomainShortTerm = TRUE
    )
  } else if (analysisType == "medical_only") {
    defaults <- list(
      useCostTotalLongTerm = TRUE,
      useCostByDomainLongTerm = TRUE,
      costDomains = c("Visit", "Procedure", "Device", "Measurement", "Observation", "Specimen")
    )
  } else if (analysisType == "pharmacy_only") {
    defaults <- list(
      useCostTotalLongTerm = TRUE,
      useCostByDomainLongTerm = TRUE,
      costDomains = "Drug"
    )
  } else if (analysisType == "utilization_only") {
    defaults <- list(
      useUtilizationLongTerm = TRUE,
      useUtilizationMediumTerm = TRUE,
      useUtilizationShortTerm = TRUE
    )
  }
  
  # Combine the template defaults with the user's arguments.
  # The user's arguments in `args` will take precedence over `defaults`.
  finalArgs <- utils::modifyList(defaults, args)
  
  # Call the main settings function with the combined arguments
  do.call(createCostCovariateSettings, finalArgs)
}

#' Get a Pre-defined Cost Analysis Study Template
#'
#' @description
#' Returns a list containing a pre-configured `costCovariateSettings` object and
#' associated metadata for a specific clinical study use-case.
#'
#' @details
#' Each template provides:
#' \itemize{
#'   \item{`settings`}{A `costCovariateSettings` object ready for use with `getDbCostCovariateData`.}
#'   \item{`description`}{A string describing the purpose of the analysis template.}
#'   \item{`recommendedCohorts`}{A character vector of example cohort types suitable for the analysis.}
#' }
#'
#' @param templateName The name of the study template. Available options are:
#'   \itemize{
#'     \item{"episodeOfCare"}{Costs before, during, and after an episode of care (e.g., surgery).}
#'     \item{"chronicDisease"}{Long-term cost and utilization tracking for chronic conditions.}
#'     \item{"acuteEvent"}{Short-term cost analysis around an acute event (e.g., MI, stroke).}
#'     \item{"preventiveCare"}{Costs associated with preventive services, excluding pharmacy.}
#'     \item{"hospitalization"}{Comprehensive analysis of costs surrounding an inpatient stay.}
#'   }
#'
#' @return
#' A list containing the `settings` object, `description`, and `recommendedCohorts`.
#'
#' @examples
#' # Get the template for analyzing an acute event
#' acuteEventTemplate <- getCostAnalysisTemplate("acuteEvent")
#'
#' # You can now use the settings object directly
#' \dontrun{
#' covData <- getDbCostCovariateData(
#'   connectionDetails = connectionDetails,
#'   cdmDatabaseSchema = "main",
#'   cohortTable = "stroke_cohort",
#'   cohortIds = 1,
#'   costCovariateSettings = acuteEventTemplate$settings
#' )
#' }
#'
#' @export
getCostAnalysisTemplate <- function(templateName = "episodeOfCare") {
  # --- Input Validation ---
  checkmate::assertChoice(templateName,
                          choices = c(
                            "episodeOfCare", "chronicDisease", "acuteEvent",
                            "preventiveCare", "hospitalization"
                          )
  )
  
  # --- Template Definitions ---
  template <- switch(templateName,
                     episodeOfCare = list(
                       settings = createCostCovariateSettings(
                         customTemporalWindows = list(c(-30, -1), c(0, 0), c(1, 30), c(31, 90)),
                         useCostTotalLongTerm = TRUE, # The 'LongTerm' flag activates the analysis for custom windows
                         useCostByDomainLongTerm = TRUE,
                         useUtilizationLongTerm = TRUE
                       ),
                       description = "Analyzes costs and utilization before, during, and after a defined episode of care.",
                       recommendedCohorts = c("Surgical procedures", "Chemotherapy regimens", "Dialysis initiation")
                     ),
                     chronicDisease = list(
                       settings = createCostCovariateSettings(
                         useCostTotalLongTerm = TRUE,
                         useCostByDomainLongTerm = TRUE,
                         useUtilizationLongTerm = TRUE,
                         longTermStartDays = -365,
                         endDays = 365 # Track for one year before and one year after index
                       ),
                       description = "Long-term cost and utilization tracking for managing chronic diseases.",
                       recommendedCohorts = c("Diabetes", "COPD", "Heart Failure", "CKD")
                     ),
                     acuteEvent = list(
                       settings = createCostCovariateSettings(
                         customTemporalWindows = list(c(-7, -1), c(0, 0), c(1, 7), c(8, 30), c(31, 90)),
                         useCostTotalLongTerm = TRUE, # The 'LongTerm' flag activates the analysis for custom windows
                         useCostByDomainLongTerm = TRUE,
                         useUtilizationLongTerm = TRUE
                       ),
                       description = "Short-term cost analysis immediately surrounding an acute medical event.",
                       recommendedCohorts = c("Myocardial Infarction", "Ischemic Stroke", "Pneumonia", "Hip Fracture")
                     ),
                     preventiveCare = list(
                       settings = createCostCovariateSettings(
                         useCostTotalLongTerm = TRUE,
                         useUtilizationLongTerm = TRUE,
                         costDomains = c("Visit", "Procedure", "Device", "Measurement", "Observation", "Specimen") # Excludes 'Drug'
                       ),
                       description = "Cost and utilization analysis for preventive care services, excluding pharmacy costs.",
                       recommendedCohorts = c("Cancer screenings", "Vaccinations", "Annual wellness visits")
                     ),
                     hospitalization = list(
                       settings = createCostCovariateSettings(
                         customTemporalWindows = list(c(-30, -1), c(0, 0), c(1, 30), c(31, 90)),
                         useCostTotalLongTerm = TRUE, # Use a standard 'use' flag to apply to all custom windows
                         useCostByDomainLongTerm = TRUE,
                         useCostByTypeLongTerm = TRUE,
                         useUtilizationLongTerm = TRUE
                       ),
                       description = "Comprehensive cost analysis for inpatient stays, including pre-admission and post-discharge periods.",
                       recommendedCohorts = c("Hospital admissions", "ICU stays", "Surgical admissions")
                     )
  )
  
  return(template)
}