#' Get Default Cost Covariate Settings
#'
#' @description
#' Provides pre-configured settings for common cost analysis scenarios. These templates
#' call `createCostCovariateSettings` with sensible defaults and can be used directly
#' with `FeatureExtraction::getDbCovariateData`.
#'
#' @param analysisType  The type of default analysis. Options are:
#'                      - `"simple"`: Total cost and cost by domain across standard time windows.
#'                      - `"medical_only"`: Costs limited to medical domains (e.g., Visit, Procedure).
#'                      - `"pharmacy_only"`: Costs limited to the 'Drug' domain.
#'                      - `"detailed_temporal"`: All standard analyses across a more granular set of time windows.
#' @param ...           Additional arguments to be passed to `createCostCovariateSettings`,
#'                      allowing for customization of the template (e.g., overriding
#'                      `temporalStartDays` or providing `includedCovariateConceptIds`).
#'
#' @return
#' An S3 object of class `costCovariateSettings`.
#'
#' @examples
#' # Get settings for a simple, standard analysis
#' simpleSettings <- getDefaultCostSettings("simple")
#'
#' # Get settings for medical costs only, but with custom time windows
#' medicalSettings <- getDefaultCostSettings(
#'   analysisType = "medical_only",
#'   temporalStartDays = c(-180, 0),
#'   temporalEndDays = c(-1, 0)
#' )
#'
#' # Get settings for a simple analysis focused only on a specific concept set
#' conceptSettings <- getDefaultCostSettings(
#'   analysisType = "simple",
#'   includedCovariateConceptIds = c(201826) # Type 2 diabetes
#' )
#'
#' @export
getDefaultCostSettings <- function(analysisType = "simple", ...) {
  # --- Input Validation ---
  checkmate::assertChoice(analysisType,
    choices = c("simple", "medical_only", "pharmacy_only", "detailed_temporal")
  )

  # Capture user-provided arguments
  args <- list(...)

  # Set template-specific defaults, which can be overridden by the user's `...` arguments
  if (analysisType == "simple") {
    # Default time windows for a simple analysis
    defaults <- list(
      temporalStartDays = c(-365, -30, 0, 1),
      temporalEndDays = c(-1, -1, 0, 365)
    )
  } else if (analysisType == "medical_only") {
    # Default windows and filter to medical domains
    defaults <- list(
      temporalStartDays = c(-365, -30, 0),
      temporalEndDays = c(-1, -1, 0),
      costDomains = c("Visit", "Procedure", "Device", "Measurement", "Observation", "Specimen")
    )
  } else if (analysisType == "pharmacy_only") {
    # Default windows and filter to drug domain
    defaults <- list(
      temporalStartDays = c(-365, -30, 0),
      temporalEndDays = c(-1, -1, 0),
      costDomains = "Drug"
    )
  } else if (analysisType == "detailed_temporal") {
    # A more granular set of time windows
    defaults <- list(
      temporalStartDays = c(-365, -180, -90, -30, 0, 1, 31, 91, 181),
      temporalEndDays   = c(-181, -91, -31, -1, 0, 30, 90, 180, 365)
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
#' associated metadata for a specific clinical study use-case. This provides a
#' convenient starting point for common types of cost analyses.
#'
#' @details
#' Each template provides:
#' \itemize{
#'   \item{`settings`}{A `costCovariateSettings` object ready for use with `FeatureExtraction::getDbCovariateData`.}
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
#'     \item{"emergencyDepartment"}{Cost analysis around an emergency department visit.}
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
#' covData <- FeatureExtraction::getDbCovariateData(
#'   connectionDetails = connectionDetails,
#'   cdmDatabaseSchema = "main",
#'   cohortTable = "stroke_cohort",
#'   cohortId = 1,
#'   covariateSettings = acuteEventTemplate$settings,
#'   aggregated = TRUE
#' )
#' }
#'
#' @export
getCostAnalysisTemplate <- function(templateName = "episodeOfCare") {
  # --- Input Validation ---
  checkmate::assertChoice(templateName,
    choices = c(
      "episodeOfCare", "chronicDisease", "acuteEvent",
      "preventiveCare", "emergencyDepartment", "hospitalization"
    )
  )

  # --- Template Definitions ---
  template <- switch(templateName,
    episodeOfCare = list(
      settings = createCostCovariateSettings(
        temporalStartDays = c(-30, 0, 1, 31, 91),
        temporalEndDays = c(-1, 0, 30, 90, 180),
        costByDomain = TRUE,
        utilization = TRUE
      ),
      description = "Analyzes costs and utilization before, during, and after a defined episode of care.",
      recommendedCohorts = c("Surgical procedures", "Chemotherapy regimens", "Dialysis initiation")
    ),
    chronicDisease = list(
      settings = createCostCovariateSettings(
        temporalStartDays = c(-365, -180, 0, 1, 181, 366),
        temporalEndDays = c(-181, -1, 0, 180, 365, 730),
        costByDomain = TRUE,
        utilization = TRUE
      ),
      description = "Long-term cost and utilization tracking for managing chronic diseases.",
      recommendedCohorts = c("Diabetes", "COPD", "Heart Failure", "CKD")
    ),
    acuteEvent = list(
      settings = createCostCovariateSettings(
        temporalStartDays = c(-7, 0, 1, 8, 31),
        temporalEndDays = c(-1, 0, 7, 30, 90),
        costByDomain = TRUE,
        utilization = TRUE
      ),
      description = "Short-term cost analysis immediately surrounding an acute medical event.",
      recommendedCohorts = c("Myocardial Infarction", "Ischemic Stroke", "Pneumonia", "Hip Fracture")
    ),
    preventiveCare = list(
      settings = createCostCovariateSettings(
        temporalStartDays = c(-365, 0, 1),
        temporalEndDays = c(-1, 0, 365),
        costDomains = c("Visit", "Procedure", "Device", "Measurement", "Observation", "Specimen"), # Excludes 'Drug'
        utilization = TRUE
      ),
      description = "Cost and utilization analysis for preventive care services, excluding pharmacy costs.",
      recommendedCohorts = c("Cancer screenings", "Vaccinations", "Annual wellness visits")
    ),
    emergencyDepartment = list(
      settings = createCostCovariateSettings(
        temporalStartDays = c(-30, -7, 0, 1, 8),
        temporalEndDays = c(-8, -1, 0, 7, 30),
        costByType = TRUE,
        costByDomain = TRUE,
        utilization = TRUE
      ),
      description = "Analysis of costs and utilization in the period immediately before and after an ED visit.",
      recommendedCohorts = c("ED visits", "Urgent care visits", "Observation stays")
    ),
    hospitalization = list(
      settings = createCostCovariateSettings(
        temporalStartDays = c(-30, 0, 1, 31, 91),
        temporalEndDays = c(-1, 0, 30, 90, 180),
        costByDomain = TRUE,
        costByType = TRUE,
        utilization = TRUE
      ),
      description = "Comprehensive cost analysis for inpatient stays, including pre-admission and post-discharge periods.",
      recommendedCohorts = c("Hospital admissions", "ICU stays", "Surgical admissions")
    )
  )

  return(template)
}
