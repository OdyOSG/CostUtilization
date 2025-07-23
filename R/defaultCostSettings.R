# R/DefaultCostSettings.R

#' Get default cost covariate settings
#'
#' @description
#' Returns pre-configured cost covariate settings for common use cases in healthcare
#' cost analysis. These settings can be used directly or modified as needed.
#'
#' @param analysisType Type of default analysis. Options are:
#'   \itemize{
#'     \item{"simple"}{Basic total cost analysis across standard time windows}
#'     \item{"comprehensive"}{Detailed analysis including all cost domains and types}
#'     \item{"medical"}{Medical costs only (excludes pharmacy)}
#'     \item{"pharmacy"}{Pharmacy costs only}
#'     \item{"highCost"}{Analysis focused on high-cost patients}
#'     \item{"temporal"}{Detailed temporal analysis with multiple time windows}
#'     \item{"utilization"}{Cost utilization patterns and indicators}
#'     \item{"stratified"}{Costs stratified by demographics and domains}
#'   }
#' @param temporalStartDays Custom start days for temporal windows (overrides defaults)
#' @param temporalEndDays Custom end days for temporal windows (overrides defaults)
#' @param costTypeConceptIds Specific cost type concept IDs to include
#' @param currencyConceptId Currency concept ID (defaults to USD: 44818568)
#' @param includeOutliers Whether to include outlier analysis
#'
#' @return
#' A costCovariateSettings object configured for the specified analysis type
#'
#' @examples
#' \dontrun{
#' # Get simple default settings
#' settings <- getDefaultCostSettings("simple")
#'
#' # Get comprehensive analysis settings
#' settings <- getDefaultCostSettings("comprehensive")
#'
#' # Get medical costs only with custom time windows
#' settings <- getDefaultCostSettings(
#'   "medical",
#'   temporalStartDays = c(-180, -90, -30),
#'   temporalEndDays = c(-1, -1, -1)
#' )
#' }
#'
#' @export
getDefaultCostSettings <- function(analysisType = "simple",
                                   temporalStartDays = NULL,
                                   temporalEndDays = NULL,
                                   costTypeConceptIds = NULL,
                                   currencyConceptId = 44818568,
                                   includeOutliers = FALSE) {
  validTypes <- c(
    "simple", "comprehensive", "medical", "pharmacy",
    "highCost", "temporal", "utilization", "stratified"
  )

  if (!analysisType %in% validTypes) {
    stop(paste("analysisType must be one of:", paste(validTypes, collapse = ", ")))
  }

  # Default cost type concept IDs if not specified
  if (is.null(costTypeConceptIds)) {
    costTypeConceptIds <- c(
      44818668, # Allowed amount
      44818669, # Paid amount
      44818670, # Patient paid amount
      44818671 # Primary payer paid amount
    )
  }

  # Base settings that apply to all types
  baseSettings <- list(
    useCosts = TRUE,
    currencyConceptIds = currencyConceptId,
    aggregateMethod = "sum",
    addDescendantsToIncludedCovariateConceptIds = FALSE,
    addDescendantsToExcludedCovariateConceptIds = FALSE
  )

  # Configure settings based on analysis type
  settings <- switch(analysisType,
    simple = {
      if (is.null(temporalStartDays)) {
        temporalStartDays <- c(-365, 0, 1)
        temporalEndDays <- c(-1, 0, 365)
      }

      c(baseSettings, list(
        temporalStartDays = temporalStartDays,
        temporalEndDays = temporalEndDays,
        costTypeConceptIds = costTypeConceptIds[1], # Allowed amount only
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = FALSE,
        includeDrugCosts = FALSE,
        includeVisitCosts = FALSE,
        includeDeviceCosts = FALSE,
        stratifyByAgeGroup = FALSE,
        stratifyByGender = FALSE,
        stratifyByCostDomain = FALSE,
        stratifyByCostType = FALSE,
        useCostDemographics = FALSE,
        useCostVisitCounts = FALSE,
        useCostUtilization = FALSE,
        includeTimeDistribution = FALSE,
        includeOutlierAnalysis = includeOutliers
      ))
    },
    comprehensive = {
      if (is.null(temporalStartDays)) {
        temporalStartDays <- c(-365, -180, -90, -30, 0, 1, 30, 90, 180, 365)
        temporalEndDays <- c(-181, -91, -31, -1, 0, 30, 90, 180, 365, 730)
      }

      c(baseSettings, list(
        temporalStartDays = temporalStartDays,
        temporalEndDays = temporalEndDays,
        costTypeConceptIds = costTypeConceptIds,
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = TRUE,
        includeDrugCosts = TRUE,
        includeVisitCosts = TRUE,
        includeDeviceCosts = TRUE,
        stratifyByAgeGroup = TRUE,
        stratifyByGender = TRUE,
        stratifyByCostDomain = TRUE,
        stratifyByCostType = TRUE,
        useCostDemographics = TRUE,
        useCostVisitCounts = TRUE,
        useCostUtilization = TRUE,
        includeTimeDistribution = TRUE,
        includeOutlierAnalysis = TRUE,
        outlierLowerQuantile = 0.01,
        outlierUpperQuantile = 0.99
      ))
    },
    medical = {
      if (is.null(temporalStartDays)) {
        temporalStartDays <- c(-365, -180, -30, 0)
        temporalEndDays <- c(-1, -1, -1, 0)
      }

      c(baseSettings, list(
        temporalStartDays = temporalStartDays,
        temporalEndDays = temporalEndDays,
        costTypeConceptIds = costTypeConceptIds[1:2], # Allowed and paid
        costDomainIds = c("Visit", "Procedure", "Measurement", "Observation"),
        includeMedicalCosts = TRUE,
        includePharmacyCosts = FALSE,
        includeProcedureCosts = TRUE,
        includeDrugCosts = FALSE,
        includeVisitCosts = TRUE,
        includeDeviceCosts = FALSE,
        stratifyByAgeGroup = FALSE,
        stratifyByGender = FALSE,
        stratifyByCostDomain = TRUE,
        stratifyByCostType = FALSE,
        useCostDemographics = FALSE,
        useCostVisitCounts = TRUE,
        useCostUtilization = FALSE,
        includeTimeDistribution = FALSE,
        includeOutlierAnalysis = includeOutliers
      ))
    },
    pharmacy = {
      if (is.null(temporalStartDays)) {
        temporalStartDays <- c(-365, -180, -30, 0)
        temporalEndDays <- c(-1, -1, -1, 0)
      }

      c(baseSettings, list(
        temporalStartDays = temporalStartDays,
        temporalEndDays = temporalEndDays,
        costTypeConceptIds = costTypeConceptIds[1:2], # Allowed and paid
        costDomainIds = c("Drug"),
        includeMedicalCosts = FALSE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = FALSE,
        includeDrugCosts = TRUE,
        includeVisitCosts = FALSE,
        includeDeviceCosts = FALSE,
        stratifyByAgeGroup = FALSE,
        stratifyByGender = FALSE,
        stratifyByCostDomain = FALSE,
        stratifyByCostType = TRUE,
        useCostDemographics = FALSE,
        useCostVisitCounts = FALSE,
        useCostUtilization = TRUE,
        includeTimeDistribution = FALSE,
        includeOutlierAnalysis = includeOutliers
      ))
    },
    highCost = {
      if (is.null(temporalStartDays)) {
        temporalStartDays <- c(-365, 0, 1)
        temporalEndDays <- c(-1, 0, 365)
      }

      c(baseSettings, list(
        temporalStartDays = temporalStartDays,
        temporalEndDays = temporalEndDays,
        costTypeConceptIds = costTypeConceptIds[1], # Allowed amount
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = TRUE,
        includeDrugCosts = TRUE,
        includeVisitCosts = TRUE,
        includeDeviceCosts = TRUE,
        stratifyByAgeGroup = TRUE,
        stratifyByGender = TRUE,
        stratifyByCostDomain = TRUE,
        stratifyByCostType = FALSE,
        useCostDemographics = TRUE,
        useCostVisitCounts = TRUE,
        useCostUtilization = TRUE,
        includeTimeDistribution = TRUE,
        includeOutlierAnalysis = TRUE,
        outlierLowerQuantile = 0.90,
        outlierUpperQuantile = 0.99
      ))
    },
    temporal = {
      if (is.null(temporalStartDays)) {
        # Detailed temporal windows
        temporalStartDays <- c(
          -730, -365, -180, -90, -60, -30, -14, -7, 0,
          1, 8, 15, 31, 61, 91, 181, 366
        )
        temporalEndDays <- c(
          -366, -181, -91, -61, -31, -15, -8, -1, 0,
          7, 14, 30, 60, 90, 180, 365, 730
        )
      }

      c(baseSettings, list(
        temporalStartDays = temporalStartDays,
        temporalEndDays = temporalEndDays,
        costTypeConceptIds = costTypeConceptIds[1], # Allowed amount
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = FALSE,
        includeDrugCosts = FALSE,
        includeVisitCosts = FALSE,
        includeDeviceCosts = FALSE,
        stratifyByAgeGroup = FALSE,
        stratifyByGender = FALSE,
        stratifyByCostDomain = TRUE,
        stratifyByCostType = FALSE,
        useCostDemographics = FALSE,
        useCostVisitCounts = FALSE,
        useCostUtilization = FALSE,
        includeTimeDistribution = TRUE,
        includeOutlierAnalysis = FALSE
      ))
    },
    utilization = {
      if (is.null(temporalStartDays)) {
        temporalStartDays <- c(-365, -180, -30, 0, 1, 30, 180)
        temporalEndDays <- c(-1, -1, -1, 0, 30, 180, 365)
      }

      c(baseSettings, list(
        temporalStartDays = temporalStartDays,
        temporalEndDays = temporalEndDays,
        costTypeConceptIds = costTypeConceptIds,
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = TRUE,
        includeDrugCosts = TRUE,
        includeVisitCosts = TRUE,
        includeDeviceCosts = FALSE,
        stratifyByAgeGroup = FALSE,
        stratifyByGender = FALSE,
        stratifyByCostDomain = TRUE,
        stratifyByCostType = TRUE,
        useCostDemographics = FALSE,
        useCostVisitCounts = TRUE,
        useCostUtilization = TRUE,
        includeTimeDistribution = FALSE,
        includeOutlierAnalysis = includeOutliers,
        aggregateMethod = "sum"
      ))
    },
    stratified = {
      if (is.null(temporalStartDays)) {
        temporalStartDays <- c(-365, -180, -30, 0)
        temporalEndDays <- c(-1, -1, -1, 0)
      }

      c(baseSettings, list(
        temporalStartDays = temporalStartDays,
        temporalEndDays = temporalEndDays,
        costTypeConceptIds = costTypeConceptIds[1:2],
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = TRUE,
        includeDrugCosts = TRUE,
        includeVisitCosts = TRUE,
        includeDeviceCosts = FALSE,
        stratifyByAgeGroup = TRUE,
        stratifyByGender = TRUE,
        stratifyByCostDomain = TRUE,
        stratifyByCostType = TRUE,
        useCostDemographics = TRUE,
        useCostVisitCounts = FALSE,
        useCostUtilization = FALSE,
        includeTimeDistribution = FALSE,
        includeOutlierAnalysis = includeOutliers
      ))
    }
  )

  # Convert to costCovariateSettings object
  do.call(costCovariateSettings, settings)
}

#' Get predefined cost analysis templates
#'
#' @description
#' Returns a list of predefined analysis templates for common cost analysis scenarios.
#' These templates can be used as starting points for custom analyses.
#'
#' @param templateName Name of the template. Options are:
#'   \itemize{
#'     \item{"episodeOfCare"}{Cost analysis for episodes of care}
#'     \item{"chronicDisease"}{Long-term cost tracking for chronic conditions}
#'     \item{"acuteEvent"}{Short-term cost analysis around acute events}
#'     \item{"preventiveCare"}{Cost analysis for preventive care services}
#'     \item{"emergencyDepartment"}{ED visit cost analysis}
#'     \item{"hospitalization"}{Inpatient stay cost analysis}
#'     \item{"specialty"}{Specialty care cost analysis}
#'     \item{"valueBasedCare"}{Cost and utilization for value-based contracts}
#'   }
#'
#' @return
#' A list containing:
#'   \itemize{
#'     \item{settings}{costCovariateSettings object}
#'     \item{description}{Description of the template}
#'     \item{recommendedCohorts}{Suggested cohort types for this analysis}
#'   }
#'
#' @export
getCostAnalysisTemplate <- function(templateName = "episodeOfCare") {
  validTemplates <- c(
    "episodeOfCare", "chronicDisease", "acuteEvent",
    "preventiveCare", "emergencyDepartment", "hospitalization",
    "specialty", "valueBasedCare"
  )

  if (!templateName %in% validTemplates) {
    stop(paste("templateName must be one of:", paste(validTemplates, collapse = ", ")))
  }

  template <- switch(templateName,
    episodeOfCare = list(
      settings = costCovariateSettings(
        useCosts = TRUE,
        temporalStartDays = c(-30, 0, 1, 31, 91),
        temporalEndDays = c(-1, 0, 30, 90, 180),
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = TRUE,
        includeVisitCosts = TRUE,
        stratifyByCostDomain = TRUE,
        stratifyByCostType = FALSE,
        aggregateMethod = "sum"
      ),
      description = "Analyzes costs before, during, and after an episode of care",
      recommendedCohorts = c("Surgical procedures", "Chemotherapy", "Dialysis")
    ),
    chronicDisease = list(
      settings = costCovariateSettings(
        useCosts = TRUE,
        temporalStartDays = c(-365, -180, 0, 1, 181, 366),
        temporalEndDays = c(-181, -1, 0, 180, 365, 730),
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        stratifyByAgeGroup = TRUE,
        stratifyByCostDomain = TRUE,
        useCostUtilization = TRUE,
        includeTimeDistribution = TRUE,
        aggregateMethod = "sum"
      ),
      description = "Long-term cost tracking for chronic disease management",
      recommendedCohorts = c("Diabetes", "COPD", "Heart failure", "CKD")
    ),
    acuteEvent = list(
      settings = costCovariateSettings(
        useCosts = TRUE,
        temporalStartDays = c(-7, 0, 1, 8, 31),
        temporalEndDays = c(-1, 0, 7, 30, 90),
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeVisitCosts = TRUE,
        includeProcedureCosts = TRUE,
        stratifyByCostDomain = TRUE,
        aggregateMethod = "sum"
      ),
      description = "Short-term cost analysis around acute medical events",
      recommendedCohorts = c("MI", "Stroke", "Pneumonia", "Fractures")
    ),
    preventiveCare = list(
      settings = costCovariateSettings(
        useCosts = TRUE,
        temporalStartDays = c(-365, 0, 1),
        temporalEndDays = c(-1, 0, 365),
        includeMedicalCosts = TRUE,
        includePharmacyCosts = FALSE,
        includeProcedureCosts = TRUE,
        includeVisitCosts = TRUE,
        stratifyByAgeGroup = TRUE,
        stratifyByGender = TRUE,
        useCostVisitCounts = TRUE,
        aggregateMethod = "sum"
      ),
      description = "Cost analysis for preventive care services and screenings",
      recommendedCohorts = c("Cancer screening", "Vaccinations", "Annual wellness visits")
    ),
    emergencyDepartment = list(
      settings = costCovariateSettings(
        useCosts = TRUE,
        temporalStartDays = c(-30, -7, 0, 1, 8),
        temporalEndDays = c(-8, -1, 0, 7, 30),
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeVisitCosts = TRUE,
        includeProcedureCosts = TRUE,
        stratifyByCostType = TRUE,
        useCostUtilization = TRUE,
        aggregateMethod = "sum"
      ),
      description = "ED visit cost analysis including pre and post visit costs",
      recommendedCohorts = c("ED visits", "Urgent care visits", "Observation stays")
    ),
    hospitalization = list(
      settings = costCovariateSettings(
        useCosts = TRUE,
        temporalStartDays = c(-30, 0, 1, 31, 91),
        temporalEndDays = c(-1, 0, 30, 90, 180),
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = TRUE,
        includeVisitCosts = TRUE,
        includeDeviceCosts = TRUE,
        stratifyByCostDomain = TRUE,
        stratifyByCostType = TRUE,
        includeOutlierAnalysis = TRUE,
        outlierLowerQuantile = 0.05,
        outlierUpperQuantile = 0.95,
        aggregateMethod = "sum"
      ),
      description = "Comprehensive inpatient stay cost analysis",
      recommendedCohorts = c("Hospital admissions", "ICU stays", "Surgical admissions")
    ),
    specialty = list(
      settings = costCovariateSettings(
        useCosts = TRUE,
        temporalStartDays = c(-180, -90, -30, 0, 1, 31, 91),
        temporalEndDays = c(-91, -31, -1, 0, 30, 90, 180),
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = TRUE,
        stratifyByCostDomain = TRUE,
        useCostVisitCounts = TRUE,
        aggregateMethod = "sum"
      ),
      description = "Specialty care cost tracking and referral patterns",
      recommendedCohorts = c("Oncology visits", "Cardiology visits", "Rheumatology visits")
    ),
    valueBasedCare = list(
      settings = costCovariateSettings(
        useCosts = TRUE,
        useCostDemographics = TRUE,
        useCostVisitCounts = TRUE,
        useCostUtilization = TRUE,
        temporalStartDays = c(-365, -180, -90, 0, 1, 91, 181),
        temporalEndDays = c(-181, -91, -1, 0, 90, 180, 365),
        includeMedicalCosts = TRUE,
        includePharmacyCosts = TRUE,
        includeProcedureCosts = TRUE,
        includeVisitCosts = TRUE,
        stratifyByAgeGroup = TRUE,
        stratifyByGender = TRUE,
        stratifyByCostDomain = TRUE,
        stratifyByCostType = TRUE,
        includeTimeDistribution = TRUE,
        includeOutlierAnalysis = TRUE,
        aggregateMethod = "sum"
      ),
      description = "Comprehensive cost and utilization analysis for value-based contracts",
      recommendedCohorts = c("ACO populations", "Bundle payment cohorts", "Risk-adjusted populations")
    )
  )

  return(template)
}
