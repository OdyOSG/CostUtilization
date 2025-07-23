# R/CostCovariateSettings.R

#' Create covariate settings for cost analysis
#'
#' @description
#' Creates comprehensive settings for extracting cost-based covariates. These settings
#' can be used to analyze various cost metrics across different dimensions and time windows.
#'
#' @param useCosts Logical indicating whether to extract cost covariates
#' @param useCostDemographics Logical indicating whether to include demographic-stratified costs
#' @param useCostVisitCounts Logical indicating whether to include visit count-based cost metrics
#' @param useCostUtilization Logical indicating whether to include utilization-based cost metrics
#' @param temporalStartDays A vector of integers specifying the start of time windows
#'   (days relative to cohort start date). Negative values indicate days before.
#' @param temporalEndDays A vector of integers specifying the end of time windows
#'   (days relative to cohort start date). Must be same length as temporalStartDays.
#' @param includedCovariateConceptIds A vector of concept IDs to specifically include.
#'   If NULL, includes all concepts.
#' @param excludedCovariateConceptIds A vector of concept IDs to specifically exclude.
#' @param includedCovariateIds A vector of covariate IDs to specifically include.
#' @param costDomainIds A vector of strings specifying which cost domains to include
#'   (e.g., "Drug", "Procedure", "Visit", "Device", "Measurement", "Observation").
#' @param costTypeConceptIds A vector of concept IDs specifying cost types to include
#'   (e.g., 44818668 for allowed amount, 44818669 for paid amount).
#' @param currencyConceptIds A vector of concept IDs for specific currencies
#'   (e.g., 44818568 for USD). If NULL, includes all currencies.
#' @param includeMedicalCosts Logical indicating whether to include medical costs
#' @param includePharmacyCosts Logical indicating whether to include pharmacy costs
#' @param includeProcedureCosts Logical indicating whether to include procedure-specific costs
#' @param includeDrugCosts Logical indicating whether to include drug-specific costs
#' @param includeVisitCosts Logical indicating whether to include visit-specific costs
#' @param includeDeviceCosts Logical indicating whether to include device-specific costs
#' @param aggregateMethod Method for aggregating costs: "sum", "mean", "median", "min", "max"
#' @param stratifyByAgeGroup Logical indicating whether to stratify costs by age groups
#' @param stratifyByGender Logical indicating whether to stratify costs by gender
#' @param stratifyByCostDomain Logical indicating whether to create separate covariates by domain
#' @param stratifyByCostType Logical indicating whether to create separate covariates by cost type
#' @param includeTimeDistribution Logical indicating whether to include temporal distribution metrics
#' @param includeOutlierAnalysis Logical indicating whether to include outlier detection metrics
#' @param outlierLowerQuantile Lower quantile for outlier detection (default 0.01)
#' @param outlierUpperQuantile Upper quantile for outlier detection (default 0.99)
#' @param addDescendantsToIncludedCovariateConceptIds Logical indicating whether to include
#'   descendant concepts
#' @param addDescendantsToExcludedCovariateConceptIds Logical indicating whether to exclude
#'   descendant concepts
#'
#' @return
#' An object of type \code{costCovariateSettings}, to be used with other functions
#' in the CostUtilization package.
#'
#' @examples
#' \dontrun{
#' # Create settings for basic cost analysis
#' settings <- costCovariateSettings(
#'   useCosts = TRUE,
#'   temporalStartDays = c(-365, -180, -30, 0),
#'   temporalEndDays = c(-1, -1, -1, 0),
#'   includeMedicalCosts = TRUE,
#'   includePharmacyCosts = TRUE
#' )
#'
#' # Create settings for specific cost types
#' settings <- costCovariateSettings(
#'   useCosts = TRUE,
#'   costTypeConceptIds = c(44818668, 44818669), # allowed and paid amounts
#'   stratifyByCostDomain = TRUE,
#'   aggregateMethod = "sum"
#' )
#' }
#'
#' @export
costCovariateSettings <- function(useCosts = TRUE,
                                  useCostDemographics = FALSE,
                                  useCostVisitCounts = FALSE,
                                  useCostUtilization = FALSE,
                                  temporalStartDays = c(-365, -180, -30, 0, 1),
                                  temporalEndDays = c(-1, -1, -1, 0, 365),
                                  includedCovariateConceptIds = NULL,
                                  excludedCovariateConceptIds = NULL,
                                  includedCovariateIds = NULL,
                                  costDomainIds = NULL,
                                  costTypeConceptIds = NULL,
                                  currencyConceptIds = NULL,
                                  includeMedicalCosts = TRUE,
                                  includePharmacyCosts = TRUE,
                                  includeProcedureCosts = TRUE,
                                  includeDrugCosts = TRUE,
                                  includeVisitCosts = TRUE,
                                  includeDeviceCosts = FALSE,
                                  aggregateMethod = "sum",
                                  stratifyByAgeGroup = FALSE,
                                  stratifyByGender = FALSE,
                                  stratifyByCostDomain = TRUE,
                                  stratifyByCostType = FALSE,
                                  includeTimeDistribution = FALSE,
                                  includeOutlierAnalysis = FALSE,
                                  outlierLowerQuantile = 0.01,
                                  outlierUpperQuantile = 0.99,
                                  addDescendantsToIncludedCovariateConceptIds = FALSE,
                                  addDescendantsToExcludedCovariateConceptIds = FALSE) {
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

  if (includeOutlierAnalysis) {
    if (outlierLowerQuantile < 0 || outlierLowerQuantile > 1) {
      stop("outlierLowerQuantile must be between 0 and 1")
    }
    if (outlierUpperQuantile < 0 || outlierUpperQuantile > 1) {
      stop("outlierUpperQuantile must be between 0 and 1")
    }
    if (outlierLowerQuantile >= outlierUpperQuantile) {
      stop("outlierLowerQuantile must be less than outlierUpperQuantile")
    }
  }

  # Create covariate concept sets based on settings
  covariateConceptSets <- list()

  # Medical costs
  if (includeMedicalCosts) {
    covariateConceptSets$medicalCosts <- list(
      covariateId = 1001,
      covariateName = "Medical costs",
      conceptIds = NULL, # Will be filtered by domain
      domains = c("Visit", "Procedure", "Measurement", "Observation")
    )
  }

  # Pharmacy costs
  if (includePharmacyCosts) {
    covariateConceptSets$pharmacyCosts <- list(
      covariateId = 1002,
      covariateName = "Pharmacy costs",
      conceptIds = NULL,
      domains = c("Drug")
    )
  }

  # Procedure costs
  if (includeProcedureCosts) {
    covariateConceptSets$procedureCosts <- list(
      covariateId = 1003,
      covariateName = "Procedure costs",
      conceptIds = NULL,
      domains = c("Procedure")
    )
  }

  # Drug costs
  if (includeDrugCosts) {
    covariateConceptSets$drugCosts <- list(
      covariateId = 1004,
      covariateName = "Drug costs",
      conceptIds = NULL,
      domains = c("Drug")
    )
  }

  # Visit costs
  if (includeVisitCosts) {
    covariateConceptSets$visitCosts <- list(
      covariateId = 1005,
      covariateName = "Visit costs",
      conceptIds = NULL,
      domains = c("Visit")
    )
  }

  # Device costs
  if (includeDeviceCosts) {
    covariateConceptSets$deviceCosts <- list(
      covariateId = 1006,
      covariateName = "Device costs",
      conceptIds = NULL,
      domains = c("Device")
    )
  }

  # Create the settings object
  settings <- list(
    useCosts = useCosts,
    useCostDemographics = useCostDemographics,
    useCostVisitCounts = useCostVisitCounts,
    useCostUtilization = useCostUtilization,
    temporalStartDays = temporalStartDays,
    temporalEndDays = temporalEndDays,
    includedCovariateConceptIds = includedCovariateConceptIds,
    excludedCovariateConceptIds = excludedCovariateConceptIds,
    includedCovariateIds = includedCovariateIds,
    costDomainIds = costDomainIds,
    costTypeConceptIds = costTypeConceptIds,
    currencyConceptIds = currencyConceptIds,
    covariateConceptSets = covariateConceptSets,
    aggregateMethod = aggregateMethod,
    stratifyByAgeGroup = stratifyByAgeGroup,
    stratifyByGender = stratifyByGender,
    stratifyByCostDomain = stratifyByCostDomain,
    stratifyByCostType = stratifyByCostType,
    includeTimeDistribution = includeTimeDistribution,
    includeOutlierAnalysis = includeOutlierAnalysis,
    outlierLowerQuantile = outlierLowerQuantile,
    outlierUpperQuantile = outlierUpperQuantile,
    addDescendantsToIncludedCovariateConceptIds = addDescendantsToIncludedCovariateConceptIds,
    addDescendantsToExcludedCovariateConceptIds = addDescendantsToExcludedCovariateConceptIds
  )

  class(settings) <- "costCovariateSettings"
  return(settings)
}

#' Check if settings are cost covariate settings
#'
#' @param object Object to check
#' @return Logical indicating if object is costCovariateSettings
#' @export
isCostCovariateSettings <- function(object) {
  inherits(object, "costCovariateSettings")
}

#' Print cost covariate settings
#'
#' @param x costCovariateSettings object
#' @param ... Additional arguments (ignored)
#' @return Invisibly returns the input
#' @export
print.costCovariateSettings <- function(x, ...) {
  writeLines("Cost Covariate Settings:")
  writeLines(paste("  Use costs:", x$useCosts))
  writeLines(paste("  Use cost demographics:", x$useCostDemographics))
  writeLines(paste("  Use cost visit counts:", x$useCostVisitCounts))
  writeLines(paste("  Use cost utilization:", x$useCostUtilization))
  
  if (length(x$temporalStartDays) > 0) {
    writeLines("\nTemporal windows:")
    for (i in seq_along(x$temporalStartDays)) {
      writeLines(paste0("  Window ", i, ": [", x$temporalStartDays[i], ", ", x$temporalEndDays[i], "]"))
    }
  }
  
  if (length(x$covariateConceptSets) > 0) {
    writeLines(paste("\nCost categories included:", length(x$covariateConceptSets)))
  }
  
  writeLines(paste("\nAggregation method:", x$aggregateMethod))
  
  if (any(x$stratifyByAgeGroup, x$stratifyByGender, x$stratifyByCostDomain, x$stratifyByCostType)) {
    writeLines("\nStratification:")
    if (x$stratifyByAgeGroup) writeLines("  - By age group")
    if (x$stratifyByGender) writeLines("  - By gender")
    if (x$stratifyByCostDomain) writeLines("  - By cost domain")
    if (x$stratifyByCostType) writeLines("  - By cost type")
  }
  
  invisible(x)
}