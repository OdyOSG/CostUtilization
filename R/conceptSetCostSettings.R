#' Create cost covariate settings from concept set
#'
#' @description
#' Creates cost covariate settings based on a specific concept set, useful for
#' analyzing costs associated with specific conditions, procedures, or drugs.
#'
#' @param conceptSetId Unique identifier for the concept set
#' @param conceptSetName Name of the concept set
#' @param conceptIds Vector of concept IDs to include
#' @param excludeDescendants Whether to exclude descendant concepts
#' @param includedCovariateIds Vector of covariate IDs to generate
#' @param temporalStartDays Start days for temporal windows
#' @param temporalEndDays End days for temporal windows
#' @param aggregateMethod Method for aggregating costs
#' @param stratifyByCostType Whether to stratify by cost type
#' @param analysisName Optional custom name for the analysis
#'
#' @return
#' An object of type \code{costCovariateSettings}
#'
#' @export
createCostCovariateSettingsFromConceptSet <- function(conceptSetId,
                                                      conceptSetName,
                                                      conceptIds,
                                                      excludeDescendants = FALSE,
                                                      includedCovariateIds = NULL,
                                                      temporalStartDays = c(-365, -180, -30, 0),
                                                      temporalEndDays = c(-1, -1, -1, 0),
                                                      aggregateMethod = "sum",
                                                      stratifyByCostType = FALSE,
                                                      analysisName = NULL) {
  if (is.null(analysisName)) {
    analysisName <- paste("Costs for", conceptSetName)
  }

  # Create analyses for each temporal window
  analyses <- data.frame(
    analysisId = seq_along(temporalStartDays),
    analysisName = paste(
      analysisName,
      ifelse(temporalStartDays < 0,
        paste(abs(temporalStartDays), "days prior"),
        ifelse(temporalStartDays == 0 & temporalEndDays == 0,
          "on index",
          paste(temporalEndDays, "days after")
        )
      )
    ),
    startDay = temporalStartDays,
    endDay = temporalEndDays,
    isBinary = FALSE,
    missingMeansZero = TRUE,
    aggregateMethod = aggregateMethod,
    stringsAsFactors = FALSE
  )

  # Create concept set
  conceptSet <- list(
    conceptSetId = conceptSetId,
    conceptSetName = conceptSetName,
    conceptIds = conceptIds,
    excludeDescendants = excludeDescendants
  )

  # Generate covariate IDs if not provided
  if (is.null(includedCovariateIds)) {
    baseId <- conceptSetId * 10000
    includedCovariateIds <- baseId + analyses$analysisId

    if (stratifyByCostType) {
      # Add additional IDs for cost type stratification
      costTypeOffset <- 1000
      includedCovariateIds <- c(
        includedCovariateIds,
        includedCovariateIds + costTypeOffset
      )
    }
  }

  settings <- list(
    useCosts = TRUE,
    analyses = analyses,
    covariateIds = includedCovariateIds,
    conceptSets = list(conceptSet),
    includedCovariateConceptIds = conceptIds,
    temporalStartDays = temporalStartDays,
    temporalEndDays = temporalEndDays,
    aggregateMethod = aggregateMethod,
    stratifyByCostType = stratifyByCostType,
    addDescendantsToIncludedCovariateConceptIds = !excludeDescendants
  )

  class(settings) <- "costCovariateSettings"
  return(settings)
}

#' Combine multiple cost covariate settings
#'
#' @description
#' Combines multiple cost covariate settings objects into a single settings object.
#'
#' @param ... Multiple costCovariateSettings objects to combine
#'
#' @return
#' A combined costCovariateSettings object
#'
#' @export
combineCostCovariateSettings <- function(...) {
  settingsList <- list(...)

  # Verify all inputs are costCovariateSettings
  if (!all(sapply(settingsList, isCostCovariateSettings))) {
    stop("All inputs must be costCovariateSettings objects")
  }

  # Initialize combined settings with first object
  combinedSettings <- settingsList[[1]]

  if (length(settingsList) > 1) {
    for (i in 2:length(settingsList)) {
      currentSettings <- settingsList[[i]]

      # Combine analyses
      if (!is.null(currentSettings$analyses)) {
        maxId <- max(combinedSettings$analyses$analysisId, na.rm = TRUE)
        currentSettings$analyses$analysisId <- currentSettings$analyses$analysisId + maxId
        combinedSettings$analyses <- rbind(
          combinedSettings$analyses,
          currentSettings$analyses
        )
      }

      # Combine covariate IDs
      combinedSettings$covariateIds <- c(
        combinedSettings$covariateIds,
        currentSettings$covariateIds
      )

      # Combine concept sets
      if (!is.null(currentSettings$conceptSets)) {
        combinedSettings$conceptSets <- c(
          combinedSettings$conceptSets,
          currentSettings$conceptSets
        )
      }

      # Combine included/excluded concept IDs
      combinedSettings$includedCovariateConceptIds <- unique(c(
        combinedSettings$includedCovariateConceptIds,
        currentSettings$includedCovariateConceptIds
      ))

      combinedSettings$excludedCovariateConceptIds <- unique(c(
        combinedSettings$excludedCovariateConceptIds,
        currentSettings$excludedCovariateConceptIds
      ))

      # Combine temporal windows (take union of unique combinations)
      allStartDays <- unique(c(
        combinedSettings$temporalStartDays,
        currentSettings$temporalStartDays
      ))
      allEndDays <- unique(c(
        combinedSettings$temporalEndDays,
        currentSettings$temporalEndDays
      ))

      # Create all combinations
      temporalCombos <- expand.grid(start = allStartDays, end = allEndDays)
      temporalCombos <- temporalCombos[temporalCombos$start <= temporalCombos$end, ]

      combinedSettings$temporalStartDays <- temporalCombos$start
      combinedSettings$temporalEndDays <- temporalCombos$end

      # Combine flags (use OR logic)
      booleanFields <- c(
        "useCosts", "useCostDemographics", "useCostVisitCounts",
        "useCostUtilization", "includeMedicalCosts", "includePharmacyCosts",
        "includeProcedureCosts", "includeDrugCosts", "includeVisitCosts",
        "includeDeviceCosts", "stratifyByAgeGroup", "stratifyByGender",
        "stratifyByCostDomain", "stratifyByCostType", "includeTimeDistribution",
        "includeOutlierAnalysis"
      )

      for (field in booleanFields) {
        if (!is.null(combinedSettings[[field]]) && !is.null(currentSettings[[field]])) {
          combinedSettings[[field]] <- combinedSettings[[field]] || currentSettings[[field]]
        }
      }
    }
  }

  class(combinedSettings) <- "costCovariateSettings"
  return(combinedSettings)
}

#' Validate cost covariate settings
#'
#' @description
#' Validates cost covariate settings to ensure they are properly configured.
#'
#' @param settings A costCovariateSettings object
#' @param connection Optional database connection for checking concept validity
#' @param cdmDatabaseSchema Optional CDM database schema
#'
#' @return
#' TRUE if valid, otherwise throws an error with details
#'
#' @export
validateCostCovariateSettings <- function(settings,
                                          connection = NULL,
                                          cdmDatabaseSchema = NULL) {
  if (!isCostCovariateSettings(settings)) {
    stop("Input must be a costCovariateSettings object")
  }

  # Check temporal windows
  if (length(settings$temporalStartDays) != length(settings$temporalEndDays)) {
    stop("temporalStartDays and temporalEndDays must have the same length")
  }

  if (any(settings$temporalStartDays > settings$temporalEndDays)) {
    stop("All temporalStartDays must be <= corresponding temporalEndDays")
  }

  # Check analyses if present
  if (!is.null(settings$analyses)) {
    requiredColumns <- c("analysisId", "analysisName", "startDay", "endDay")
    missingColumns <- setdiff(requiredColumns, names(settings$analyses))
    if (length(missingColumns) > 0) {
      stop(paste("analyses missing required columns:", paste(missingColumns, collapse = ", ")))
    }

    if (any(duplicated(settings$analyses$analysisId))) {
      stop("Duplicate analysis IDs found")
    }
  }

  # Check concept IDs if database connection provided
  if (!is.null(connection) && !is.null(cdmDatabaseSchema)) {
    # Check cost type concept IDs
    if (!is.null(settings$costTypeConceptIds)) {
      sql <- "SELECT concept_id
              FROM @cdm_database_schema.concept
              WHERE concept_id IN (@concept_ids)
              AND domain_id = 'Cost Type'
              AND invalid_reason IS NULL"

      sql <- SqlRender::render(sql,
        cdm_database_schema = cdmDatabaseSchema,
        concept_ids = settings$costTypeConceptIds
      )

      validConceptIds <- DatabaseConnector::querySql(connection, sql)
      invalidIds <- setdiff(settings$costTypeConceptIds, validConceptIds$CONCEPT_ID)

      if (length(invalidIds) > 0) {
        warning(paste("Invalid cost type concept IDs:", paste(invalidIds, collapse = ", ")))
      }
    }

    # Check currency concept IDs
    if (!is.null(settings$currencyConceptIds)) {
      sql <- "SELECT concept_id
              FROM @cdm_database_schema.concept
              WHERE concept_id IN (@concept_ids)
              AND domain_id = 'Currency'
              AND invalid_reason IS NULL"

      sql <- SqlRender::render(sql,
        cdm_database_schema = cdmDatabaseSchema,
        concept_ids = settings$currencyConceptIds
      )

      validConceptIds <- DatabaseConnector::querySql(connection, sql)
      invalidIds <- setdiff(settings$currencyConceptIds, validConceptIds$CONCEPT_ID)

      if (length(invalidIds) > 0) {
        warning(paste("Invalid currency concept IDs:", paste(invalidIds, collapse = ", ")))
      }
    }
  }

  message("Cost covariate settings validated successfully")
  return(TRUE)
}
