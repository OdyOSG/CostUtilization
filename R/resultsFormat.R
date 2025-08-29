#' Create Cost Covariate Data in FeatureExtraction Format
#'
#' @description
#' Converts cost analysis results from `calculateCostOfCare()` into a standardized
#' `CovariateData` object compatible with the FeatureExtraction package and broader
#' OHDSI ecosystem. Handles both aggregated and person-level result formats automatically.
#'
#' @param costResults An Andromeda object returned from `calculateCostOfCare()`.
#'   Can contain either:
#'   - **Aggregated format**: `metric_type`, `metric_name`, `metric_value` columns
#'   - **Person-level format**: `person_id`, `cost`, `adjusted_cost` columns
#' @param costOfCareSettings A `CostOfCareSettings` object used in the analysis.
#' @param cohortId Integer cohort ID for the analysis.
#' @param databaseId Character database identifier for metadata.
#' @param analysisId Integer analysis ID for systematic covariate numbering.
#'   Default: 1000L.
#' @param temporalCovariateSettings Optional temporal covariate settings for
#'   time-windowed analyses.
#'
#' @return A `CovariateData` object (Andromeda-backed) with components:
#'   - `covariates`: Person-level cost and utilization metrics
#'   - `covariateRef`: Reference table describing each covariate
#'   - `analysisRef`: Analysis-level metadata and parameters
#'   - `timeRef`: Time window reference (if temporal analysis)
#'   - `metaData`: Comprehensive analysis metadata
#'
#' @details
#' ## Automatic Format Detection
#' 
#' The function automatically detects whether results are in aggregated or 
#' person-level format by examining column names:
#' 
#' - **Aggregated**: Contains `metric_type`, `metric_name`, `metric_value`
#' - **Person-level**: Contains `person_id`, `cost`, and optionally `adjusted_cost`
#' 
#' ## Covariate ID Generation
#' 
#' Systematic covariate IDs are generated using the formula:
#' `analysisId * 1000 + typeOffset + metricOffset`
#' 
#' Where:
#' - `typeOffset`: 100 (visit-level) or 200 (line-level)  
#' - `metricOffset`: 1-50 for different cost/utilization metrics
#' 
#' ## Supported Metrics
#' 
#' ### Cost Metrics
#' - Total cost/charge
#' - CPI-adjusted costs (if enabled)
#' - Per-person-per-month (PPPM)
#' - Per-person-per-year (PPPY)
#' 
#' ### Utilization Metrics
#' - Visit counts
#' - Event counts  
#' - Rates per 1000 person-years
#' 
#' @examples
#' \dontrun{
#' # Basic usage
#' settings <- createCostOfCareSettings(costConceptId = 31973L)
#' results <- calculateCostOfCare(...)
#' 
#' covariateData <- createCostCovariateData(
#'   costResults = results,
#'   costOfCareSettings = settings,
#'   cohortId = 1,
#'   databaseId = "MyDatabase"
#' )
#' 
#' # View summary
#' summary(covariateData)
#' 
#' # Use in OHDSI pipelines
#' runCmAnalyses(..., covariateSettings = covariateData)
#' }
#'
#' @export
createCostCovariateData <- function(costResults,
                                   costOfCareSettings,
                                   cohortId,
                                   databaseId,
                                   analysisId = 1000L,
                                   temporalCovariateSettings = NULL) {
  
  # Input validation
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(costResults, "Andromeda", add = errorMessages)
  checkmate::assertClass(costOfCareSettings, "CostOfCareSettings", add = errorMessages)
  checkmate::assertIntegerish(cohortId, len = 1, add = errorMessages)
  checkmate::assertCharacter(databaseId, len = 1, add = errorMessages)
  checkmate::assertIntegerish(analysisId, len = 1, add = errorMessages)
  checkmate::reportAssertions(errorMessages)
  
  # Detect result format by examining available tables and columns
  resultFormat <- .detectResultFormat(costResults)
  
  # Convert based on detected format
  if (resultFormat == "aggregated") {
    covariateData <- .convertAggregatedResults(
      costResults = costResults,
      costOfCareSettings = costOfCareSettings,
      cohortId = cohortId,
      databaseId = databaseId,
      analysisId = analysisId,
      temporalCovariateSettings = temporalCovariateSettings
    )
  } else if (resultFormat == "person_level") {
    covariateData <- .convertPersonLevelResults(
      costResults = costResults,
      costOfCareSettings = costOfCareSettings,
      cohortId = cohortId,
      databaseId = databaseId,
      analysisId = analysisId,
      temporalCovariateSettings = temporalCovariateSettings
    )
  } else {
    cli::cli_abort("Unable to detect valid result format. Expected either aggregated or person-level format.")
  }
  
  return(covariateData)
}

#' Convenience Function for FeatureExtraction Format Conversion
#'
#' @description
#' A simplified wrapper around `createCostCovariateData()` for quick conversion
#' of cost analysis results to FeatureExtraction format.
#'
#' @inheritParams createCostCovariateData
#'
#' @return A `CovariateData` object compatible with FeatureExtraction package.
#'
#' @export
convertToFeatureExtractionFormat <- function(costResults,
                                            costOfCareSettings,
                                            cohortId,
                                            databaseId = "Unknown",
                                            analysisId = 1000L) {
  createCostCovariateData(
    costResults = costResults,
    costOfCareSettings = costOfCareSettings,
    cohortId = cohortId,
    databaseId = databaseId,
    analysisId = analysisId
  )
}

# ============================================================================
# Internal Helper Functions
# ============================================================================

#' Detect Result Format from Andromeda Object
#' @param costResults Andromeda object from calculateCostOfCare()
#' @return Character: "aggregated", "person_level", or "unknown"
#' @keywords internal
.detectResultFormat <- function(costResults) {
  
  # Check if results table exists
  if (!"results" %in% names(costResults)) {
    return("unknown")
  }
  
  # Get column names from results table
  resultsCols <- names(costResults$results)
  
  # Check for aggregated format columns
  aggregatedCols <- c("metric_type", "metric_name", "metric_value")
  hasAggregated <- all(aggregatedCols %in% resultsCols)
  
  # Check for person-level format columns  
  personLevelCols <- c("person_id", "cost")
  hasPersonLevel <- all(personLevelCols %in% resultsCols)
  
  if (hasAggregated) {
    return("aggregated")
  } else if (hasPersonLevel) {
    return("person_level")
  } else {
    return("unknown")
  }
}

#' Convert Aggregated Results to FeatureExtraction Format
#' @inheritParams createCostCovariateData
#' @return CovariateData object
#' @keywords internal
.convertAggregatedResults <- function(costResults,
                                     costOfCareSettings,
                                     cohortId,
                                     databaseId,
                                     analysisId,
                                     temporalCovariateSettings) {
  
  # Extract results as tibble for processing
  resultsData <- costResults$results %>%
    dplyr::collect() %>%
    dplyr::as_tibble()
  
  # Create systematic covariate mappings
  covariateMapping <- .createCovariateMapping(
    resultsData = resultsData,
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings
  )
  
  # Generate person-level covariates from aggregated data
  # For aggregated data, we create population-level metrics as covariates
  covariates <- .generateCovariatesFromAggregated(
    resultsData = resultsData,
    covariateMapping = covariateMapping,
    cohortId = cohortId
  )
  
  # Create covariate reference
  covariateRef <- .createCovariateRef(
    covariateMapping = covariateMapping,
    costOfCareSettings = costOfCareSettings
  )
  
  # Create analysis reference
  analysisRef <- .createAnalysisRef(
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings,
    databaseId = databaseId
  )
  
  # Create time reference (if temporal)
  timeRef <- .createTimeRef(temporalCovariateSettings)
  
  # Create metadata
  metaData <- .createMetaData(
    costOfCareSettings = costOfCareSettings,
    cohortId = cohortId,
    databaseId = databaseId,
    analysisId = analysisId,
    resultFormat = "aggregated"
  )
  
  # Assemble CovariateData object
  covariateData <- .assembleCovariateData(
    covariates = covariates,
    covariateRef = covariateRef,
    analysisRef = analysisRef,
    timeRef = timeRef,
    metaData = metaData
  )
  
  return(covariateData)
}

#' Convert Person-Level Results to FeatureExtraction Format
#' @inheritParams createCostCovariateData
#' @return CovariateData object
#' @keywords internal
.convertPersonLevelResults <- function(costResults,
                                      costOfCareSettings,
                                      cohortId,
                                      databaseId,
                                      analysisId,
                                      temporalCovariateSettings) {
  
  # Extract results as tibble for processing
  resultsData <- costResults$results %>%
    dplyr::collect() %>%
    dplyr::as_tibble()
  
  # Create systematic covariate mappings
  covariateMapping <- .createPersonLevelCovariateMapping(
    resultsData = resultsData,
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings
  )
  
  # Generate person-level covariates
  covariates <- .generateCovariatesFromPersonLevel(
    resultsData = resultsData,
    covariateMapping = covariateMapping,
    cohortId = cohortId
  )
  
  # Create covariate reference
  covariateRef <- .createCovariateRef(
    covariateMapping = covariateMapping,
    costOfCareSettings = costOfCareSettings
  )
  
  # Create analysis reference
  analysisRef <- .createAnalysisRef(
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings,
    databaseId = databaseId
  )
  
  # Create time reference (if temporal)
  timeRef <- .createTimeRef(temporalCovariateSettings)
  
  # Create metadata
  metaData <- .createMetaData(
    costOfCareSettings = costOfCareSettings,
    cohortId = cohortId,
    databaseId = databaseId,
    analysisId = analysisId,
    resultFormat = "person_level"
  )
  
  # Assemble CovariateData object
  covariateData <- .assembleCovariateData(
    covariates = covariates,
    covariateRef = covariateRef,
    analysisRef = analysisRef,
    timeRef = timeRef,
    metaData = metaData
  )
  
  return(covariateData)
}

#' Create Covariate Mapping for Aggregated Results
#' @param resultsData Tibble of aggregated results
#' @param analysisId Analysis ID for covariate numbering
#' @param costOfCareSettings Settings object
#' @return Tibble with covariate mappings
#' @keywords internal
.createCovariateMapping <- function(resultsData, analysisId, costOfCareSettings) {
  
  # Determine type offset based on metric_type
  metricType <- unique(resultsData$metric_type)[1]
  typeOffset <- dplyr::case_when(
    metricType == "visit_level" ~ 100L,
    metricType == "line_level" ~ 200L,
    .default = 100L
  )
  
  # Create mapping for each unique metric
  uniqueMetrics <- resultsData %>%
    dplyr::distinct(metric_name) %>%
    dplyr::arrange(metric_name) %>%
    dplyr::mutate(
      metric_offset = dplyr::row_number(),
      covariate_id = analysisId * 1000L + typeOffset + metric_offset,
      covariate_name = .formatCovariateName(metric_name, costOfCareSettings),
      analysis_id = analysisId
    )
  
  return(uniqueMetrics)
}

#' Create Covariate Mapping for Person-Level Results
#' @param resultsData Tibble of person-level results
#' @param analysisId Analysis ID for covariate numbering
#' @param costOfCareSettings Settings object
#' @return Tibble with covariate mappings
#' @keywords internal
.createPersonLevelCovariateMapping <- function(resultsData, analysisId, costOfCareSettings) {
  
  # Determine available cost columns
  costCols <- intersect(names(resultsData), c("cost", "adjusted_cost"))
  
  # Create mapping for each cost type
  covariateMapping <- purrr::map_dfr(seq_along(costCols), ~ {
    costCol <- costCols[.x]
    
    dplyr::tibble(
      metric_name = costCol,
      metric_offset = .x,
      covariate_id = analysisId * 1000L + 300L + .x, # 300 for person-level
      covariate_name = .formatCovariateName(costCol, costOfCareSettings),
      analysis_id = analysisId
    )
  })
  
  return(covariateMapping)
}

#' Generate Covariates from Aggregated Results
#' @param resultsData Aggregated results tibble
#' @param covariateMapping Covariate mapping tibble
#' @param cohortId Cohort ID
#' @return Tibble of covariates
#' @keywords internal
.generateCovariatesFromAggregated <- function(resultsData, covariateMapping, cohortId) {
  
  # Join results with mapping to get covariate IDs
  covariates <- resultsData %>%
    dplyr::inner_join(covariateMapping, by = "metric_name") %>%
    dplyr::transmute(
      rowId = cohortId, # For aggregated data, use cohortId as rowId
      covariateId = covariate_id,
      covariateValue = metric_value
    ) %>%
    dplyr::filter(!is.na(covariateValue), covariateValue != 0)
  
  return(covariates)
}

#' Generate Covariates from Person-Level Results
#' @param resultsData Person-level results tibble
#' @param covariateMapping Covariate mapping tibble
#' @param cohortId Cohort ID
#' @return Tibble of covariates
#' @keywords internal
.generateCovariatesFromPersonLevel <- function(resultsData, covariateMapping, cohortId) {
  
  # Reshape person-level data to long format
  costCols <- intersect(names(resultsData), c("cost", "adjusted_cost"))
  
  covariates <- resultsData %>%
    dplyr::select(person_id, dplyr::all_of(costCols)) %>%
    tidyr::pivot_longer(
      cols = dplyr::all_of(costCols),
      names_to = "metric_name",
      values_to = "covariateValue"
    ) %>%
    dplyr::inner_join(covariateMapping, by = "metric_name") %>%
    dplyr::transmute(
      rowId = person_id,
      covariateId = covariate_id,
      covariateValue = covariateValue
    ) %>%
    dplyr::filter(!is.na(covariateValue), covariateValue != 0)
  
  return(covariates)
}

#' Create Covariate Reference Table
#' @param covariateMapping Covariate mapping tibble
#' @param costOfCareSettings Settings object
#' @return Tibble of covariate references
#' @keywords internal
.createCovariateRef <- function(covariateMapping, costOfCareSettings) {
  
  covariateRef <- covariateMapping %>%
    dplyr::transmute(
      covariateId = covariate_id,
      covariateName = covariate_name,
      analysisId = analysis_id,
      conceptId = costOfCareSettings$costConceptId %||% 0L
    )
  
  return(covariateRef)
}

#' Create Analysis Reference Table
#' @param analysisId Analysis ID
#' @param costOfCareSettings Settings object
#' @param databaseId Database identifier
#' @return Tibble of analysis references
#' @keywords internal
.createAnalysisRef <- function(analysisId, costOfCareSettings, databaseId) {
  
  # Create descriptive analysis name
  analysisName <- .createAnalysisName(costOfCareSettings)
  
  analysisRef <- dplyr::tibble(
    analysisId = analysisId,
    analysisName = analysisName,
    domainId = "Cost",
    startDay = costOfCareSettings$startOffsetDays %||% 0L,
    endDay = costOfCareSettings$endOffsetDays %||% 365L,
    isBinary = "N",
    missingMeansZero = "Y"
  )
  
  return(analysisRef)
}

#' Create Time Reference Table
#' @param temporalCovariateSettings Temporal settings (optional)
#' @return Tibble of time references or NULL
#' @keywords internal
.createTimeRef <- function(temporalCovariateSettings) {
  
  if (is.null(temporalCovariateSettings)) {
    return(NULL)
  }
  
  # Create time reference based on temporal settings
  # This would be expanded based on actual temporal covariate requirements
  timeRef <- dplyr::tibble(
    timeId = 1L,
    startDay = temporalCovariateSettings$startDay %||% 0L,
    endDay = temporalCovariateSettings$endDay %||% 365L
  )
  
  return(timeRef)
}

#' Create Comprehensive Metadata
#' @param costOfCareSettings Settings object
#' @param cohortId Cohort ID
#' @param databaseId Database identifier
#' @param analysisId Analysis ID
#' @param resultFormat Result format type
#' @return List of metadata
#' @keywords internal
.createMetaData <- function(costOfCareSettings, cohortId, databaseId, analysisId, resultFormat) {
  
  metaData <- list(
    # Analysis provenance
    analysisId = analysisId,
    cohortId = cohortId,
    databaseId = databaseId,
    resultFormat = resultFormat,
    
    # Cost analysis settings
    costConceptId = costOfCareSettings$costConceptId,
    currencyConceptId = costOfCareSettings$currencyConceptId,
    cpiAdjustment = costOfCareSettings$cpiAdjustment,
    microCosting = costOfCareSettings$microCosting,
    
    # Time window
    anchorCol = costOfCareSettings$anchorCol,
    startOffsetDays = costOfCareSettings$startOffsetDays,
    endOffsetDays = costOfCareSettings$endOffsetDays,
    
    # Filtering
    hasVisitRestriction = costOfCareSettings$hasVisitRestriction,
    hasEventFilters = costOfCareSettings$hasEventFilters,
    nFilters = costOfCareSettings$nFilters %||% 0L,
    
    # Package info
    packageVersion = utils::packageVersion("CostUtilization"),
    creationTime = Sys.time(),
    
    # OHDSI compatibility
    populationSize = NA_integer_, # To be filled by calling function if known
    outcomeId = NA_integer_
  )
  
  return(metaData)
}

#' Assemble CovariateData Object
#' @param covariates Covariates tibble
#' @param covariateRef Covariate reference tibble
#' @param analysisRef Analysis reference tibble
#' @param timeRef Time reference tibble (optional)
#' @param metaData Metadata list
#' @return CovariateData object
#' @keywords internal
.assembleCovariateData <- function(covariates, covariateRef, analysisRef, timeRef, metaData) {
  
  # Create Andromeda object for scalable storage
  covariateData <- Andromeda::andromeda()
  
  # Add main tables
  covariateData$covariates <- covariates
  covariateData$covariateRef <- covariateRef
  covariateData$analysisRef <- analysisRef
  
  # Add time reference if present
  if (!is.null(timeRef)) {
    covariateData$timeRef <- timeRef
  }
  
  # Add metadata
  attr(covariateData, "metaData") <- metaData
  
  # Set class for S3 methods
  class(covariateData) <- c("CovariateData", "Andromeda", class(covariateData))
  
  return(covariateData)
}

#' Format Covariate Name
#' @param metricName Raw metric name
#' @param costOfCareSettings Settings object for context
#' @return Formatted covariate name
#' @keywords internal
.formatCovariateName <- function(metricName, costOfCareSettings) {
  
  # Get cost concept description
  costConceptId <- costOfCareSettings$costConceptId %||% 31973L
  costType <- dplyr::case_when(
    costConceptId == 31973L ~ "Total Charge",
    costConceptId == 31985L ~ "Total Cost", 
    costConceptId == 31980L ~ "Paid by Payer",
    costConceptId == 31981L ~ "Paid by Patient",
    costConceptId == 31974L ~ "Patient Copay",
    costConceptId == 31975L ~ "Patient Coinsurance",
    costConceptId == 31976L ~ "Patient Deductible",
    costConceptId == 31979L ~ "Amount Allowed",
    .default = paste("Cost Concept", costConceptId)
  )
  
  # Format metric name
  formattedName <- stringr::str_to_title(stringr::str_replace_all(metricName, "_", " "))
  
  # Combine with cost type
  covariateName <- paste(costType, "-", formattedName)
  
  # Add time window info
  startDays <- costOfCareSettings$startOffsetDays %||% 0L
  endDays <- costOfCareSettings$endOffsetDays %||% 365L
  timeWindow <- paste0("(", startDays, " to ", endDays, " days)")
  
  finalName <- paste(covariateName, timeWindow)
  
  return(finalName)
}

#' Create Analysis Name
#' @param costOfCareSettings Settings object
#' @return Descriptive analysis name
#' @keywords internal
.createAnalysisName <- function(costOfCareSettings) {
  
  # Base name
  baseName <- "Cost Analysis"
  
  # Add cost type
  costConceptId <- costOfCareSettings$costConceptId %||% 31973L
  costType <- dplyr::case_when(
    costConceptId == 31973L ~ "Total Charge",
    costConceptId == 31985L ~ "Total Cost",
    costConceptId == 31980L ~ "Paid by Payer", 
    costConceptId == 31981L ~ "Paid by Patient",
    .default = paste("Concept", costConceptId)
  )
  
  # Add modifiers
  modifiers <- c()
  
  if (isTRUE(costOfCareSettings$cpiAdjustment)) {
    modifiers <- c(modifiers, "CPI-Adjusted")
  }
  
  if (isTRUE(costOfCareSettings$microCosting)) {
    modifiers <- c(modifiers, "Line-Level")
  }
  
  if (isTRUE(costOfCareSettings$hasEventFilters)) {
    modifiers <- c(modifiers, "Event-Filtered")
  }
  
  if (isTRUE(costOfCareSettings$hasVisitRestriction)) {
    modifiers <- c(modifiers, "Visit-Restricted")
  }
  
  # Combine components
  components <- c(baseName, costType)
  if (length(modifiers) > 0) {
    components <- c(components, paste0("(", paste(modifiers, collapse = ", "), ")"))
  }
  
  analysisName <- paste(components, collapse = " - ")
  
  return(analysisName)
}

# ============================================================================
# S3 Methods for CovariateData
# ============================================================================

#' Summary Method for CovariateData
#' @param object CovariateData object
#' @param ... Additional arguments (unused)
#' @return Summary information
#' @export
summary.CovariateData <- function(object, ...) {
  
  metaData <- attr(object, "metaData")
  
  # Count covariates and people
  nCovariates <- object$covariateRef %>% dplyr::tally() %>% dplyr::pull(n)
  nPeople <- object$covariates %>% dplyr::distinct(rowId) %>% dplyr::tally() %>% dplyr::pull(n)
  nValues <- object$covariates %>% dplyr::tally() %>% dplyr::pull(n)
  
  cat("CovariateData Summary\n")
  cat("====================\n\n")
  
  cat("Analysis Information:\n")
  cat("  Analysis ID:", metaData$analysisId %||% "Unknown", "\n")
  cat("  Cohort ID:", metaData$cohortId %||% "Unknown", "\n")
  cat("  Database:", metaData$databaseId %||% "Unknown", "\n")
  cat("  Result Format:", metaData$resultFormat %||% "Unknown", "\n\n")
  
  cat("Cost Analysis Settings:\n")
  cat("  Cost Concept ID:", metaData$costConceptId %||% "Unknown", "\n")
  cat("  Time Window:", metaData$startOffsetDays %||% 0, "to", metaData$endOffsetDays %||% 365, "days\n")
  cat("  CPI Adjustment:", metaData$cpiAdjustment %||% FALSE, "\n")
  cat("  Micro-costing:", metaData$microCosting %||% FALSE, "\n")
  cat("  Event Filters:", metaData$hasEventFilters %||% FALSE, "\n\n")
  
  cat("Data Summary:\n")
  cat("  Number of Covariates:", nCovariates, "\n")
  cat("  Number of People:", nPeople, "\n")
  cat("  Number of Covariate Values:", nValues, "\n\n")
  
  cat("Creation Info:\n")
  cat("  Package Version:", as.character(metaData$packageVersion %||% "Unknown"), "\n")
  cat("  Created:", format(metaData$creationTime %||% Sys.time()), "\n")
  
  invisible(object)
}

#' Print Method for CovariateData
#' @param x CovariateData object
#' @param ... Additional arguments (unused)
#' @return Invisibly returns the object
#' @export
print.CovariateData <- function(x, ...) {
  
  metaData <- attr(x, "metaData")
  
  cat("CovariateData object\n")
  cat("Analysis ID:", metaData$analysisId %||% "Unknown", "\n")
  cat("Database:", metaData$databaseId %||% "Unknown", "\n")
  cat("Format:", metaData$resultFormat %||% "Unknown", "\n")
  
  # Show table info
  cat("\nTables:\n")
  for (tableName in names(x)) {
    nRows <- x[[tableName]] %>% dplyr::tally() %>% dplyr::pull(n)
    cat("  ", tableName, ":", nRows, "rows\n")
  }
  
  invisible(x)
}