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
#'   - **Person-level format**: `person_id`, `cost`, `adjusted_cost` (optional) columns
#' @param costOfCareSettings A `CostOfCareSettings` object used in the analysis.
#' @param cohortId Integer cohort ID for the analysis.
#' @param databaseId Character database identifier for metadata.
#' @param analysisId Integer analysis ID for systematic covariate numbering. Default: 1000L.
#'
#' @return A `CovariateData` object (Andromeda-backed) with components:
#'   - `covariates`: Person-level cost/utilization metrics (`rowId`, `covariateId`, `covariateValue`)
#'   - `covariateRef`: Reference table describing each covariate
#'   - `analysisRef`: Analysis-level metadata and parameters
#'   - `timeRef`: Time window reference
#'   - `metaData`: Comprehensive analysis metadata (in the object attribute)
#'
#' @export
createCostCovariateData <- function(costResults,
                                    costOfCareSettings,
                                    cohortId,
                                    databaseId,
                                    analysisId = 1000L) {
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
  if (identical(resultFormat, "aggregated")) {
    covariateData <- .convertAggregatedResults(
      costResults = costResults,
      costOfCareSettings = costOfCareSettings,
      cohortId = cohortId,
      databaseId = databaseId,
      analysisId = analysisId
    )
  } else if (identical(resultFormat, "person_level")) {
    covariateData <- .convertPersonLevelResults(
      costResults = costResults,
      costOfCareSettings = costOfCareSettings,
      cohortId = cohortId,
      databaseId = databaseId,
      analysisId = analysisId
    )
  } else {
    rlang::abort("Unable to detect valid result format. Expected either aggregated or person-level format.")
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
#' @return A `CovariateData` object compatible with FeatureExtraction package.
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

# Internal helpers -------------------------------------------------------------

#' Detect Result Format from Andromeda Object
#' @param costResults Andromeda object from calculateCostOfCare()
#' @return Character: "aggregated", "person_level", or "unknown"
#' @keywords internal
.detectResultFormat <- function(costResults) {
  if (!"results" %in% names(costResults)) {
    return("unknown")
  }
  resultsCols <- names(costResults$results)
  
  hasAggregated  <- all(c("metric_type", "metric_name", "metric_value") %in% resultsCols)
  hasPersonLevel <- all(c("person_id", "cost") %in% resultsCols)
  
  if (hasAggregated)  return("aggregated")
  if (hasPersonLevel) return("person_level")
  rlang::abort("Unknown results type: cannot find aggregated or person-level columns in `results`.")
}

#' Convert Aggregated Results to FeatureExtraction Format
#' @inheritParams createCostCovariateData
#' @return CovariateData object
#' @keywords internal
.convertAggregatedResults <- function(costResults,
                                      costOfCareSettings,
                                      cohortId,
                                      databaseId,
                                      analysisId) {
  
  resultsData <- costResults$results |>
    dplyr::collect() |>
    dplyr::as_tibble()
  
  covariateMapping <- .createCovariateMapping(
    resultsData = resultsData,
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings
  )
  
  covariates <- .generateCovariatesFromAggregated(
    resultsData = resultsData,
    covariateMapping = covariateMapping,
    cohortId = cohortId
  )
  
  covariateRef <- .createCovariateRef(
    covariateMapping = covariateMapping,
    costOfCareSettings = costOfCareSettings
  )
  
  analysisRef <- .createAnalysisRef(
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings,
    databaseId = databaseId
  )
  
  timeRef <- .createTimeRef(costOfCareSettings)
  
  metaData <- .createMetaData(
    costOfCareSettings = costOfCareSettings,
    cohortId = cohortId,
    databaseId = databaseId,
    analysisId = analysisId,
    resultFormat = "aggregated"
  )
  
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
                                       analysisId) {
  
  resultsData <- costResults$results |>
    dplyr::collect() |>
    dplyr::as_tibble()
  
  covariateMapping <- .createPersonLevelCovariateMapping(
    resultsData = resultsData,
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings
  )
  
  covariates <- .generateCovariatesFromPersonLevel(
    resultsData = resultsData,
    covariateMapping = covariateMapping,
    cohortId = cohortId
  )
  
  covariateRef <- .createCovariateRef(
    covariateMapping = covariateMapping,
    costOfCareSettings = costOfCareSettings
  )
  
  analysisRef <- .createAnalysisRef(
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings,
    databaseId = databaseId
  )
  
  timeRef <- .createTimeRef(costOfCareSettings)
  
  metaData <- .createMetaData(
    costOfCareSettings = costOfCareSettings,
    cohortId = cohortId,
    databaseId = databaseId,
    analysisId = analysisId,
    resultFormat = "person_level"
  )
  
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
  
  tmp <- resultsData |>
    dplyr::distinct(metric_type, metric_name) |>
    dplyr::arrange(metric_type, metric_name) |>
    dplyr::group_by(metric_type) |>
    dplyr::mutate(metric_offset = dplyr::row_number()) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      type_offset = dplyr::case_match(
        metric_type,
        "visit_level" ~ 100L,
        "line_level"  ~ 200L,
        .default      ~ 100L
      ),
      covariate_id   = analysisId * 1000L + type_offset + metric_offset,
      covariate_name = .formatCovariateName(metric_name, costOfCareSettings),
      analysis_id    = analysisId
    )
  
  return(tmp)
}

#' Create Covariate Mapping for Person-Level Results
#' @param resultsData Tibble of person-level results
#' @param analysisId Analysis ID for covariate numbering
#' @param costOfCareSettings Settings object
#' @return Tibble with covariate mappings
#' @keywords internal
.createPersonLevelCovariateMapping <- function(resultsData, analysisId, costOfCareSettings) {
  
  costCols <- intersect(names(resultsData), c("cost", "adjusted_cost"))
  
  purrr::map_dfr(seq_along(costCols), ~{
    costCol <- costCols[.x]
    dplyr::tibble(
      metric_name    = costCol,
      metric_offset  = .x,
      covariate_id   = analysisId * 1000L + 300L + .x,  # 300 reserved for person-level
      covariate_name = .formatCovariateName(costCol, costOfCareSettings),
      analysis_id    = analysisId
    )
  })
}

#' Generate Covariates from Aggregated Results
#' @param resultsData Aggregated results tibble
#' @param covariateMapping Covariate mapping tibble
#' @param cohortId Cohort ID
#' @return Tibble of covariates
#' @keywords internal
.generateCovariatesFromAggregated <- function(resultsData, covariateMapping, cohortId) {
  
  covariates <- resultsData |>
    dplyr::inner_join(covariateMapping, by = c("metric_type", "metric_name")) |>
    dplyr::transmute(
      rowId          = as.integer(cohortId),          # population-level row; one row per covariate
      covariateId    = as.numeric(covariate_id),
      covariateValue = as.numeric(metric_value)
    ) |>
    dplyr::filter(!is.na(covariateValue), covariateValue != 0)
  
  return(covariates)
}

#' Generate Covariates from Person-Level Results
#' @param resultsData Person-level results tibble
#' @param covariateMapping Covariate mapping tibble
#' @param cohortId Cohort ID (kept for symmetry; not used directly here)
#' @return Tibble of covariates
#' @keywords internal
.generateCovariatesFromPersonLevel <- function(resultsData, covariateMapping, cohortId) {
  
  costCols <- intersect(names(resultsData), c("cost", "adjusted_cost"))
  
  covariates <- resultsData |>
    dplyr::select(.data$person_id, dplyr::all_of(costCols)) |>
    dplyr::rowwise() |>
    dplyr::mutate(
      data = list(
        tibble::tibble(
          metric_name    = costCols,
          covariateValue = c_across(dplyr::all_of(costCols))
        )
      )
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(data = purrr::map2(.data$person_id, .data$data,
                                     ~ dplyr::mutate(.y, person_id = .x))) |>
    dplyr::select(.data$data) |>
    dplyr::bind_rows() |>
    dplyr::inner_join(covariateMapping, by = "metric_name") |>
    dplyr::transmute(
      rowId          = as.integer(.data$person_id),
      covariateId    = as.numeric(.data$covariate_id),
      covariateValue = as.numeric(.data$covariateValue)
    ) |>
    dplyr::filter(!is.na(.data$covariateValue) & .data$covariateValue != 0)
  
  return(covariates)
}

#' Create Covariate Reference Table
#' @param covariateMapping Covariate mapping tibble
#' @param costOfCareSettings Settings object
#' @return Tibble of covariate references
#' @keywords internal
.createCovariateRef <- function(covariateMapping, costOfCareSettings) {
  conceptId <- costOfCareSettings$costConceptId
  conceptId <- if (is.null(conceptId)) 0L else as.integer(conceptId)
  
  covariateMapping |>
    dplyr::transmute(
      covariateId   = covariate_id,
      covariateName = covariate_name,
      analysisId    = analysis_id,
      conceptId     = conceptId
    )
}

#' Create Analysis Reference Table
#' @param analysisId Analysis ID
#' @param costOfCareSettings Settings object
#' @param databaseId Database identifier
#' @return Tibble of analysis references
#' @keywords internal
.createAnalysisRef <- function(analysisId, costOfCareSettings, databaseId) {
  
  analysisName <- .createAnalysisName(costOfCareSettings)
  
  dplyr::tibble(
    analysisId        = analysisId,
    analysisName      = analysisName,
    domainId          = "Cost",
    startDay          = as.integer(costOfCareSettings$startOffsetDays %||% 0L),
    endDay            = as.integer(costOfCareSettings$endOffsetDays %||% 365L),
    isBinary          = "N",
    missingMeansZero  = "Y"
  )
}

#' Create Time Reference Table
#' @keywords internal
.createTimeRef <- function(costOfCareSettings) {
  dplyr::tibble(
    timeId   = 1L,
    startDay = as.integer(costOfCareSettings$startOffsetDays),
    endDay   = as.integer(costOfCareSettings$endOffsetDays)
  )
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
  
  pkgVer <- tryCatch(
    as.character(utils::packageVersion("CostUtilization")),
    error = function(e) NA_character_
  )
  
  list(
    analysisId          = analysisId,
    cohortId            = cohortId,
    databaseId          = databaseId,
    resultFormat        = resultFormat,
    costConceptId       = costOfCareSettings$costConceptId,
    currencyConceptId   = costOfCareSettings$currencyConceptId,
    cpiAdjustment       = costOfCareSettings$cpiAdjustment,
    microCosting        = costOfCareSettings$microCosting,
    anchorCol           = costOfCareSettings$anchorCol,
    startOffsetDays     = costOfCareSettings$startOffsetDays,
    endOffsetDays       = costOfCareSettings$endOffsetDays,
    hasVisitRestriction = costOfCareSettings$hasVisitRestriction,
    hasEventFilters     = costOfCareSettings$hasEventFilters,
    nFilters            = costOfCareSettings$nFilters %||% 0L,
    packageVersion      = pkgVer,
    creationTime        = Sys.time()
  )
}

#' Assemble CovariateData Object
#' @param covariates Covariates tibble
#' @param covariateRef Covariate reference tibble
#' @param analysisRef Analysis reference tibble
#' @param timeRef Time reference tibble (optional)
#' @param metaData Metadata list
#' @return CovariateData object
#' @keywords internal
.assembleCovariateData <- function(covariates, covariateRef, analysisRef, timeRef = NULL, metaData) {
  
  covariateData <- Andromeda::andromeda()
  
  covariateData$covariates   <- covariates
  covariateData$covariateRef <- covariateRef
  covariateData$analysisRef  <- analysisRef
  
  if (!is.null(timeRef)) {
    covariateData$timeRef <- timeRef
  }
  
  attr(covariateData, "metaData") <- metaData
  class(covariateData) <- c("CovariateData", class(covariateData))
  
  return(covariateData)
}

#' Format Covariate Name
#' @param metricName Raw metric name
#' @param costOfCareSettings Settings object for context
#' @return Formatted covariate name
#' @keywords internal
.formatCovariateName <- function(metricName, costOfCareSettings) {
  
  costConceptId <- costOfCareSettings$costConceptId %||% 31973L
  costType <- dplyr::case_match(
    as.integer(costConceptId),
    31973L ~ "Total Charge",
    31985L ~ "Total Cost",
    31980L ~ "Paid by Payer",
    31981L ~ "Paid by Patient",
    31974L ~ "Patient Copay",
    31975L ~ "Patient Coinsurance",
    31976L ~ "Patient Deductible",
    31979L ~ "Amount Allowed",
    .default = paste("Cost Concept", costConceptId)
  )
  

  covariateName <- to_title_case_base(metricTitle)
  
  startDays <- costOfCareSettings$startOffsetDays
  endDays   <- costOfCareSettings$endOffsetDays
  timeWindow <- paste0("(", startDays, " to ", endDays, " days)")
  
  paste(covariateName, timeWindow)
}

#' Create Analysis Name
#' @param costOfCareSettings Settings object
#' @return Descriptive analysis name
#' @keywords internal
.createAnalysisName <- function(costOfCareSettings) {
  baseName <- "Cost Analysis"
  costConceptId <- costOfCareSettings$costConceptId %||% 31973L
  costType <- dplyr::case_match(
    as.integer(costConceptId),
    31973L ~ "Total Charge",
    31985L ~ "Total Cost",
    31980L ~ "Paid by Payer",
    31981L ~ "Paid by Patient",
    .default = paste("Concept", costConceptId)
  )
  
  modifiers <- c()
  if (isTRUE(costOfCareSettings$cpiAdjustment))       modifiers <- c(modifiers, "CPI-Adjusted")
  if (isTRUE(costOfCareSettings$microCosting))        modifiers <- c(modifiers, "Line-Level")
  if (isTRUE(costOfCareSettings$hasEventFilters))     modifiers <- c(modifiers, "Event-Filtered")
  if (isTRUE(costOfCareSettings$hasVisitRestriction)) modifiers <- c(modifiers, "Visit-Restricted")
  
  components <- c(baseName, costType)
  if (length(modifiers) > 0) {
    components <- c(components, paste0("(", paste(modifiers, collapse = ", "), ")"))
  }
  
  paste(components, collapse = " - ")
}

