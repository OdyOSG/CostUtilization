#' Create FeatureExtraction CovariateData from Cost Analysis Results
#'
#' @description
#' Converts cost analysis results from `calculateCostOfCare()` into standardized
#' FeatureExtraction `CovariateData` format. Handles both aggregated and non-aggregated
#' results from Andromeda objects returned by the database query.
#'
#' @param costResults List containing `results` and `diagnostics` from `calculateCostOfCare()`.
#'   The `results` can be either:
#'   - **Aggregated**: Andromeda table with columns `metric_type`, `metric_name`, `metric_value`
#'   - **Non-aggregated**: Andromeda table with columns `person_id`, `cost`, `adjusted_cost` (optional)
#' @param costOfCareSettings Settings object from `createCostOfCareSettings()`.
#' @param cohortId Integer cohort ID for the analysis.
#' @param databaseId Character database identifier.
#' @param analysisId Integer analysis ID (default: 1000).
#' @param aggregated Logical indicating if results are aggregated (default: auto-detect).
#' @param temporalCovariateSettings Optional temporal covariate settings for time-based analysis.
#'
#' @return A `CovariateData` object compatible with FeatureExtraction package.
#' @export
#'
#' @examples
#' \dontrun{
#' # Basic usage with aggregated results
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
#' # Usage with non-aggregated person-level results
#' covariateData <- createCostCovariateData(
#'   costResults = results,
#'   costOfCareSettings = settings,
#'   cohortId = 1,
#'   databaseId = "MyDatabase",
#'   aggregated = FALSE
#' )
#' }
createCostCovariateData <- function(costResults,
                                    costOfCareSettings,
                                    cohortId,
                                    databaseId,
                                    analysisId = 1000L,
                                    aggregated = NULL,
                                    temporalCovariateSettings = NULL) {
  
  # Input validation
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertList(costResults, names = "named", add = errorMessages)
  checkmate::assertClass(costOfCareSettings, "CostOfCareSettings", add = errorMessages)
  checkmate::assertIntegerish(cohortId, len = 1, add = errorMessages)
  checkmate::assertCharacter(databaseId, len = 1, add = errorMessages)
  checkmate::assertIntegerish(analysisId, len = 1, add = errorMessages)
  checkmate::assertFlag(aggregated, null.ok = TRUE, add = errorMessages)
  checkmate::reportAssertions(errorMessages)
  
  if (!all(c("results", "diagnostics") %in% names(costResults))) {
    cli::cli_abort("costResults must contain 'results' and 'diagnostics' elements")
  }
  
  # Auto-detect aggregation level if not specified
  if (is.null(aggregated)) {
    aggregated <- .detectAggregationLevel(costResults$results)
  }
  
  # Create CovariateData based on aggregation level
  if (aggregated) {
    .createAggregatedCovariateData(
      costResults = costResults,
      costOfCareSettings = costOfCareSettings,
      cohortId = cohortId,
      databaseId = databaseId,
      analysisId = analysisId,
      temporalCovariateSettings = temporalCovariateSettings
    )
  } else {
    .createPersonLevelCovariateData(
      costResults = costResults,
      costOfCareSettings = costOfCareSettings,
      cohortId = cohortId,
      databaseId = databaseId,
      analysisId = analysisId,
      temporalCovariateSettings = temporalCovariateSettings
    )
  }
}

#' Convert Cost Results to FeatureExtraction Format (Convenience Function)
#'
#' @description
#' Convenience wrapper around `createCostCovariateData()` for quick conversion.
#'
#' @inheritParams createCostCovariateData
#' @return A `CovariateData` object.
#' @export
convertToFeatureExtractionFormat <- function(costResults,
                                             costOfCareSettings,
                                             cohortId,
                                             databaseId,
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

#' Detect Aggregation Level from Results
#' @param results Results table (tibble or Andromeda table)
#' @return Logical indicating if results are aggregated
#' @noRd
.detectAggregationLevel <- function(results) {
  # Check column names to determine aggregation level
  if (inherits(results, "tbl_Andromeda")) {
    colNames <- names(results)
  } else {
    colNames <- names(results)
  }
  
  # Aggregated results have metric_type, metric_name, metric_value
  aggregatedCols <- c("metric_type", "metric_name", "metric_value")
  # Non-aggregated results have person_id, cost
  personLevelCols <- c("person_id", "cost")
  
  hasAggregatedCols <- all(aggregatedCols %in% colNames)
  hasPersonLevelCols <- all(personLevelCols %in% colNames)
  
  if (hasAggregatedCols && !hasPersonLevelCols) {
    return(TRUE)
  } else if (hasPersonLevelCols && !hasAggregatedCols) {
    return(FALSE)
  } else {
    cli::cli_warn("Cannot auto-detect aggregation level. Assuming aggregated format.")
    return(TRUE)
  }
}

#' Create CovariateData from Aggregated Results
#' @inheritParams createCostCovariateData
#' @return CovariateData object
#' @noRd
.createAggregatedCovariateData <- function(costResults,
                                           costOfCareSettings,
                                           cohortId,
                                           databaseId,
                                           analysisId,
                                           temporalCovariateSettings) {
  
  results <- costResults$results
  
  # Convert Andromeda to tibble if needed
  if (inherits(results, "tbl_Andromeda")) {
    results <- results |> dplyr::collect()
  }
  
  # Create Andromeda object for CovariateData
  andromedaObject <- Andromeda::andromeda()
  
  # Transform aggregated metrics to covariates format
  covariates <- results |>
    dplyr::mutate(
      rowId = cohortId,
      covariateId = .generateCovariateId(analysisId, .data$metric_type, .data$metric_name),
      covariateValue = as.numeric(.data$metric_value)
    ) |>
    dplyr::select(.data$rowId, .data$covariateId, .data$covariateValue) |>
    dplyr::filter(!is.na(.data$covariateValue))
  
  andromedaObject$covariates <- covariates
  
  # Create covariate reference
  andromedaObject$covariateRef <- results |>
    dplyr::mutate(
      covariateId = .generateCovariateId(analysisId, .data$metric_type, .data$metric_name),
      covariateName = stringr::str_c(.data$metric_type, ": ", .data$metric_name),
      analysisId = analysisId,
      conceptId = 0L
    ) |>
    dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId) |>
    dplyr::distinct()
  
  # Create analysis reference
  andromedaObject$analysisRef <- .createAnalysisRef(
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings,
    aggregated = TRUE
  )
  
  # Create metadata
  metaData <- .createMetaData(
    costOfCareSettings = costOfCareSettings,
    cohortId = cohortId,
    databaseId = databaseId,
    aggregated = TRUE,
    temporalCovariateSettings = temporalCovariateSettings
  )
  
  # Create CovariateData object
  result <- list(
    covariates = andromedaObject$covariates,
    covariateRef = andromedaObject$covariateRef,
    analysisRef = andromedaObject$analysisRef,
    metaData = metaData
  )
  
  class(result) <- "CovariateData"
  attr(result, "metaData") <- metaData
  
  return(result)
}

#' Create CovariateData from Person-Level Results
#' @inheritParams createCostCovariateData
#' @return CovariateData object
#' @noRd
.createPersonLevelCovariateData <- function(costResults,
                                            costOfCareSettings,
                                            cohortId,
                                            databaseId,
                                            analysisId,
                                            temporalCovariateSettings) {
  
  results <- costResults$results
  
  # Convert Andromeda to tibble if needed
  if (inherits(results, "tbl_Andromeda")) {
    results <- results |> dplyr::collect()
  }
  
  # Create Andromeda object for CovariateData
  andromedaObject <- Andromeda::andromeda()
  
  # Check if CPI adjustment is present
  hasCpiAdjustment <- "adjusted_cost" %in% names(results) && costOfCareSettings$cpiAdjustment
  
  # Create covariates from person-level data
  covariates_list <- list()
  
  # Base cost covariate
  covariates_list[[1]] <- results |>
    dplyr::mutate(
      rowId = .data$person_id,
      covariateId = analysisId * 1000L + 1L,  # Cost covariate
      covariateValue = as.numeric(.data$cost)
    ) |>
    dplyr::select(.data$rowId, .data$covariateId, .data$covariateValue) |>
    dplyr::filter(!is.na(.data$covariateValue), .data$covariateValue > 0)
  
  # CPI-adjusted cost covariate (if available)
  if (hasCpiAdjustment) {
    covariates_list[[2]] <- results |>
      dplyr::mutate(
        rowId = .data$person_id,
        covariateId = analysisId * 1000L + 2L,  # Adjusted cost covariate
        covariateValue = as.numeric(.data$adjusted_cost)
      ) |>
      dplyr::select(.data$rowId, .data$covariateId, .data$covariateValue) |>
      dplyr::filter(!is.na(.data$covariateValue), .data$covariateValue > 0)
  }
  
  # Binary indicator covariate (has any cost)
  covariates_list[[length(covariates_list) + 1]] <- results |>
    dplyr::filter(!is.na(.data$cost), .data$cost > 0) |>
    dplyr::mutate(
      rowId = .data$person_id,
      covariateId = analysisId * 1000L + 10L,  # Binary indicator
      covariateValue = 1.0
    ) |>
    dplyr::select(.data$rowId, .data$covariateId, .data$covariateValue) |>
    dplyr::distinct()
  
  # Combine all covariates
  andromedaObject$covariates <- purrr::reduce(covariates_list, dplyr::bind_rows)
  
  # Create covariate reference
  covariateRef_list <- list(
    dplyr::tibble(
      covariateId = analysisId * 1000L + 1L,
      covariateName = .createCovariateName(costOfCareSettings, "cost"),
      analysisId = analysisId,
      conceptId = as.integer(costOfCareSettings$costConceptId %||% 0L)
    ),
    dplyr::tibble(
      covariateId = analysisId * 1000L + 10L,
      covariateName = .createCovariateName(costOfCareSettings, "has_cost"),
      analysisId = analysisId,
      conceptId = as.integer(costOfCareSettings$costConceptId %||% 0L)
    )
  )
  
  if (hasCpiAdjustment) {
    covariateRef_list[[length(covariateRef_list) + 1]] <- dplyr::tibble(
      covariateId = analysisId * 1000L + 2L,
      covariateName = .createCovariateName(costOfCareSettings, "adjusted_cost"),
      analysisId = analysisId,
      conceptId = as.integer(costOfCareSettings$costConceptId %||% 0L)
    )
  }
  
  andromedaObject$covariateRef <- purrr::reduce(covariateRef_list, dplyr::bind_rows)
  
  # Create analysis reference
  andromedaObject$analysisRef <- .createAnalysisRef(
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings,
    aggregated = FALSE
  )
  
  # Create metadata
  metaData <- .createMetaData(
    costOfCareSettings = costOfCareSettings,
    cohortId = cohortId,
    databaseId = databaseId,
    aggregated = FALSE,
    temporalCovariateSettings = temporalCovariateSettings
  )
  
  # Create CovariateData object
  result <- list(
    covariates = andromedaObject$covariates,
    covariateRef = andromedaObject$covariateRef,
    analysisRef = andromedaObject$analysisRef,
    metaData = metaData
  )
  
  class(result) <- "CovariateData"
  attr(result, "metaData") <- metaData
  
  return(result)
}

#' Generate Covariate ID from Analysis Components
#' @param analysisId Base analysis ID
#' @param metricType Type of metric (e.g., "visit_level", "line_level")
#' @param metricName Name of metric (e.g., "total_cost", "cost_pppm")
#' @return Integer covariate ID
#' @noRd
.generateCovariateId <- function(analysisId, metricType, metricName) {
  # Create a systematic ID scheme
  baseId <- analysisId * 1000L
  
  # Type offset
  typeOffset <- dplyr::case_when(
    metricType == "visit_level" ~ 100L,
    metricType == "line_level" ~ 200L,
    TRUE ~ 0L
  )
  
  # Metric offset
  metricOffset <- dplyr::case_when(
    stringr::str_detect(metricName, "total_cost") ~ 1L,
    stringr::str_detect(metricName, "total_adjusted_cost") ~ 2L,
    stringr::str_detect(metricName, "cost_pppm") ~ 3L,
    stringr::str_detect(metricName, "adjusted_cost_pppm") ~ 4L,
    stringr::str_detect(metricName, "cost_pppy") ~ 5L,
    stringr::str_detect(metricName, "adjusted_cost_pppy") ~ 6L,
    stringr::str_detect(metricName, "n_persons_with_cost") ~ 10L,
    stringr::str_detect(metricName, "distinct_visits") ~ 11L,
    stringr::str_detect(metricName, "distinct_events") ~ 12L,
    stringr::str_detect(metricName, "events_per_1000_py") ~ 13L,
    TRUE ~ 99L
  )
  
  return(baseId + typeOffset + metricOffset)
}

#' Create Analysis Reference Table
#' @param analysisId Analysis ID
#' @param costOfCareSettings Settings object
#' @param aggregated Whether results are aggregated
#' @return Tibble with analysis reference
#' @noRd
.createAnalysisRef <- function(analysisId, costOfCareSettings, aggregated) {
  
  # Determine analysis type
  analysisType <- if (aggregated) "Cost Analysis (Aggregated)" else "Cost Analysis (Person-Level)"
  
  # Create domain description
  domainDescription <- if (!is.null(costOfCareSettings$eventFilters) && length(costOfCareSettings$eventFilters) > 0) {
    domains <- purrr::map_chr(costOfCareSettings$eventFilters, ~ .x$domain)
    paste("Filtered by:", paste(unique(domains), collapse = ", "))
  } else {
    "All domains"
  }
  
  # Time window description
  timeWindow <- sprintf("Days %d to %d relative to %s",
                        costOfCareSettings$startOffsetDays %||% 0L,
                        costOfCareSettings$endOffsetDays %||% 365L,
                        costOfCareSettings$anchorCol %||% "cohort_start_date")
  
  dplyr::tibble(
    analysisId = analysisId,
    analysisName = analysisType,
    domainId = "Cost",
    startDay = as.integer(costOfCareSettings$startOffsetDays %||% 0L),
    endDay = as.integer(costOfCareSettings$endOffsetDays %||% 365L),
    isBinary = if (aggregated) "N" else "Y",
    missingMeansZero = "Y",
    description = paste(analysisType, "-", domainDescription, "-", timeWindow)
  )
}

#' Create Covariate Name
#' @param costOfCareSettings Settings object
#' @param metricType Type of metric
#' @return Character covariate name
#' @noRd
.createCovariateName <- function(costOfCareSettings, metricType) {
  
  # Base cost concept description
  costConceptDesc <- dplyr::case_when(
    costOfCareSettings$costConceptId == 31973L ~ "Total Charge",
    costOfCareSettings$costConceptId == 31985L ~ "Total Cost",
    costOfCareSettings$costConceptId == 31980L ~ "Paid by Payer",
    costOfCareSettings$costConceptId == 31981L ~ "Paid by Patient",
    costOfCareSettings$costConceptId == 31974L ~ "Patient Copay",
    costOfCareSettings$costConceptId == 31975L ~ "Patient Coinsurance",
    costOfCareSettings$costConceptId == 31976L ~ "Patient Deductible",
    costOfCareSettings$costConceptId == 31979L ~ "Amount Allowed",
    TRUE ~ paste("Cost Concept", costOfCareSettings$costConceptId)
  )
  
  # Metric type description
  metricDesc <- dplyr::case_when(
    metricType == "cost" ~ "Amount",
    metricType == "adjusted_cost" ~ "CPI-Adjusted Amount",
    metricType == "has_cost" ~ "Has Any Cost",
    TRUE ~ stringr::str_to_title(stringr::str_replace_all(metricType, "_", " "))
  )
  
  # Time window
  timeWindow <- sprintf("days %d to %d",
                        costOfCareSettings$startOffsetDays %||% 0L,
                        costOfCareSettings$endOffsetDays %||% 365L)
  
  # Event filter description
  filterDesc <- if (!is.null(costOfCareSettings$eventFilters) && length(costOfCareSettings$eventFilters) > 0) {
    paste("filtered by", length(costOfCareSettings$eventFilters), "event types")
  } else {
    "all events"
  }
  
  paste(costConceptDesc, metricDesc, timeWindow, filterDesc, sep = " - ")
}

#' Create Metadata for CovariateData
#' @param costOfCareSettings Settings object
#' @param cohortId Cohort ID
#' @param databaseId Database ID
#' @param aggregated Whether results are aggregated
#' @param temporalCovariateSettings Temporal settings
#' @return List with metadata
#' @noRd
.createMetaData <- function(costOfCareSettings,
                            cohortId,
                            databaseId,
                            aggregated,
                            temporalCovariateSettings) {
  
  list(
    sql = "Cost analysis using CostUtilization package",
    call = match.call(),
    packageVersion = "CostUtilization",
    analysisType = if (aggregated) "aggregated" else "person_level",
    cohortId = cohortId,
    databaseId = databaseId,
    costConceptId = costOfCareSettings$costConceptId,
    currencyConceptId = costOfCareSettings$currencyConceptId,
    timeWindow = list(
      startOffsetDays = costOfCareSettings$startOffsetDays,
      endOffsetDays = costOfCareSettings$endOffsetDays,
      anchorCol = costOfCareSettings$anchorCol
    ),
    eventFilters = costOfCareSettings$eventFilters,
    visitRestrictions = if (costOfCareSettings$hasVisitRestriction) {
      costOfCareSettings$restrictVisitConceptIds
    } else {
      NULL
    },
    cpiAdjustment = costOfCareSettings$cpiAdjustment,
    microCosting = costOfCareSettings$microCosting,
    temporalCovariateSettings = temporalCovariateSettings,
    createdOn = Sys.time()
  )
}

# ============================================================================
# Utility Functions
# ============================================================================

#' Check if Object is CovariateData
#' @param x Object to check
#' @return Logical
#' @export
is.CovariateData <- function(x) {
  inherits(x, "CovariateData")
}

#' Get Covariate Values for Specific People
#' @param covariateData CovariateData object
#' @param rowIds Vector of row IDs (person IDs) to extract
#' @return Tibble with covariate values
#' @export
getCovariateValues <- function(covariateData, rowIds) {
  checkmate::assertClass(covariateData, "CovariateData")
  checkmate::assertIntegerish(rowIds, min.len = 1)
  
  covariateData$covariates |>
    dplyr::filter(.data$rowId %in% !!rowIds) |>
    dplyr::left_join(
      covariateData$covariateRef |> dplyr::select(.data$covariateId, .data$covariateName),
      by = "covariateId"
    ) |>
    dplyr::collect()
}

#' Convert CovariateData to Wide Format
#' @param covariateData CovariateData object
#' @return Tibble in wide format
#' @export
toWideFormat <- function(covariateData) {
  checkmate::assertClass(covariateData, "CovariateData")
  
  # Get covariate names
  covariateNames <- covariateData$covariateRef |>
    dplyr::select(.data$covariateId, .data$covariateName) |>
    dplyr::collect()
  
  # Convert to wide format
  covariateData$covariates |>
    dplyr::left_join(covariateNames, by = "covariateId") |>
    dplyr::select(.data$rowId, .data$covariateName, .data$covariateValue) |>
    tidyr::pivot_wider(
      names_from = .data$covariateName,
      values_from = .data$covariateValue,
      values_fill = 0
    ) |>
    dplyr::collect()
}