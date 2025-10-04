#' Create Cost Covariate Data in FeatureExtraction Format
#'
#' @description
#' Converts results from `calculateCostOfCare()` into a standardized
#' [`CovariateData`](https://ohdsi.github.io/FeatureExtraction/reference/CovariateData-class.html)
#' object compatible with the **FeatureExtraction** package and the OHDSI ecosystem.
#' Both **aggregated** and **person-level** result formats are automatically supported:
#'
#' - **Aggregated format**: one row per metric across the full cohort, with columns
#'   `metric_type`, `metric_name`, `metric_value`.
#' - **Person-level format**: one row per person, with at least `person_id` and `cost`,
#'   and optionally `adjusted_cost`.
#'
#' @param costResults An `Andromeda` object returned from [calculateCostOfCare()].
#'   Must contain a `results` table in either aggregated or person-level format.
#' @param costOfCareSettings A `CostOfCareSettings` object specifying the analysis
#'   parameters (time window, concept IDs, costing approach, etc).
#' @param cohortId Integer. Cohort ID used for covariate construction.
#' @param databaseId Character. Identifier of the source database for metadata tracking.
#' @param analysisId Integer. Analysis ID used to generate systematic covariate IDs.
#'   Default is `1000L`. Covariate IDs are constructed as `analysisId * 1000 + offset`.
#'
#' @return
#' A `CovariateData` object (backed by an `Andromeda` database) with the following tables:
#' - **covariates**: person-level or cohort-level cost metrics
#'   (`rowId`, `covariateId`, `covariateValue`)
#' - **covariateRef**: mapping of covariate IDs to names and concepts
#' - **analysisRef**: analysis-level metadata (ID, name, domain, time window)
#' - **timeRef**: time-window reference (always length 1 for cost analyses)
#'
#' Metadata about the analysis is stored in `attr(covariateData, "metaData")`.
#'
#' @examples
#' \dontrun{
#' results <- calculateCostOfCare(cohort, costOfCareSettings)
#' covData <- createCostCovariateData(
#'   costResults = results,
#'   costOfCareSettings = costOfCareSettings,
#'   cohortId = 1L,
#'   databaseId = "CDM_DB"
#' )
#'
#' # Use FeatureExtraction utilities:
#' FeatureExtraction::summarizeCovariates(covData)
#' }
#'
#' @export
createCostCovariateData <- function(costResults,
                                    costOfCareSettings,
                                    cohortId,
                                    databaseId,
                                    analysisId = 1000L) {
  rlang::is_installed("pillar")
  rlang::is_installed("crayon")
  # Input validation
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(costResults, "Andromeda", add = errorMessages)
  checkmate::assertClass(costOfCareSettings, "CostOfCareSettings", add = errorMessages)
  checkmate::assertIntegerish(cohortId, len = 1, add = errorMessages)
  checkmate::assertCharacter(databaseId, len = 1, add = errorMessages)
  checkmate::assertIntegerish(analysisId, len = 1, add = errorMessages)
  checkmate::reportAssertions(errorMessages)

  resultFormat <- .detectResultFormat(costResults)

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
    rlang::abort("Unable to detect valid result format in `costResults`.")
  }

  return(covariateData)
}

#' Convenience Function for FeatureExtraction Format Conversion
#'
#' @description
#' A simplified wrapper around `createCostCovariateData()` for quick conversion.
#'
#' @inheritParams createCostCovariateData
#' @return A `CovariateData` S4 object.
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

.detectResultFormat <- function(costResults) {
  if (!"results" %in% names(costResults)) {
    return("unknown")
  }

  resultsCols <- names(costResults$results)
  hasAggregated <- all(c("metric_type", "metric_name", "metric_value") %in% resultsCols)
  hasPersonLevel <- all(c("person_id", "cost") %in% resultsCols)

  if (hasAggregated) {
    return("aggregated")
  }
  if (hasPersonLevel) {
    return("person_level")
  }

  "unknown"
}

.convertAggregatedResults <- function(costResults, costOfCareSettings, cohortId, databaseId, analysisId) {
  resultsData <- dplyr::collect(costResults$results)

  covariateMapping <- .createCovariateMapping(resultsData, analysisId, costOfCareSettings)
  covariates <- .generateCovariatesFromAggregated(resultsData, covariateMapping, cohortId)
  covariateRef <- .createCovariateRef(covariateMapping, costOfCareSettings)
  analysisRef <- .createAnalysisRef(analysisId, costOfCareSettings)
  timeRef <- .createTimeRef(costOfCareSettings)
  metaData <- .createMetaData(costOfCareSettings, cohortId, databaseId, analysisId, "aggregated")

  .assembleCovariateData(covariates, covariateRef, analysisRef, timeRef, metaData)
}

#' Convert Person-Level Results to CovariateData
#'
#' @description
#' Internal helper function that transforms a person-level results tibble into a
#' standardized `CovariateData` object. It uses tidy evaluation to flexibly
#' handle user-specified cost columns.
#'
#' @param costResults An `Andromeda` object containing the `results` table.
#' @param costCols A list of quosures representing the unquoted column names
#'   to be converted into covariates.
#' @param costOfCareSettings The `CostOfCareSettings` object for the analysis.
#' @param cohortId The cohort ID for the analysis.
#' @param databaseId The database identifier.
#' @param analysisId The base analysis ID for covariates.
#'
#' @return A `CovariateData` object.
#' @noRd
.convertPersonLevelResults <- function(costResults,
                                       costCols,
                                       costOfCareSettings,
                                       cohortId,
                                       databaseId,
                                       analysisId) {
  # Step 1: Extract data and validate the specified columns
  resultsData <- dplyr::collect(costResults$results)
  costColNames <- purrr::map_chr(costCols, rlang::as_name)
  validCostColNames <- intersect(costColNames, names(resultsData))

  # Step 2: Handle the edge case where no valid columns are found
  if (rlang::is_empty(validCostColNames)) {
    cli::cli_warn(c(
      "No valid cost columns found to create covariates.",
      "i" = "Specified columns: {.val {costColNames}}",
      "i" = "Available columns: {.val {names(resultsData)}}",
      "i" = "Returning an empty CovariateData object."
    ))

    # Construct an empty but valid CovariateData object
    emptyCovariates <- dplyr::tibble(rowId = integer(), covariateId = numeric(), covariateValue = numeric())
    emptyCovariateRef <- dplyr::tibble(covariateId = numeric(), covariateName = character(), analysisId = integer(), conceptId = integer())

    return(
      .assembleCovariateData(
        covariates = emptyCovariates,
        covariateRef = emptyCovariateRef,
        analysisRef = .createAnalysisRef(analysisId, costOfCareSettings),
        timeRef = .createTimeRef(costOfCareSettings),
        metaData = .createMetaData(costOfCareSettings, cohortId, databaseId, analysisId, "person_level")
      )
    )
  }

  # Step 3: Generate mapping and reference tables based on valid columns
  covariateMapping <- .createPersonLevelCovariateMapping(
    costColNames = validCostColNames,
    analysisId = analysisId,
    costOfCareSettings = costOfCareSettings
  )

  covariateRef <- .createCovariateRef(covariateMapping, costOfCareSettings)
  analysisRef <- .createAnalysisRef(analysisId, costOfCareSettings)
  timeRef <- .createTimeRef(costOfCareSettings)
  metaData <- .createMetaData(costOfCareSettings, cohortId, databaseId, analysisId, "person_level")

  # Step 4: Generate the main 'covariates' table using the validated columns
  covariates <- .generateCovariatesFromPersonLevel(
    resultsData = resultsData,
    covariateMapping = covariateMapping,
    costColNames = validCostColNames # Pass the validated names
  )

  # Step 5: Assemble all components into the final CovariateData object
  .assembleCovariateData(
    covariates = covariates,
    covariateRef = covariateRef,
    analysisRef = analysisRef,
    timeRef = timeRef,
    metaData = metaData
  )
}



.createCovariateMapping <- function(resultsData, analysisId, costOfCareSettings) {
  resultsData |>
    dplyr::distinct(.data$metric_type, .data$metric_name) |>
    dplyr::arrange(.data$metric_type, .data$metric_name) |>
    dplyr::group_by(.data$metric_type) |>
    dplyr::mutate(metric_offset = dplyr::row_number()) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      type_offset = dplyr::case_match(
        .data$metric_type,
        "visit_level" ~ 100L,
        "line_level" ~ 200L,
        .default = 100L
      ),
      covariate_id = analysisId * 1000L + .data$type_offset + .data$metric_offset,
      covariate_name = .formatCovariateName(.data$metric_name, costOfCareSettings),
      analysis_id = analysisId
    )
}

#' Create Covariate Mapping for Person-Level Metrics
#'
#' @description
#' Internal helper that generates a mapping tibble for person-level cost metrics.
#' It creates unique covariate IDs and formatted names for each specified cost column.
#'
#' @param costColNames A character vector of valid column names from the results
#'   data (e.g., `c("cost", "adjusted_cost")`).
#' @param analysisId The base analysis ID for generating covariate IDs.
#' @param costOfCareSettings The `CostOfCareSettings` object for the analysis.
#'
#' @return A tibble with columns: `metric_name`, `metric_offset`, `covariate_id`,
#'   `covariate_name`, and `analysis_id`.
#' @noRd
.createPersonLevelCovariateMapping <- function(costColNames,
                                               analysisId,
                                               costOfCareSettings) {
  purrr::imap_dfr(costColNames, ~ {
    metricName <- .x
    metricOffset <- .y

    dplyr::tibble(
      metric_name = metricName,
      metric_offset = as.integer(metricOffset),
      covariate_id = as.numeric(analysisId * 1000L + 300L + metricOffset),
      covariate_name = .formatCovariateName(metricName, costOfCareSettings),
      analysis_id = as.integer(analysisId)
    )
  })
}

.generateCovariatesFromAggregated <- function(resultsData, covariateMapping, cohortId) {
  resultsData |>
    dplyr::inner_join(covariateMapping, by = c("metric_type", "metric_name")) |>
    dplyr::transmute(
      rowId = as.integer(cohortId),
      covariateId = as.numeric(.data$covariate_id),
      covariateValue = as.numeric(.data$metric_value)
    ) |>
    dplyr::filter(!is.na(.data$covariateValue), .data$covariateValue != 0)
}

#' Generate Person-Level Covariates from a Pivoted Data Frame
#'
#' @description
#' Internal helper that takes person-level results, pivots them to a long format
#' based on the specified cost columns, and joins with the covariate mapping to
#' produce the final `covariates` table.
#'
#' @param resultsData A tibble of person-level results.
#' @param covariateMapping A mapping tibble from `.createPersonLevelCovariateMapping`.
#' @param costColNames A character vector of validated column names to pivot.
#'
#' @return A tibble in the format of the `covariates` table.
#' @noRd
.generateCovariatesFromPersonLevel <- function(resultsData,
                                               covariateMapping,
                                               costColNames) {
  resultsData |>
    tidyr::pivot_longer(
      cols = dplyr::any_of(costColNames), # Use any_of for safety
      names_to = "metric_name",
      values_to = "covariateValue"
    ) |>
    dplyr::inner_join(covariateMapping, by = "metric_name") |>
    dplyr::filter(!is.na(.data$covariateValue), .data$covariateValue != 0) |>
    dplyr::transmute(
      rowId = as.integer(.data$person_id),
      covariateId = as.numeric(.data$covariate_id),
      .data$covariateValue
    )
}

.createCovariateRef <- function(covariateMapping, costOfCareSettings) {
  conceptId <- as.integer(costOfCareSettings$costConceptId %||% 0L)

  covariateMapping |>
    dplyr::transmute(
      covariateId = .data$covariate_id,
      covariateName = .data$covariate_name,
      analysisId = .data$analysis_id,
      conceptId = conceptId
    )
}

.createAnalysisRef <- function(analysisId, costOfCareSettings) {
  dplyr::tibble(
    analysisId = analysisId,
    analysisName = .createAnalysisName(costOfCareSettings),
    domainId = "Cost",
    startDay = as.integer(costOfCareSettings$startOffsetDays %||% 0L),
    endDay = as.integer(costOfCareSettings$endOffsetDays %||% 365L),
    isBinary = "N",
    missingMeansZero = "Y"
  )
}

.createTimeRef <- function(costOfCareSettings) {
  dplyr::tibble(
    timeId = 1L,
    startDay = as.integer(costOfCareSettings$startOffsetDays),
    endDay = as.integer(costOfCareSettings$endOffsetDays)
  )
}

.createMetaData <- function(costOfCareSettings, cohortId, databaseId, analysisId, resultFormat) {
  pkgVer <- tryCatch(as.character(utils::packageVersion("CostUtilization")), error = function(e) "dev")

  list(
    analysisId = analysisId,
    cohortId = cohortId,
    databaseId = databaseId,
    resultFormat = resultFormat,
    costConceptId = costOfCareSettings$costConceptId,
    currencyConceptId = costOfCareSettings$currencyConceptId,
    cpiAdjustment = costOfCareSettings$cpiAdjustment,
    microCosting = costOfCareSettings$microCosting,
    anchorCol = costOfCareSettings$anchorCol,
    startOffsetDays = costOfCareSettings$startOffsetDays,
    endOffsetDays = costOfCareSettings$endOffsetDays,
    packageVersion = pkgVer,
    creationTime = Sys.time()
  )
}
.assembleCovariateData <- function(covariates, covariateRef, analysisRef, timeRef, metaData) {
  covariateData <- Andromeda::andromeda(
    covariates = covariates,
    covariateRef = covariateRef,
    analysisRef = analysisRef,
    timeRef = timeRef
  )
  attr(covariateData, "metaData") <- metaData

  # Assign the S4 class to make it a fully compatible CovariateData object
  class(covariateData) <- "CovariateData"

  return(covariateData)
}

.formatCovariateName <- function(metricName, costOfCareSettings) {
  costConceptId <- costOfCareSettings$costConceptId %||% 31973L
  costType <- dplyr::case_match(
    as.integer(costConceptId),
    31973L ~ "Total Charge", 31985L ~ "Total Cost", 31980L ~ "Paid by Payer",
    31981L ~ "Paid by Patient", 31974L ~ "Patient Copay", 31975L ~ "Patient Coinsurance",
    31976L ~ "Patient Deductible", 31979L ~ "Amount Allowed",
    .default = paste("Cost Concept", costConceptId)
  )

  covariateName <- to_title_case_base(gsub("_", " ", metricName))
  timeWindow <- paste0("(", costOfCareSettings$startOffsetDays, " to ", costOfCareSettings$endOffsetDays, " days)")

  paste(costType, "-", covariateName, timeWindow)
}

.createAnalysisName <- function(costOfCareSettings) {
  baseName <- "Cost Analysis"
  modifiers <- c()
  if (costOfCareSettings$cpiAdjustment) modifiers <- c(modifiers, "CPI-Adjusted")
  if (costOfCareSettings$microCosting) modifiers <- c(modifiers, "Line-Level")
  modifierStr <- if (length(modifiers) > 0) paste0(" (", paste(modifiers, collapse = ", "), ")") else ""
  paste0(baseName, modifierStr)
}
