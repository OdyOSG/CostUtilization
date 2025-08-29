#' Format CostUtilization results in FeatureExtraction aggregated format
#'
#' @description
#' Convert the output from calculateCostOfCare() to an object compatible with
#' the FeatureExtraction aggregated covariate structure. This produces a list
#' with components aggregatedCovariates, aggregatedCovariateRef, analysisRef,
#' and metaData, and assigns class "AggregatedCovariateData".
#'
#' The mapping creates a small set of covariates representing total and rate
#' metrics commonly used downstream (e.g., totalCost, totalAdjustedCost, PPPM, PPPY).
#'
#' @param results Tibble returned as results from calculateCostOfCare().
#'   Expected to contain camelCase columns like totalCost, totalAdjustedCost,
#'   costPppm, costPppq, costPppy, eventsPer1000Py, nPersonsWithCost,
#'   distinctVisits, distinctEvents, totalPersonYears, etc.
#' @param diagnostics Tibble returned as diagnostics from calculateCostOfCare().
#'   Used to infer population size from the '01_person_subset' row.
#' @param settings The CostOfCareSettings object used for the run. Used to populate
#'   analysisRef and metaData.
#' @param cohortId Integer cohort definition id (copied into metaData).
#' @param analysisId Integer analysis id to assign in analysisRef. Default: 400L.
#' @param costConceptName Optional override for human-readable cost type label.
#'   If NULL, inferred from settings$costConceptId.
#'
#' @return A list with elements aggregatedCovariates, aggregatedCovariateRef,
#'   analysisRef, and metaData. The object has class "AggregatedCovariateData".
#' @export
asFeatureExtractionAggregated <- function(
  results,
  diagnostics,
  settings,
  cohortId,
  analysisId = 400L,
  costConceptName = NULL
) {
  rlang::arg_match0(class(settings), values = NULL) # no-op to keep lintr happy
  # defensive checks
  checkmate::assert_data_frame(results, min.rows = 1)
  checkmate::assert_data_frame(diagnostics, min.rows = 1)
  checkmate::assertClass(settings, "CostOfCareSettings")
  checkmate::assertIntegerish(cohortId, len = 1, any.missing = FALSE)
  checkmate::assertIntegerish(analysisId, len = 1, any.missing = FALSE)
  checkmate::assertString(costConceptName, null.ok = TRUE)

  res <- dplyr::as_tibble(results)
  diag <- dplyr::as_tibble(diagnostics)

  # population size from diagnostics (01_person_subset)
  populationSize <- diag |>
    dplyr::filter(.data$stepName %in% c("01_person_subset", "00_initial_cohort")) |>
    dplyr::arrange(.data$stepName) |>
    dplyr::slice_tail(n = 1) |>
    dplyr::pull(.data$nPersons) %||% NA_real_

  # infer readable cost concept name
  costMap <- c(
    `31973` = "Total charge",
    `31985` = "Total cost",
    `31980` = "Paid by payer",
    `31981` = "Paid by patient",
    `31974` = "Patient copay",
    `31975` = "Patient coinsurance",
    `31976` = "Patient deductible",
    `31979` = "Amount allowed"
  )
  costConceptName <- costConceptName %||% purrr::pluck(costMap, as.character(settings$costConceptId), .default = "Cost")

  # pull single-row metrics
  r <- res |> dplyr::slice(1)
  metricType <- r$metricType %||% NA_character_

  # Utility: construct one aggregated covariate row
  make_cov_row <- function(covariateId, meanValue = NA_real_, sumValue = NA_real_, countValue = populationSize) {
    dplyr::tibble(
      covariateId = as.integer(covariateId),
      sumValue = as.numeric(sumValue),
      mean = as.numeric(meanValue),
      standardDeviation = NA_real_,
      minValue = NA_real_,
      p10Value = NA_real_,
      medianValue = NA_real_,
      p90Value = NA_real_,
      maxValue = NA_real_,
      countValue = as.numeric(countValue)
    )
  }

  # We'll assign deterministic covariateIds under the chosen analysisId namespace
  # covariateId = analysisId * 1000 + index
  id_base <- as.integer(analysisId) * 1000L

  # Build covariateRef definitions
  cov_defs <- dplyr::tribble(
    ~idx, ~name, ~field,
    1L,  glue::glue("{costConceptName} - Total cost (metric={metricType})"),              "totalCost",
    2L,  glue::glue("{costConceptName} - Total adjusted cost (metric={metricType})"),     "totalAdjustedCost",
    3L,  glue::glue("Events - Distinct visits (metric={metricType})"),                   "distinctVisits",
    4L,  glue::glue("Events - Distinct events (metric={metricType})"),                   "distinctEvents",
    5L,  glue::glue("Coverage - Persons with any cost (metric={metricType})"),           "nPersonsWithCost",
    6L,  glue::glue("Rate - Cost PPPM (per person per month)"),                          "costPppm",
    7L,  glue::glue("Rate - Adjusted cost PPPM (per person per month)"),                 "adjustedCostPppm",
    8L,  glue::glue("Rate - Cost PPPQ (per person per quarter)"),                        "costPppq",
    9L,  glue::glue("Rate - Adjusted cost PPPQ (per person per quarter)"),               "adjustedCostPppq",
    10L, glue::glue("Rate - Cost PPPY (per person per year)"),                           "costPppy",
    11L, glue::glue("Rate - Adjusted cost PPPY (per person per year)"),                  "adjustedCostPppy",
    12L, glue::glue("Rate - Events per 1000 PY"),                                        "eventsPer1000Py"
  )

  covariateRef <- cov_defs |>
    dplyr::mutate(
      covariateId = id_base + .data$idx,
      covariateName = as.character(.data$name),
      analysisId = as.integer(analysisId),
      conceptId = dplyr::case_when(
        .data$field %in% c("totalCost", "totalAdjustedCost", "costPppm", "adjustedCostPppm", "costPppq", "adjustedCostPppq", "costPppy", "adjustedCostPppy") ~ as.integer(settings$costConceptId),
        TRUE ~ NA_integer_
      )
    ) |>
    dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId)

  # Build aggregatedCovariates values using available metrics
  # For sum-type measures, mean = sum / populationSize; For rate measures (PPP*),
  # we treat them as already means; sum = mean * populationSize.
  is_rate <- function(field) field %in% c("costPppm", "adjustedCostPppm", "costPppq", "adjustedCostPppq", "costPppy", "adjustedCostPppy")

  aggregatedCovariates <- cov_defs |>
    dplyr::mutate(
      covariateId = id_base + .data$idx,
      value = purrr::map_dbl(.data$field, ~ as.numeric(r[[.x]] %||% NA_real_)),
      isRate = purrr::map_lgl(.data$field, is_rate),
      sumValue = dplyr::if_else(.data$isRate, .data$value * populationSize, .data$value),
      meanValue = dplyr::if_else(.data$isRate, .data$value, .data$value / populationSize)
    ) |>
    dplyr::transmute(
      covariateId = .data$covariateId,
      sumValue = .data$sumValue,
      mean = .data$meanValue,
      standardDeviation = NA_real_,
      minValue = NA_real_,
      p10Value = NA_real_,
      medianValue = NA_real_,
      p90Value = NA_real_,
      maxValue = NA_real_,
      countValue = as.numeric(populationSize)
    )

  # analysisRef: use settings to populate window
  startDay <- as.integer(settings$startOffsetDays %||% 0L)
  endDay <- as.integer(settings$endOffsetDays %||% 365L)

  analysisName <- glue::glue("CostUtilization: {costConceptName} ({metricType}); window [{startDay},{endDay}] days")

  analysisRef <- dplyr::tibble(
    analysisId = as.integer(analysisId),
    analysisName = as.character(analysisName),
    domainId = "Cost",
    startDay = startDay,
    endDay = endDay,
    isBinary = 0L
  )

  # metaData: include useful context
  metaData <- list(
    call = match.call(),
    cohortId = as.integer(cohortId),
    metricType = metricType,
    populationSize = as.numeric(populationSize),
    costConceptId = as.integer(settings$costConceptId),
    currencyConceptId = as.integer(settings$currencyConceptId),
    anchorCol = settings$anchorCol,
    window = list(startOffsetDays = startDay, endOffsetDays = endDay),
    hasEventFilters = isTRUE(settings$hasEventFilters),
    hasVisitRestriction = isTRUE(settings$hasVisitRestriction),
    microCosting = isTRUE(settings$microCosting),
    createdDate = Sys.time()
  )

  out <- list(
    aggregatedCovariates = aggregatedCovariates,
    aggregatedCovariateRef = covariateRef,
    analysisRef = analysisRef,
    metaData = metaData
  )
  class(out) <- c("AggregatedCovariateData", class(out))
  out
}
