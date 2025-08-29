#' Map CostUtilization results to FeatureExtraction aggregated format
#'
#' @description
#' Convert CostUtilization query outputs (either aggregated metric rows or
#' person-level rows), provided as Andromeda tables or data frames, into an
#' object compatible with FeatureExtraction's AggregatedCovariateData format.
#'
#' Supported input shapes for `results`:
#' - Aggregated: columns [metric_type, metric_name, metric_value]
#' - Person-level: columns [person_id, cost, (optional) adjusted_cost]
#'
#' Diagnostics (optional) are used to derive population size. If not provided,
#' and results are person-level, population size is inferred from distinct
#' person_id in results; otherwise left as NA.
#'
#' @param results An Andromeda object (with a table named `results`) or a
#'   data.frame/tibble with one of the supported shapes.
#' @param diagnostics Optional Andromeda object (with a table `diagnostics`) or
#'   data.frame/tibble containing at least [stepName, nPersons].
#' @param settings CostOfCareSettings used to generate results.
#' @param cohortId Integer cohort id.
#' @param analysisId Integer analysis id base; covariateId is composed as
#'   analysisId*1000 + index.
#' @param costConceptName Optional override for readable cost concept label.
#' @param tableName Name of table inside Andromeda for results (default: "results").
#' @param diagTableName Name of table inside Andromeda for diagnostics (default: "diagnostics").
#'
#' @return A list with elements aggregatedCovariates, aggregatedCovariateRef,
#'   analysisRef, metaData, with class "AggregatedCovariateData".
#' @export
#' @examples
#' if (requireNamespace("Andromeda", quietly = TRUE)) {
#'   andr <- Andromeda::andromeda()
#'   # Aggregated shape example
#'   Andromeda::insertTable(andr, "results", data.frame(
#'     metric_type = "visit_level",
#'     metric_name = c("total_cost", "cost_pppm"),
#'     metric_value = c(10000, 123.45)
#'   ))
#'
#'   fe <- asFeatureExtractionAggregated(
#'     results = andr,
#'     diagnostics = NULL,
#'     settings = createCostOfCareSettings(),
#'     cohortId = 1,
#'     analysisId = 401L
#'   )
#'   class(fe)
#'   Andromeda::close(andr)
#' }
#'

asFeatureExtractionAggregated <- function(
  results,
  diagnostics = NULL,
  settings,
  cohortId,
  analysisId = 400L,
  costConceptName = NULL,
  tableName = "results",
  diagTableName = "diagnostics"
) {
  stopifnot(!missing(results), !missing(settings), !missing(cohortId))

  # Helper: fetch from Andromeda or pass-through
  fetch_tbl <- function(x, name = NULL) {
    if (inherits(x, "Andromeda")) {
      if (is.null(name) || !name %in% names(x)) {
        rlang::abort(glue::glue("Table '{name}' not found in Andromeda input"))
      }
      return(dplyr::collect(x[[name]]))
    } else if (inherits(x, "tbl_dbi") || inherits(x, "tbl_lazy")) {
      return(dplyr::collect(x))
    } else if (is.null(x)) {
      return(NULL)
    } else {
      return(dplyr::as_tibble(x))
    }
  }

  res <- fetch_tbl(results, tableName)
  diag <- fetch_tbl(diagnostics, diagTableName)

  # Population size
  populate_size_from_diag <- function(diag_df) {
    if (is.null(diag_df) || nrow(diag_df) == 0) return(NA_real_)
    cols <- names(diag_df)
    step_col <- dplyr::case_when(
      "stepName" %in% cols ~ "stepName",
      "step_name" %in% cols ~ "step_name",
      TRUE ~ NA_character_
    )
    n_col <- dplyr::case_when(
      "nPersons" %in% cols ~ "nPersons",
      "n_persons" %in% cols ~ "n_persons",
      TRUE ~ NA_character_
    )
    if (is.na(step_col) || is.na(n_col)) return(NA_real_)
    diag_df |>
      dplyr::rename(stepName = dplyr::all_of(step_col), nPersons = dplyr::all_of(n_col)) |>
      dplyr::filter(.data$stepName %in% c("01_person_subset", "00_initial_cohort")) |>
      dplyr::arrange(.data$stepName) |>
      dplyr::slice_tail(n = 1) |>
      dplyr::pull(.data$nPersons) |>
      as.numeric() |>
      purrr::pluck(1, .default = NA_real_)
  }

  populationSize <- populate_size_from_diag(diag)

  # Identify result shape
  cols <- names(res)
  is_aggregated <- all(c("metric_type", "metric_name", "metric_value") %in% cols)
  is_person_level <- all(c("person_id", "cost") %in% cols)

  if (!is_aggregated && !is_person_level) {
    rlang::abort("Unsupported results shape: expected aggregated (metric_type/name/value) or person-level (person_id, cost[, adjusted_cost]).")
  }

  # Cost concept name mapper
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
  costConceptName <- costConceptName %||%
    purrr::pluck(costMap, as.character(settings$costConceptId), .default = "Cost")

  id_base <- as.integer(analysisId) * 1000L
  # Metric classification
  rate_metrics <- c(
    "cost_pppm", "adjusted_cost_pppm",
    "cost_pppq", "adjusted_cost_pppq",
    "cost_pppy", "adjusted_cost_pppy",
    "events_per_1000_py", "visits_per_1000_py",
    "visit_dates_per_1000_py", "visit_details_per_1000_py"
  )

  # Build from aggregated rows
  if (is_aggregated) {
    r <- res |>
      dplyr::mutate(
        metric_type = as.character(.data$metric_type),
        metric_name = as.character(.data$metric_name),
        metric_value = as.numeric(.data$metric_value)
      )

    # Try to infer population size from diagnostics; if missing, leave NA
    # Build covariate ref from unique metric names
    cov_defs <- r |>
      dplyr::distinct(.data$metric_type, .data$metric_name) |>
      dplyr::arrange(.data$metric_name) |>
      dplyr::mutate(idx = dplyr::row_number()) |>
      dplyr::mutate(
        covariateId = id_base + .data$idx,
        covariateName = dplyr::case_when(
          grepl("^total_cost$", .data$metric_name) ~ glue::glue("{costConceptName} - Total cost (metric={metric_type})"),
          grepl("^total_adjusted_cost$", .data$metric_name) ~ glue::glue("{costConceptName} - Total adjusted cost (metric={metric_type})"),
          grepl("^n_persons_with_cost$", .data$metric_name) ~ glue::glue("Coverage - Persons with any cost (metric={metric_type})"),
          grepl("^distinct_visits$", .data$metric_name) ~ glue::glue("Events - Distinct visits (metric={metric_type})"),
          grepl("^distinct_events$", .data$metric_name) ~ glue::glue("Events - Distinct events (metric={metric_type})"),
          grepl("^visits_per_1000_py$", .data$metric_name) ~ glue::glue("Rate - Visits per 1000 PY (metric={metric_type})"),
          grepl("^visit_dates_per_1000_py$", .data$metric_name) ~ glue::glue("Rate - Visit dates per 1000 PY (metric={metric_type})"),
          grepl("^visit_details_per_1000_py$", .data$metric_name) ~ glue::glue("Rate - Visit details per 1000 PY (metric={metric_type})"),
          grepl("^events_per_1000_py$", .data$metric_name) ~ glue::glue("Rate - Events per 1000 PY (metric={metric_type})"),
          grepl("^cost_pppm$", .data$metric_name) ~ glue::glue("Rate - Cost PPPM (metric={metric_type})"),
          grepl("^adjusted_cost_pppm$", .data$metric_name) ~ glue::glue("Rate - Adjusted cost PPPM (metric={metric_type})"),
          grepl("^cost_pppq$", .data$metric_name) ~ glue::glue("Rate - Cost PPPQ (metric={metric_type})"),
          grepl("^adjusted_cost_pppq$", .data$metric_name) ~ glue::glue("Rate - Adjusted cost PPPQ (metric={metric_type})"),
          grepl("^cost_pppy$", .data$metric_name) ~ glue::glue("Rate - Cost PPPY (metric={metric_type})"),
          grepl("^adjusted_cost_pppy$", .data$metric_name) ~ glue::glue("Rate - Adjusted cost PPPY (metric={metric_type})"),
          TRUE ~ glue::glue("Metric - {metric_name} (metric={metric_type})")
        ),
        analysisId = as.integer(analysisId),
        conceptId = dplyr::if_else(
          .data$metric_name %in% c("total_cost", "total_adjusted_cost",
                                   "cost_pppm", "adjusted_cost_pppm",
                                   "cost_pppq", "adjusted_cost_pppq",
                                   "cost_pppy", "adjusted_cost_pppy"),
          as.integer(settings$costConceptId),
          as.integer(NA)
        )
      )

    covariateRef <- cov_defs |>
      dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId)

    # aggregatedCovariates
    # Join measured values by metric_name/type -> covariateId
    agg_tbl <- r |>
      dplyr::inner_join(
        cov_defs |> dplyr::select(.data$metric_type, .data$metric_name, .data$covariateId),
        by = c("metric_type", "metric_name")
      ) |>
      dplyr::mutate(
        isRate = .data$metric_name %in% rate_metrics,
        sumValue = dplyr::if_else(.data$isRate, .data$metric_value * populationSize, .data$metric_value),
        meanValue = dplyr::if_else(.data$isRate, .data$metric_value, .data$metric_value / populationSize)
      ) |>
      dplyr::transmute(
        covariateId = .data$covariateId,
        sumValue = as.numeric(.data$sumValue),
        mean = as.numeric(.data$meanValue),
        standardDeviation = NA_real_,
        minValue = NA_real_,
        p10Value = NA_real_,
        medianValue = NA_real_,
        p90Value = NA_real_,
        maxValue = NA_real_,
        countValue = as.numeric(populationSize)
      )

  } else {
    # Person-level: compute stats for cost and optionally adjusted_cost
    # If diagnostics missing, infer population size
    if (is.na(populationSize)) {
      populationSize <- res |>
        dplyr::distinct(.data$person_id) |>
        nrow() |>
        as.numeric()
    }

    # A helper to compute summary stats for a numeric vector
    summarize_numeric <- function(x) {
      x <- as.numeric(x)
      x <- x[is.finite(x)]
      tibble::tibble(
        sumValue = sum(x, na.rm = TRUE),
        mean = mean(x, na.rm = TRUE),
        standardDeviation = stats::sd(x, na.rm = TRUE),
        minValue = suppressWarnings(min(x, na.rm = TRUE)),
        p10Value = as.numeric(stats::quantile(x, probs = 0.10, na.rm = TRUE, names = FALSE, type = 7)),
        medianValue = as.numeric(stats::quantile(x, probs = 0.50, na.rm = TRUE, names = FALSE, type = 7)),
        p90Value = as.numeric(stats::quantile(x, probs = 0.90, na.rm = TRUE, names = FALSE, type = 7)),
        maxValue = suppressWarnings(max(x, na.rm = TRUE))
      )
    }

    # Define covariates present
    available <- c("cost", "adjusted_cost")[c("cost", "adjusted_cost") %in% names(res)]
    if (length(available) == 0) rlang::abort("Person-level results must contain 'cost' and/or 'adjusted_cost' columns.")

    cov_defs <- tibble::tibble(
      idx = seq_along(available),
      metric_name = available,
      covariateId = id_base + idx,
      covariateName = dplyr::case_when(
        available == "cost" ~ glue::glue("{costConceptName} - Person total cost"),
        available == "adjusted_cost" ~ glue::glue("{costConceptName} - Person adjusted cost"),
        TRUE ~ glue::glue("{available}")
      ),
      analysisId = as.integer(analysisId),
      conceptId = as.integer(settings$costConceptId)
    )

    covariateRef <- cov_defs |>
      dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId)

    # Compute stats for each metric
    agg_tbl <- purrr::map_dfr(seq_along(available), function(i) {
      col <- available[[i]]
      stats_tbl <- summarize_numeric(res[[col]])
      dplyr::bind_cols(tibble::tibble(covariateId = cov_defs$covariateId[[i]]), stats_tbl)
    }) |>
      dplyr::mutate(countValue = as.numeric(populationSize))
  }

  analysisRef <- dplyr::tibble(
    analysisId = as.integer(analysisId),
    analysisName = as.character(glue::glue(
      "CostUtilization: {costConceptName}; window [{settings$startOffsetDays %||% 0L},{settings$endOffsetDays %||% 365L}] days")),
    domainId = "Cost",
    startDay = as.integer(settings$startOffsetDays %||% 0L),
    endDay = as.integer(settings$endOffsetDays %||% 365L),
    isBinary = 0L
  )

  metaData <- list(
    call = match.call(),
    cohortId = as.integer(cohortId),
    populationSize = as.numeric(populationSize),
    costConceptId = as.integer(settings$costConceptId),
    currencyConceptId = as.integer(settings$currencyConceptId),
    anchorCol = settings$anchorCol,
    window = list(
      startOffsetDays = as.integer(settings$startOffsetDays %||% 0L),
      endOffsetDays = as.integer(settings$endOffsetDays %||% 365L)
    ),
    hasEventFilters = isTRUE(settings$hasEventFilters),
    hasVisitRestriction = isTRUE(settings$hasVisitRestriction),
    microCosting = isTRUE(settings$microCosting),
    createdDate = Sys.time(),
    input = list(
      isAggregated = is_aggregated,
      isPersonLevel = is_person_level,
      isAndromeda = inherits(results, "Andromeda")
    )
  )

  out <- list(
    aggregatedCovariates = agg_tbl,
    aggregatedCovariateRef = covariateRef,
    analysisRef = analysisRef,
    metaData = metaData
  )
  class(out) <- c("AggregatedCovariateData", class(out))
  out
}
