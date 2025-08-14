#' Create Cost and Utilization Settings
#'
#' @description
#' Creates a settings object for configuring a cost and healthcare resource utilization analysis.
#' This object is used by the `computeCostUtilization` function.
#'
#' @section Precedence Rules:
#' Some arguments take precedence over others if both are provided:
#' \itemize{
#'   \item \strong{`useConceptSet` > `costDomains`}: If `useConceptSet` is specified, it provides a
#'   precise definition of the events to be included in the analysis. The broader `costDomains`
#'   argument will be ignored, and a warning will be issued.
#' }
#'
#' @param timeWindows             A list of numeric vectors, where each vector has two elements:
#'                                the start and end day relative to the cohort start date.
#'                                For example, `list(c(-365, 0))` defines a 1-year lookback window.
#' @param useInCohortWindow       A logical flag. If TRUE, the analysis will include the
#'                                time a person is inside the cohort (from `cohort_start_date` to
#'                                `cohort_end_date`) as a distinct analysis window.
#' @param costDomains             A character vector of cost domains to include ('Condition', 'Drug').
#'                                This argument is ignored if `useConceptSet` is provided. (case independent)
#' @param useConceptSet           An optional concept set to restrict the events used for cost and
#'                                utilization calculation. This provides fine-grained control over
#'                                which events contribute to the totals. The input can be a numeric
#'                                vector of concept IDs, a `Capr` object, or a standard list-based
#'                                concept set expression.
#' @param costTypeConceptIds      A numeric vector of `cost_type_concept_id`s to restrict the
#'                                analysis to. If NULL, all cost types are included.
#' @param currencyConceptId       The `currency_concept_id` to filter costs on. Defaults to
#'                                44818668 (US Dollar).
#' @param aggregate               A character vector specifying the aggregation level for per-patient
#'                                metrics. The calculation is based on the total cost/utilization
#'                                divided by the total observed person-days in a window, which is then
#'                                scaled to the desired time unit. Supported options are:
#'                                \itemize{
#'                                  \item{\code{'pppd'}: Per-Patient Per-Day. The base rate, calculated as total value / total observation days.}
#'                                  \item{\code{'pppm'}: Per-Patient Per-Month. The per-day rate multiplied by the average days in a month (30.44).}
#'                                  \item{\code{'pppq'}: Per-Patient Per-Quarter. The per-day rate multiplied by the average days in a quarter (91.31).}
#'                                  \item{\code{'pppy'}: Per-Patient Per-Year. The per-day rate multiplied by the average days in a year (365.25).}
#'                                }
#' @param standardizationData     A data frame used to standardize costs. If NULL, no
#'                                standardization is performed.
#'
#' @return
#' An object of class `costUtilizationSettings`.
#'
#' @export
createCostUtilizationSettings <- function(
    timeWindows = list(c(-365, 0)),
    useInCohortWindow = FALSE,
    costDomains = NULL,
    useConceptSet = NULL,
    costTypeConceptIds = NULL,
    currencyConceptId = 44818668, # US dollar
    aggregate = c("pppm", "pppy"),
    standardizationData = NULL) {
  # --- 1. Initial Argument Validation ---
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertList(timeWindows, types = "numeric", null.ok = TRUE, add = errorMessages)
  if (!is.null(timeWindows)) {
    purrr::walk(timeWindows, ~ checkmate::assertNumeric(.x, len = 2, add = errorMessages))
  }
  checkmate::assertFlag(useInCohortWindow, add = errorMessages)
  if (is.null(timeWindows) && !useInCohortWindow) {
    errorMessages$push("At least one windowing strategy must be used.")
  }
  checkmate::assertCharacter(costDomains, null.ok = TRUE, add = errorMessages)
  if (!is.null(useConceptSet)) {
    checkmate::assert(
      checkmate::checkIntegerish(useConceptSet),
      checkmate::checkList(useConceptSet),
      checkmate::checkClass(useConceptSet, "ConceptSet"),
      combine = "or",
      add = errorMessages,
      .var.name = "useConceptSet"
    )
  }
  checkmate::assertIntegerish(costTypeConceptIds, null.ok = TRUE, add = errorMessages)
  checkmate::assertInt(currencyConceptId, add = errorMessages)
  checkmate::assertCharacter(aggregate, min.len = 1, add = errorMessages)
  checkmate::assertSubset(aggregate, choices = c("pppd", "pppm", "pppq", "pppy"), add = errorMessages)
  checkmate::assertDataFrame(standardizationData, null.ok = TRUE, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  settings_env <- as.list(environment())
  clean_settings <- .normalizeSettings(settings_env$settings)

  conceptSetDefinition <- tryCatch(
    {
      .parseConceptSet(clean_settings$useConceptSet)
    },
    error = function(e) {
      stop(e$message, call. = FALSE)
    }
  )

  structure(
    list(
      timeWindows = clean_settings$timeWindows,
      useInCohortWindow = clean_settings$useInCohortWindow,
      costDomains = clean_settings$costDomains,
      conceptSetDefinition = conceptSetDefinition,
      costTypeConceptIds = clean_settings$costTypeConceptIds,
      currencyConceptId = clean_settings$currencyConceptId,
      aggregate = clean_settings$aggregate,
      standardizationData = clean_settings$standardizationData
    ),
    class = "costUtilizationSettings"
  )
}


#' Print Cost and Utilization Settings
#'
#' @description
#' Provides a formatted, human-readable summary of the `costUtilizationSettings` object.
#'
#' @param x An object of class `costUtilizationSettings`.
#' @param ... For future extensions. Not used in this method.
#' @export
print.costUtilizationSettings <- function(x, ...) {
  cli::cli_h1("Cost & Utilization Analysis Settings")

  # --- Windowing Strategy ---
  cli::cli_h2("Windowing Strategy")
  if (!is.null(x$timeWindows) && length(x$timeWindows) > 0) {
    window_strings <- purrr::map_chr(x$timeWindows, ~ paste0("(", .x[1], "d to ", .x[2], "d)"))
    cli::cli_bullets(c("*" = "Fixed Time Windows (relative to cohort start):", " " = window_strings))
  }
  cli::cli_alert(paste("Analyze time within cohort period (start to end date):", cli::style_bold(x$useInCohortWindow)))
  cli::cli_text() # Add a blank line

  # --- Event & Cost Parameters ---
  cli::cli_h2("Event & Cost Parameters")
  cli::cli_text("The primary event filter is determined by the following (in order of precedence):")

  # This logic now reflects the precedence rule. It shows what is *actually* being used.
  if (!is.null(x$conceptSetDefinition)) {
    cli::cli_rule("1. By Custom Concept Set")
    cli::cli_alert_success("A concept set is being used to define events.")
    cli::cli_li(items = c("Total entries in concept set: {cli::style_bold(nrow(x$conceptSetDefinition))}"))
  } else if (!is.null(x$costDomains)) {
    cli::cli_rule("2. By Cost Domain")
    domains_str <- paste(x$costDomains, collapse = ", ")
    cli::cli_alert_info("Events are being filtered by cost domain.")
    cli::cli_li(items = c("Included domains: {cli::style_bold(domains_str)}"))
  } else {
    cli::cli_rule("3. By Default (All Domains)")
    cli::cli_alert_info("No specific event filter provided; all cost domains will be included.")
  }
  cli::cli_rule()
  cost_types_str <- if (is.null(x$costTypeConceptIds)) "All Types" else paste(x$costTypeConceptIds, collapse = ", ")
  cli::cli_li(items = c("Cost Type Concept IDs: {cli::style_bold(cost_types_str)}"))
  cli::cli_li(items = c("Currency Concept ID: {cli::style_bold(x$currencyConceptId)}"))
  cli::cli_text()

  # --- Aggregation & Standardization ---
  cli::cli_h2("Aggregation & Standardization")
  cli::cli_li(items = c("Aggregation Intervals: {cli::style_bold(toupper(paste(x$aggregate, collapse = ', ')))}"))

  if (!is.null(x$standardizationData)) {
    cli::cli_alert_success("Cost Standardization is ENABLED")
    cli::cli_li(items = c("Standardization Data: Provided ({nrow(x$standardizationData)} rows)"))
  } else {
    cli::cli_alert_info("Cost Standardization is DISABLED")
  }
  invisible(x)
}
