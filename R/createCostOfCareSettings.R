#' Create Cost of Care Settings
#'
#' @description
#' Create a validated settings object for the `calculateCostOfCare` analysis.
#' This is the recommended way to specify analysis parameters.
#'
#' @param anchorCol Character; which cohort column anchors the analysis window.
#'   One of `"cohort_start_date"` or `"cohort_end_date"`. Default: `"cohort_start_date"`.
#' @param startOffsetDays Integer; days to add to the anchor date for window start
#'   (can be negative). Default: 0.
#' @param endOffsetDays Integer; days to add to the anchor date for window end.
#'   Must be greater than `startOffsetDays`. Default: 365.
#' @param restrictVisitConceptIds Optional integer vector of visit concept IDs to restrict analysis.
#'   If provided, only visits with these concept IDs are considered.
#' @param eventFilters Optional list of event filters (see Details). Each filter defines a set of
#'   concepts by OMOP domain; if provided, they can be used to constrain qualifying events/visits.
#' @param microCosting Logical; if `TRUE`, performs line-level costing at the `visit_detail` level.
#'   Requires `eventFilters` and `primaryEventFilterName`. Default: `FALSE`.
#' @param primaryEventFilterName Character; when `microCosting = TRUE`, the name of the primary
#'   event filter (one of the `eventFilters[[]]$name`) that identifies the line-level events to cost.
#' @param costConceptId Integer; concept ID for the cost type. Default: `31973` (charged).
#' @param currencyConceptId Integer; concept ID for the currency. Default: `44818668` (USD).
#' @param additionalCostConceptIds Optional integer vector of additional cost concept IDs to include.
#'   These can be used by downstream SQL to widen the cost types considered.
#' @param cpiAdjustment Logical; if `TRUE`, adjust costs using CPI factors from `cpiFilePath`. Default: `FALSE`.
#' @param cpiFilePath Optional character path to a CPI adjustment table/file. Required if `cpiAdjustment = TRUE`.
#'
#' @details
#' **Event filters structure**
#'
#' The `eventFilters` argument must be a list of lists, where each inner list has:
#' \itemize{
#'   \item `name`: A unique character string for the filter.
#'   \item `domain`: The OMOP domain (one of `"All"`, `"Drug"`, `"Condition"`, `"Procedure"`,
#'         `"Observation"`, `"Measurement"`, `"Device"`, `"Visit"`).
#'   \item `conceptIds`: An integer vector of concept IDs (length \eqn{\ge} 1).
#' }
#'
#' **CPI adjustment**
#'
#' If `cpiAdjustment = TRUE`, you must provide `cpiFilePath`. Your pipeline should read this
#' into a table that exposes, at minimum, a `year` and an `adj_factor` column; downstream SQL
#' joins on the `year` extracted from cost dates. The function only validates the file path;
#' loading/attaching the table is left to the calling workflow.
#'
#' @return A `CostOfCareSettings` object (list with class) containing:
#' \itemize{
#'   \item `anchorCol`, `startOffsetDays`, `endOffsetDays`
#'   \item `hasVisitRestriction`, `restrictVisitConceptIds`
#'   \item `hasEventFilters`, `eventFilters`, `nFilters`
#'   \item `microCosting`, `primaryEventFilterName`
#'   \item `costConceptId`, `currencyConceptId`, `additionalCostConceptIds`
#'   \item `cpiAdjustment`, `cpiFilePath`
#' }
#' @export
createCostOfCareSettings <- function(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0L,
    endOffsetDays = 365L,
    restrictVisitConceptIds = NULL,
    eventFilters = NULL,
    microCosting = FALSE,
    primaryEventFilterName = NULL,
    costConceptId = 31973,
    currencyConceptId = 44818668,
    additionalCostConceptIds = NULL,
    cpiAdjustment = FALSE,
    cpiFilePath = NULL) {
  # --- Input Validation with checkmate ---
  errorMessages <- checkmate::makeAssertCollection()

  # anchor column
  checkmate::assertChoice(
    anchorCol,
    choices = c("cohort_start_date", "cohort_end_date"),
    add = errorMessages
  )

  # offsets
  checkmate::assertIntegerish(startOffsetDays, len = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertIntegerish(endOffsetDays, len = 1, any.missing = FALSE, add = errorMessages)

  # costs/currency
  checkmate::assertIntegerish(costConceptId, len = 1, lower = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertIntegerish(currencyConceptId, len = 1, lower = 1, any.missing = FALSE, add = errorMessages)

  # optional additional cost concepts
  if (!is.null(additionalCostConceptIds)) {
    checkmate::assertIntegerish(additionalCostConceptIds, lower = 1, any.missing = FALSE, unique = TRUE, add = errorMessages)
  }

  # flags
  checkmate::assertFlag(microCosting, add = errorMessages)
  checkmate::assertFlag(cpiAdjustment, add = errorMessages)

  # visit restrictions
  if (!is.null(restrictVisitConceptIds)) {
    checkmate::assertIntegerish(restrictVisitConceptIds, lower = 1, min.len = 1, unique = TRUE, any.missing = FALSE, add = errorMessages)
  }

  # event filters (structural validation after assertions)
  if (!is.null(eventFilters)) {
    # placeholder; detailed structural validation below
    checkmate::assertList(eventFilters, add = errorMessages)
  }

  # collect assertion failures (so far)
  checkmate::reportAssertions(errorMessages)

  # semantic checks
  if (endOffsetDays <= startOffsetDays) {
    cli::cli_abort(c(
      "Invalid time window specification",
      "x" = "endOffsetDays ({endOffsetDays}) must be greater than startOffsetDays ({startOffsetDays})",
      "i" = "The analysis window must have a positive duration."
    ))
  }

  # event filters detailed validation
  if (!is.null(eventFilters)) {
    validateEventFilters(eventFilters)
    nFilters <- length(eventFilters)
    cli::cli_inform(c("i" = "Configured {nFilters} event filter{?s} for analysis"))
  } else {
    nFilters <- 0L
  }

  # micro-costing constraints
  if (isTRUE(microCosting)) {
    if (is.null(primaryEventFilterName) || !is.character(primaryEventFilterName) || length(primaryEventFilterName) != 1L || nchar(primaryEventFilterName) == 0L) {
      cli::cli_abort(c(
        "Micro-costing requires a primary event filter",
        "x" = "primaryEventFilterName must be a non-empty character string"
      ))
    }
    if (is.null(eventFilters)) {
      cli::cli_abort(c(
        "Micro-costing requires event filters",
        "x" = "eventFilters is NULL but microCosting is TRUE",
        "i" = "Define at least one event filter for micro-costing analysis."
      ))
    }
    filterNames <- vapply(eventFilters, function(f) f$name, character(1))
    if (!primaryEventFilterName %in% filterNames) {
      cli::cli_abort(c(
        "Primary event filter not found",
        "x" = "'{primaryEventFilterName}' is not in the event filters",
        "i" = "Available filters: {.val {filterNames}}"
      ))
    }
  }

  # CPI constraints
  if (isTRUE(cpiAdjustment)) {
    if (is.null(cpiFilePath) || !is.character(cpiFilePath) || length(cpiFilePath) != 1L || nchar(cpiFilePath) == 0L) {
      cli::cli_abort(c(
        "CPI adjustment enabled but no CPI file provided",
        "x" = "cpiFilePath must be a non-empty path when cpiAdjustment = TRUE"
      ))
    }
    if (!file.exists(cpiFilePath)) {
      cli::cli_abort(c(
        "CPI file not found",
        "x" = "File does not exist: '{cpiFilePath}'",
        "i" = "Provide a valid path to CPI adjustment data."
      ))
    }
  }

  # helpful notices
  if (!is.null(restrictVisitConceptIds)) {
    cli::cli_inform(c("i" = "Analysis will be restricted to {length(restrictVisitConceptIds)} visit concept{?s}."))
  }

  # --- Create Settings Object ---
  settings <- structure(
    list(
      anchorCol = anchorCol,
      startOffsetDays = as.integer(startOffsetDays),
      endOffsetDays = as.integer(endOffsetDays),
      hasVisitRestriction = !is.null(restrictVisitConceptIds),
      restrictVisitConceptIds = if (is.null(restrictVisitConceptIds)) NULL else as.integer(restrictVisitConceptIds),
      hasEventFilters = !is.null(eventFilters),
      eventFilters = eventFilters,
      nFilters = as.integer(nFilters),
      microCosting = isTRUE(microCosting),
      primaryEventFilterName = primaryEventFilterName,
      costConceptId = as.integer(costConceptId),
      currencyConceptId = as.integer(currencyConceptId),
      additionalCostConceptIds = if (is.null(additionalCostConceptIds)) NULL else as.integer(additionalCostConceptIds),
      cpiAdjustment = isTRUE(cpiAdjustment),
      cpiFilePath = cpiFilePath
    ),
    class = "CostOfCareSettings"
  )

  settings
}

#' Validate Event Filters
#'
#' @description
#' Internal function to validate the structure of event filters.
#'
#' @param eventFilters List of event filter specifications
#' @noRd
validateEventFilters <- function(eventFilters) {
  if (!is.list(eventFilters)) {
    cli::cli_abort(c(
      "Invalid event filters format",
      "x" = "eventFilters must be a list",
      "i" = "Received {.cls {class(eventFilters)}}"
    ))
  }

  # Check each filter
  purrr::walk(seq_along(eventFilters), ~ {
    filter <- eventFilters[[.x]]

    if (!is.list(filter)) {
      cli::cli_abort(c(
        "Invalid event filter structure",
        "x" = "Filter {.x} is not a list",
        "i" = "Each filter must be a list with 'name', 'domain', and 'conceptIds'"
      ))
    }

    # Check required fields
    requiredFields <- c("name", "domain", "conceptIds")
    missingFields <- setdiff(requiredFields, names(filter))

    if (length(missingFields) > 0) {
      cli::cli_abort(c(
        "Missing required fields in event filter {.x}",
        "x" = "Missing: {.field {missingFields}}",
        "i" = "Each filter must have: {.field {requiredFields}}"
      ))
    }

    # Validate name
    if (!is.character(filter$name) || length(filter$name) != 1 || nchar(filter$name) == 0) {
      cli::cli_abort(c(
        "Invalid filter name in event filter {.x}",
        "x" = "name must be a non-empty character string",
        "i" = "Received: {.val {filter$name}}"
      ))
    }

    # Validate domain
    validDomains <- c(
      "Drug", "Condition", "Procedure", "Observation",
      "Measurement", "Device", "Visit", "All"
    )

    if (!filter$domain %in% validDomains) {
      cli::cli_abort(c(
        "Invalid domain in event filter {.x}",
        "x" = "'{filter$domain}' is not a valid OMOP domain",
        "i" = "Valid domains: {.val {validDomains}}"
      ))
    }

    # Validate concept IDs
    checkmate::assertIntegerish(
      filter$conceptIds,
      lower = 1,
      min.len = 1,
      any.missing = FALSE,
      .var.name = glue::glue("conceptIds in filter '{filter$name}'")
    )
  })

  # Check for duplicate names
  filterNames <- purrr::map_chr(eventFilters, "name")
  duplicateNames <- filterNames[duplicated(filterNames)]

  if (length(duplicateNames) > 0) {
    cli::cli_abort(c(
      "Duplicate filter names detected",
      "x" = "The following names appear multiple times: {.val {unique(duplicateNames)}}",
      "i" = "Each event filter must have a unique name"
    ))
  }

  invisible(TRUE)
}
