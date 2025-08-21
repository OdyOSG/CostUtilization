#' Create Cost of Care Settings
#'
#' @description
#' Creates a validated settings object for the `calculateCostOfCare` analysis. This
#' is the recommended way to specify analysis parameters.
#'
#' @param anchorCol Column to use as anchor for time window ("cohort_start_date" or "cohort_end_date").
#' @param startOffsetDays Integer, days to add to anchor date for window start (can be negative).
#' @param endOffsetDays Integer, days to add to anchor date for window end.
#' @param restrictVisitConceptIds Optional vector of visit concept IDs to restrict analysis.
#' @param eventFilters Optional list of event filters. See details for structure.
#' @param microCosting Logical; if TRUE, performs line-level costing at the visit_detail level.
#' @param primaryEventFilterName For micro-costing, the name of the primary event filter.
#' @param costConceptId Concept ID for the cost type. Default: 31978 (total charge).
#' @param currencyConceptId Concept ID for the currency. Default: 44818668 (USD).
#' @param additionalCostConceptIds Optional vector of additional cost concept IDs to analyze.
#'
#' @details
#' The `eventFilters` argument must be a list of lists, where each inner list has:
#' \itemize{
#'   \item `name`: A unique character string for the filter.
#'   \item `domain`: The OMOP domain (e.g., "Drug", "Procedure").
#'   \item `conceptIds`: A vector of integer concept IDs.
#' }
#'
#' @return A `CostOfCareSettings` object.
#' @export
#' 
createCostOfCareSettings <- function(
    anchorCol = c("cohort_start_date"),
    startOffsetDays = 0L,
    endOffsetDays = 365L,
    restrictVisitConceptIds = NULL,
    eventFilters = NULL,
    microCosting = FALSE,
    primaryEventFilterName = NULL,
    costConceptId = 31978L,
    currencyConceptId = 44818668L,
    additionalCostConceptIds = NULL
) {
  # --- Input Validation with checkmate ---
  errorMessages <- checkmate::makeAssertCollection()
  
  # Validate anchor column
  checkmate::assertChoice(
    anchorCol, 
    choices = c("cohort_start_date", "cohort_end_date"),
    add = errorMessages
  )
  
  # Validate offset days
  checkmate::assertIntegerish(
    startOffsetDays, 
    len = 1, 
    any.missing = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    endOffsetDays, 
    len = 1, 
    any.missing = FALSE,
    add = errorMessages
  )
  
  # Validate cost concept IDs
  checkmate::assertIntegerish(
    costConceptId, 
    len = 1, 
    lower = 1,
    any.missing = FALSE,
    add = errorMessages
  )
  checkmate::assertIntegerish(
    currencyConceptId, 
    len = 1, 
    lower = 1,
    any.missing = FALSE,
    add = errorMessages
  )
  
  # Validate boolean flags
  checkmate::assertFlag(microCosting, add = errorMessages)
  
  # Report any basic validation errors
  checkmate::reportAssertions(errorMessages)
  
  # Additional validation logic
  if (endOffsetDays <= startOffsetDays) {
    cli::cli_abort(c(
      "Invalid time window specification",
      "x" = "endOffsetDays ({endOffsetDays}) must be greater than startOffsetDays ({startOffsetDays})",
      "i" = "The analysis window must have a positive duration"
    ))
  }
  
  # Validate visit restriction
  if (!is.null(restrictVisitConceptIds)) {
    checkmate::assertIntegerish(
      restrictVisitConceptIds, 
      lower = 1, 
      min.len = 1, 
      unique = TRUE,
      any.missing = FALSE
    )
    cli::cli_inform(c(
      "i" = "Analysis will be restricted to {length(restrictVisitConceptIds)} visit concept{?s}"
    ))
  }
  
  # Validate event filters
  if (!is.null(eventFilters)) {
    validateEventFilters(eventFilters)
    nFilters <- length(eventFilters)
    cli::cli_inform(c(
      "i" = "Configured {nFilters} event filter{?s} for analysis"
    ))
  } else {
    nFilters <- 0
  }
  
  # Validate micro-costing settings
  if (microCosting) {
    if (is.null(primaryEventFilterName)) {
      cli::cli_abort(c(
        "Micro-costing requires a primary event filter",
        "x" = "primaryEventFilterName is NULL",
        "i" = "Specify which event filter identifies the line-level events to cost"
      ))
    }
    
    checkmate::assertString(primaryEventFilterName, min.chars = 1)
    
    if (!is.null(eventFilters)) {
      filterNames <- purrr::map_chr(eventFilters, "name")
      if (!primaryEventFilterName %in% filterNames) {
        cli::cli_abort(c(
          "Primary event filter not found",
          "x" = "'{primaryEventFilterName}' is not in the event filters",
          "i" = "Available filters: {.val {filterNames}}"
        ))
      }
    } else {
      cli::cli_abort(c(
        "Micro-costing requires event filters",
        "x" = "eventFilters is NULL but microCosting is TRUE",
        "i" = "Define at least one event filter for micro-costing analysis"
      ))
    }
  }
  
  # Validate additional cost concepts
  if (!is.null(additionalCostConceptIds)) {
    checkmate::assertIntegerish(
      additionalCostConceptIds,
      lower = 1,
      unique = TRUE,
      any.missing = FALSE
    )
  }
  
  # --- Create Settings Object ---
  settings <- structure(
    list(
      anchorCol = anchorCol,
      startOffsetDays = as.integer(startOffsetDays),
      endOffsetDays = as.integer(endOffsetDays),
      hasVisitRestriction = !is.null(restrictVisitConceptIds),
      restrictVisitConceptIds = as.integer(restrictVisitConceptIds),
      hasEventFilters = !is.null(eventFilters),
      eventFilters = eventFilters,
      nFilters = nFilters,
      microCosting = microCosting,
      primaryEventFilterName = primaryEventFilterName,
      costConceptId = as.integer(costConceptId),
      currencyConceptId = as.integer(currencyConceptId),
      additionalCostConceptIds = as.integer(additionalCostConceptIds)
    ),
    class = "CostOfCareSettings"
  )
  
  return(settings)
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
  purrr::walk(seq_along(eventFilters), function(i) {
    filter <- eventFilters[[i]]
    
    if (!is.list(filter)) {
      cli::cli_abort(c(
        "Invalid event filter structure",
        "x" = "Filter {i} is not a list",
        "i" = "Each filter must be a list with 'name', 'domain', and 'conceptIds'"
      ))
    }
    
    # Check required fields
    requiredFields <- c("name", "domain", "conceptIds")
    missingFields <- setdiff(requiredFields, names(filter))
    
    if (length(missingFields) > 0) {
      cli::cli_abort(c(
        "Missing required fields in event filter {i}",
        "x" = "Missing: {.field {missingFields}}",
        "i" = "Each filter must have: {.field {requiredFields}}"
      ))
    }
    
    # Validate name
    if (!is.character(filter$name) || length(filter$name) != 1 || nchar(filter$name) == 0) {
      cli::cli_abort(c(
        "Invalid filter name in event filter {i}",
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
        "Invalid domain in event filter {i}",
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

#' Print Cost of Care Settings
#' 
#' @param x A CostOfCareSettings object
#' @param ... Additional arguments (not used)
#' 
#' @return Invisibly returns the input object
#' @export
print.CostOfCareSettings <- function(x, ...) {
  cli::cli_h2("Cost of Care Settings")
  
  cli::cli_h3("Time Window")
  cli::cli_ul(c(
    "Anchor: {.field {x$anchorCol}}",
    "Start: {x$startOffsetDays} days",
    "End: {x$endOffsetDays} days",
    "Duration: {x$endOffsetDays - x$startOffsetDays} days"
  ))
  
  cli::cli_h3("Cost Configuration")
  cli::cli_ul(c(
    "Cost concept ID: {x$costConceptId}",
    "Currency concept ID: {x$currencyConceptId}"
  ))
  
  if (!is.null(x$additionalCostConceptIds)) {
    cli::cli_text("Additional cost concepts: {.val {x$additionalCostConceptIds}}")
  }
  
  if (x$hasVisitRestriction) {
    cli::cli_h3("Visit Restrictions")
    cli::cli_text("Restricted to {length(x$restrictVisitConceptIds)} visit concept{?s}")
  }
  
  if (x$hasEventFilters) {
    cli::cli_h3("Event Filters")
    purrr::walk(x$eventFilters, function(filter) {
      cli::cli_ul(
        "{.strong {filter$name}}: {filter$domain} domain with {length(filter$conceptIds)} concept{?s}"
      )
    })
  }
  
  if (x$microCosting) {
    cli::cli_h3("Micro-Costing")
    cli::cli_text("Primary filter: {.val {x$primaryEventFilterName}}")
  }
  
  invisible(x)
}