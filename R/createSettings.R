#' @title Create Settings Objects for Cost Analysis
#' @description
#' This file contains functions to create validated settings objects for the CostUtilization package.
#' The package uses a split settings approach where different aspects of the analysis are configured
#' through separate, focused settings objects.
#'
#' @details
#' The split settings approach provides:
#' - Better separation of concerns
#' - Easier validation and testing
#' - More flexible configuration
#' - Clearer documentation
#'
#' @name settings
NULL

#' Create Cost of Care Settings
#'
#' @description
#' Creates a validated settings object for the core cost-of-care analysis parameters.
#' This settings object controls the time window, visit restrictions, event filters,
#' and cost concept selections.
#'
#' @param anchorCol Column to use as anchor for time window ("cohort_start_date" or "cohort_end_date").
#' @param startOffsetDays Integer, days to add to anchor date for window start (can be negative).
#' @param endOffsetDays Integer, days to add to anchor date for window end.
#' @param restrictVisitConceptIds Optional vector of visit concept IDs to restrict analysis.
#' @param eventFilters Optional list of event filters. See details for structure.
#' @param microCosting Logical; if TRUE, performs line-level costing at the visit_detail level.
#' @param primaryEventFilterName For micro-costing, the name of the primary event filter.
#' @param costConceptId Concept ID for the cost type (e.g., total charge). Default: 31978.
#' @param currencyConceptId Concept ID for the currency. Default: 44818668 (USD).
#'
#' @details
#' The `eventFilters` argument must be a list of lists, where each inner list has:
#' \itemize{
#'   \item `name`: A unique character string for the filter.
#'   \item `domain`: The OMOP domain (e.g., "Drug", "Procedure").
#'   \item `conceptIds`: A vector of integer concept IDs.
#' }
#'
#' @return A validated `CostOfCareSettings` S3 object.
#' 
#' @examples
#' \dontrun{
#' # Basic settings
#' settings <- createCostOfCareSettings(
#'   anchorCol = "cohort_start_date",
#'   startOffsetDays = -365,
#'   endOffsetDays = 0
#' )
#' 
#' # Settings with event filters
#' diabetesFilters <- list(
#'   list(
#'     name = "Diabetes Diagnoses",
#'     domain = "Condition",
#'     conceptIds = c(201820, 201826)
#'   ),
#'   list(
#'     name = "Diabetes Medications",
#'     domain = "Drug",
#'     conceptIds = c(1503297, 1502826)
#'   )
#' )
#' 
#' settingsWithFilters <- createCostOfCareSettings(
#'   anchorCol = "cohort_start_date",
#'   startOffsetDays = -365,
#'   endOffsetDays = 365,
#'   eventFilters = diabetesFilters
#' )
#' }
#' 
#' @export
createCostOfCareSettings <- function(
    anchorCol = c("cohort_start_date", "cohort_end_date"),
    startOffsetDays = 0L,
    endOffsetDays = 365L,
    restrictVisitConceptIds = NULL,
    eventFilters = NULL,
    microCosting = FALSE,
    primaryEventFilterName = NULL,
    costConceptId = 31978L,
    currencyConceptId = 44818668L
) {
  # Input validation
  anchorCol <- rlang::arg_match(anchorCol)
  
  checkmate::assertIntegerish(startOffsetDays, len = 1, any.missing = FALSE)
  checkmate::assertIntegerish(endOffsetDays, len = 1, any.missing = FALSE)
  
  if (endOffsetDays <= startOffsetDays) {
    cli::cli_abort("{.arg endOffsetDays} must be greater than {.arg startOffsetDays}.")
  }
  
  checkmate::assertFlag(microCosting)
  
  if (!is.null(restrictVisitConceptIds)) {
    checkmate::assertIntegerish(
      restrictVisitConceptIds, 
      lower = 1, 
      min.len = 1, 
      unique = TRUE,
      any.missing = FALSE
    )
  }
  
  if (!is.null(eventFilters)) {
    validateEventFilters(eventFilters)
  }
  
  if (microCosting) {
    checkmate::assertString(primaryEventFilterName, null.ok = FALSE)
    if (is.null(eventFilters) || 
        !primaryEventFilterName %in% purrr::map_chr(eventFilters, "name")) {
      cli::cli_abort(
        "When {.code microCosting = TRUE}, {.arg primaryEventFilterName} must match a name in {.arg eventFilters}."
      )
    }
  }
  
  checkmate::assertInt(costConceptId, lower = 1)
  checkmate::assertInt(currencyConceptId, lower = 1)
  
  # Create settings object
  settings <- list(
    anchorCol = anchorCol,
    startOffsetDays = as.integer(startOffsetDays),
    endOffsetDays = as.integer(endOffsetDays),
    hasVisitRestriction = !is.null(restrictVisitConceptIds),
    restrictVisitConceptIds = restrictVisitConceptIds,
    hasEventFilters = !is.null(eventFilters),
    eventFilters = eventFilters,
    nFilters = if (is.null(eventFilters)) 0L else length(eventFilters),
    microCosting = microCosting,
    primaryEventFilterName = primaryEventFilterName,
    costConceptId = as.integer(costConceptId),
    currencyConceptId = as.integer(currencyConceptId)
  )
  
  class(settings) <- c("CostOfCareSettings", "list")
  return(settings)
}

#' Create CPI Adjustment Settings
#'
#' @description
#' Creates a validated settings object for Consumer Price Index (CPI) adjustment.
#' This allows costs to be standardized to a specific year to account for inflation.
#'
#' @param enabled Logical; whether to enable CPI adjustment.
#' @param targetYear Target year for standardization. If NULL, uses current year.
#' @param dataPath Path to custom CPI data CSV file. If NULL, uses package default.
#' @param cpiType Type of CPI to use: "medical" (default) or "all_items".
#'
#' @return A validated `CpiAdjustmentSettings` S3 object.
#' 
#' @examples
#' \dontrun{
#' # Use default medical CPI data
#' cpiSettings <- createCpiAdjustmentSettings(
#'   enabled = TRUE,
#'   targetYear = 2023
#' )
#' 
#' # Use custom CPI data
#' cpiSettingsCustom <- createCpiAdjustmentSettings(
#'   enabled = TRUE,
#'   targetYear = 2023,
#'   dataPath = "path/to/custom_cpi.csv"
#' )
#' }
#' 
#' @export
createCpiAdjustmentSettings <- function(
    enabled = FALSE,
    targetYear = NULL,
    dataPath = NULL,
    cpiType = c("medical", "all_items")
) {
  checkmate::assertFlag(enabled)
  
  if (enabled) {
    if (is.null(targetYear)) {
      targetYear <- as.integer(format(Sys.Date(), "%Y"))
    }
    checkmate::assertIntegerish(
      targetYear, 
      len = 1, 
      lower = 1900, 
      upper = 2100,
      any.missing = FALSE
    )
    
    if (!is.null(dataPath)) {
      checkmate::assertFileExists(dataPath)
    }
    
    cpiType <- rlang::arg_match(cpiType)
  }
  
  settings <- list(
    enabled = enabled,
    targetYear = targetYear,
    dataPath = dataPath,
    cpiType = cpiType
  )
  
  class(settings) <- c("CpiAdjustmentSettings", "list")
  return(settings)
}

#' Validate Event Filters
#'
#' @description
#' Internal function to validate the structure of event filters.
#'
#' @param eventFilters List of event filter specifications.
#'
#' @return NULL (throws error if invalid).
#' 
#' @keywords internal
validateEventFilters <- function(eventFilters) {
  checkmate::assertList(eventFilters, min.len = 1)
  
  filterNames <- character()
  
  for (i in seq_along(eventFilters)) {
    filter <- eventFilters[[i]]
    
    if (!is.list(filter)) {
      cli::cli_abort("Event filter {i} must be a list.")
    }
    
    # Check required fields
    requiredFields <- c("name", "domain", "conceptIds")
    if (!all(requiredFields %in% names(filter))) {
      cli::cli_abort(
        "Event filter {i} must have fields: {.field {requiredFields}}"
      )
    }
    
    # Validate name
    checkmate::assertString(filter$name, min.chars = 1)
    if (filter$name %in% filterNames) {
      cli::cli_abort("Duplicate event filter name: {.val {filter$name}}")
    }
    filterNames <- c(filterNames, filter$name)
    
    # Validate domain
    validDomains <- c(
      "Drug", "Condition", "Procedure", "Observation", 
      "Measurement", "Visit", "Device"
    )
    checkmate::assertChoice(filter$domain, validDomains)
    
    # Validate concept IDs
    checkmate::assertIntegerish(
      filter$conceptIds, 
      lower = 1, 
      min.len = 1,
      unique = TRUE,
      any.missing = FALSE
    )
  }
  
  invisible(NULL)
}

#' Print Cost of Care Settings
#'
#' @param x A `CostOfCareSettings` object.
#' @param ... Additional arguments (ignored).
#'
#' @return The input object invisibly.
#' 
#' @export
print.CostOfCareSettings <- function(x, ...) {
  cli::cli_h3("Cost of Care Settings")
  cli::cli_ul()
  cli::cli_li("Time window: {x$anchorCol} + [{x$startOffsetDays}, {x$endOffsetDays}] days")
  cli::cli_li("Cost concept: {x$costConceptId}")
  cli::cli_li("Currency concept: {x$currencyConceptId}")
  
  if (x$hasVisitRestriction) {
    cli::cli_li("Visit restriction: {length(x$restrictVisitConceptIds)} concept(s)")
  }
  
  if (x$hasEventFilters) {
    cli::cli_li("Event filters: {x$nFilters} filter(s)")
    for (filter in x$eventFilters) {
      cli::cli_li("{filter$name} ({filter$domain}): {length(filter$conceptIds)} concept(s)")
    }
  }
  
  if (x$microCosting) {
    cli::cli_li("Micro-costing enabled")
    cli::cli_li("Primary filter: {x$primaryEventFilterName}")
  }
  
  cli::cli_end()
  invisible(x)
}

#' Print CPI Adjustment Settings
#'
#' @param x A `CpiAdjustmentSettings` object.
#' @param ... Additional arguments (ignored).
#'
#' @return The input object invisibly.
#' 
#' @export
print.CpiAdjustmentSettings <- function(x, ...) {
  cli::cli_h3("CPI Adjustment Settings")
  cli::cli_ul()
  cli::cli_li("Enabled: {x$enabled}")
  
  if (x$enabled) {
    cli::cli_li("Target year: {x$targetYear}")
    cli::cli_li("CPI type: {x$cpiType}")
    if (!is.null(x$dataPath)) {
      cli::cli_li("Custom CPI data: {x$dataPath}")
    } else {
      cli::cli_li("Using default {x$cpiType} CPI data")
    }
  }
  
  cli::cli_end()
  invisible(x)
}

#' Validate Cost of Care Settings
#'
#' @description
#' Validates a CostOfCareSettings object to ensure it's properly formed.
#'
#' @param settings A `CostOfCareSettings` object.
#'
#' @return The validated settings object.
#' 
#' @export
validateCostOfCareSettings <- function(settings) {
  checkmate::assertClass(settings, "CostOfCareSettings")
  
  # Re-validate critical fields
  requiredFields <- c(
    "anchorCol", "startOffsetDays", "endOffsetDays",
    "hasVisitRestriction", "hasEventFilters", "microCosting",
    "costConceptId", "currencyConceptId"
  )
  
  checkmate::assertNames(
    names(settings), 
    must.include = requiredFields
  )
  
  return(settings)
}

#' Validate CPI Adjustment Settings
#'
#' @description
#' Validates a CpiAdjustmentSettings object to ensure it's properly formed.
#'
#' @param settings A `CpiAdjustmentSettings` object.
#'
#' @return The validated settings object.
#' 
#' @export
validateCpiAdjustmentSettings <- function(settings) {
  checkmate::assertClass(settings, "CpiAdjustmentSettings")
  
  requiredFields <- c("enabled", "targetYear", "dataPath", "cpiType")
  checkmate::assertNames(
    names(settings), 
    must.include = requiredFields
  )
  
  return(settings)
}