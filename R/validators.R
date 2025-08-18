#' Validators for CostUtilization Package
#'
#' @description
#' Internal validation functions for the CostUtilization package.
#' These functions ensure input parameters meet requirements before analysis execution.
#'
#' @keywords internal
#' Validate All Input Parameters
#'
#' @description
#' Comprehensive validation of all input parameters for calculateCostOfCare function
#'
#' @param params List of parameters to validate
#'
#' @return Invisible TRUE if all validations pass, otherwise throws error

validateInputs <- function(params) {
  # Validate schema names
  checkmate::assertCharacter(params$cdmDatabaseSchema, len = 1, min.chars = 1)
  checkmate::assertCharacter(params$cohortDatabaseSchema, len = 1, min.chars = 1)
  
  # Validate table names
  checkmate::assertCharacter(params$cohortTable, len = 1, min.chars = 1)
  
  # Validate cohort ID
  checkmate::assertIntegerish(params$cohortId, lower = 1, len = 1)
  
  # Validate anchor column
  checkmate::assertChoice(params$anchorCol, c("cohort_start_date", "cohort_end_date"))
  
  # Validate time windows
  checkmate::assertIntegerish(params$startOffsetDays, len = 1)
  checkmate::assertIntegerish(params$endOffsetDays, len = 1)
  
  # Validate that time window is valid (end must be after start)
  if (params$endOffsetDays <= params$startOffsetDays) {
    cli::cli_abort("endOffsetDays must be greater than startOffsetDays")
  }
  
  # Validate optional visit restrictions
  if (!is.null(params$restrictVisitConceptIds)) {
    checkmate::assertIntegerish(params$restrictVisitConceptIds, lower = 1, min.len = 1)
  }
  
  # Validate event filters
  if (!is.null(params$eventFilters)) {
    validateEventFilters(params$eventFilters)
  }
  
  # Validate micro-costing parameters
  checkmate::assertLogical(params$microCosting, len = 1)
  
  if (params$microCosting) {
    if (!is.null(params$primaryEventFilterName)) {
      checkmate::assertCharacter(params$primaryEventFilterName, len = 1, min.chars = 1)
      
      # Check that primaryEventFilterName exists in eventFilters
      if (!is.null(params$eventFilters)) {
        filterNames <- purrr::map_chr(params$eventFilters, "name")
        if (!params$primaryEventFilterName %in% filterNames) {
          cli::cli_abort(
            "primaryEventFilterName '{params$primaryEventFilterName}' not found in eventFilters"
          )
        }
      } else {
        cli::cli_abort("primaryEventFilterName specified but no eventFilters provided")
      }
    }
  }
  
  # Validate cost concept IDs
  checkmate::assertIntegerish(params$costConceptId, lower = 1, len = 1)
  checkmate::assertIntegerish(params$currencyConceptId, lower = 1, len = 1)
  
  # Validate temp emulation schema if provided
  if (!is.null(params$tempEmulationSchema)) {
    checkmate::assertCharacter(params$tempEmulationSchema, len = 1, min.chars = 1)
  }
  
  # Validate flags
  checkmate::assertLogical(params$asPermanent, len = 1)
  checkmate::assertLogical(params$verbose, len = 1)
  
  invisible(TRUE)
}


#' Validate Event Filters Structure
#'
#' @description
#' Validates the structure and content of event filters
#'
#' @param eventFilters List of event filters to validate
#'
#' @return Invisible TRUE if valid, otherwise throws error
#' @noRd
validateEventFilters <- function(eventFilters) {
  checkmate::assertList(eventFilters, min.len = 1)
  
  # Check for duplicate names
  filterNames <- purrr::map_chr(eventFilters, ~ .x$name)
  if (anyDuplicated(filterNames)) {
    cli::cli_abort("Event filter names must be unique")
  }
  
  # Validate each filter
  for (i in seq_along(eventFilters)) {
    filter <- eventFilters[[i]]
    
    # Check structure
    checkmate::assertList(filter, names = "named")
    checkmate::assertNames(
      names(filter), 
      must.include = c("name", "domain", "conceptIds")
    )
    
    # Validate name
    checkmate::assertCharacter(filter$name, len = 1, min.chars = 1)
    
    # Validate domain
    validDomains <- c("All", "Condition", "Procedure", "Drug", "Measurement", "Observation")
    checkmate::assertChoice(filter$domain, validDomains)
    
    # Validate concept IDs
    checkmate::assertIntegerish(filter$conceptIds, min.len = 1, lower = 1)
    
    # Check for duplicate concept IDs within filter
    if (anyDuplicated(filter$conceptIds)) {
      cli::cli_warn(
        "Event filter '{filter$name}' contains duplicate concept IDs"
      )
    }
  }
  
  invisible(TRUE)
}

#' Validate Cost Concept IDs
#'
#' @description
#' Validates that the specified cost and currency concept IDs exist in the vocabulary
#'
#' @param connection DatabaseConnector connection object
#' @param vocabDatabaseSchema Vocabulary schema name
#' @param costConceptId Cost type concept ID
#' @param currencyConceptId Currency concept ID
#'
#' @return Invisible TRUE if valid, otherwise throws error
#' @noRd
validateCostConceptIds <- function(connection, vocabDatabaseSchema, 
                                   costConceptId, currencyConceptId) {
  # Check cost concept ID
  sql <- SqlRender::render(
    "SELECT concept_id, concept_name, domain_id
     FROM @vocab_database_schema.concept
     WHERE concept_id IN (@cost_concept_id, @currency_concept_id)
       AND invalid_reason IS NULL",
    vocab_database_schema = vocabDatabaseSchema,
    cost_concept_id = costConceptId,
    currency_concept_id = currencyConceptId
  )
  
  result <- DatabaseConnector::querySql(connection, sql)
  
  if (nrow(result) == 0) {
    cli::cli_abort(
      "Cost concept ID {costConceptId} or currency concept ID {currencyConceptId} not found in vocabulary"
    )
  }
  
  # Check if cost concept is in correct domain
  costConcept <- result[result$CONCEPT_ID == costConceptId, ]
  if (nrow(costConcept) > 0 && !costConcept$DOMAIN_ID[1] %in% c("Type Concept", "Metadata")) {
    cli::cli_warn(
      "Cost concept ID {costConceptId} ('{costConcept$CONCEPT_NAME[1]}') is in domain '{costConcept$DOMAIN_ID[1]}', expected 'Type Concept'"
    )
  }
  invisible(TRUE)
}

#' Validate Time Window
#'
#' @description
#' Validates that the time window parameters create a valid analysis period
#'
#' @param startOffsetDays Start offset in days from anchor date
#' @param endOffsetDays End offset in days from anchor date
#' @param anchorCol Anchor column (cohort_start_date or cohort_end_date)
#'
#' @return Invisible TRUE if valid, otherwise throws error
#' @noRd
validateTimeWindow <- function(startOffsetDays, endOffsetDays, anchorCol) {
  # Check that end is after start
  if (endOffsetDays <= startOffsetDays) {
    cli::cli_abort(
      "Invalid time window: endOffsetDays ({endOffsetDays}) must be greater than startOffsetDays ({startOffsetDays})"
    )
  }
  # Check window size
  windowDays <- endOffsetDays - startOffsetDays
  if (windowDays > 3650) {  # More than 10 years
    cli::cli_warn(
      "Time window spans {windowDays} days (>10 years). This may impact performance."
    )
  }
  
  # Warn about negative offsets with cohort_end_date
  if (anchorCol == "cohort_end_date" && startOffsetDays < 0) {
    cli::cli_inform(
      "Using negative startOffsetDays ({startOffsetDays}) with cohort_end_date anchor will look forward in time from cohort end"
    )
  }
  
  invisible(TRUE)
}