#' Generate Dynamic SQL for Cost Analysis
#'
#' @param settings Cost utilization settings object
#' @param cohortIds Vector of cohort IDs to analyze
#' @return Character string containing the complete SQL
generateCostAnalysisSql <- function(settings, cohortIds = -1, .route) {
  domainConceptIds <- NULL
  if (purrr::is_null(.route) | !is.list(.route)) {
    conceptSetPreamble <- ""
    finalCodesetTable <- ""
    if (is.numeric(.route)) domainConceptIds <- .route  
  } else if (is.list(.route)) {
    finalCodesetTable <- purrr::pluck(.route, "finalCodesetTable")
    conceptSetPreamble <- purrr::pluck(.route, "sql")
  } 
  # Generate window definitions
  windowSql <- generateWindowsSql(settings$timeWindows)
  
  # Generate filters based on the new function
  filters <- generateFilters(settings)
  
  # Build the complete SQL
  sql <- SqlRender::loadRenderTranslateSql(
    "CostAnalysis.sql",
    packageName = 'CostUtilization',
    timeWindows = windowSql,
    useInCohortWindow = settings$useInCohortWindow,
    conceptSetPreamble = conceptSetPreamble,
    cohortIds = cohortIds,
    # Pass the new, consolidated filter strings
    dynamic_join = filters$conceptSetJoin,
    dynamic_where_clause = filters$where_clause,
    final_codeset = finalCodesetTable
    )
  return(sql)
}

#' Generate SQL for time windows
generateWindowsSql <- function(timeWindows) {
  if (is.null(timeWindows) || length(timeWindows) == 0) {
    return("")
  }
  windowQueries <- purrr::map_chr(timeWindows, function(window) {
    glue::glue("
      SELECT 
        cohort_definition_id,
        subject_id,
        'Fixed: {window[1]}d to {window[2]}d' as window_name,
        DATEADD(day, {window[1]}, cohort_start_date) as window_start,
        DATEADD(day, {window[2]}, cohort_start_date) as window_end
      FROM target_cohorts
    ")
  })
  paste(windowQueries, collapse = "\nUNION ALL\n")
}

#' Generate filter conditions based on settings
generateFilters <- function(settings, domainConceptIds) {
  # Helper function to get domain concept IDs from cdmMetadata
  # This should ideally be in a helper file.
  # Create a list of all potential WHERE clause conditions
  where_conditions <- list(
    # Currency filter
    if (!is.null(settings$currencyConceptId)) {
      glue::glue("co.currency_concept_id = {settings$currencyConceptId}")
    },
    # Cost type filter
    if (!is.null(settings$costTypeConceptIds)) {
      glue::glue("co.cost_type_concept_id IN ({paste(settings$costTypeConceptIds, collapse = ',')})")
    },
    # Domain filter (only when no concept set is used)
    if (!is.null(settings$costDomains) && is.null(settings$conceptSetDefinition)) {
      glue::glue("co.cost_event_field_concept_id IN ({paste(domainConceptIds, collapse = ',')})")
    }
  )
  
  # Use purrr to remove NULLs and combine the conditions with AND
  where_clause <- where_conditions |>
    purrr::compact() |>
    purrr::map_chr(~ paste0("    ", .x)) |>
    paste(collapse = "\nAND ")
  
  # Handle the concept set join separately
  conceptSetJoin <- if (!is.null(settings$conceptSetDefinition)) { "
    JOIN #events_of_interest eoi
      ON co.cost_event_id = eoi.event_id
      AND co.cost_domain_id = eoi.domain_id"
     } else ""  
  # Return a list with the join and where clauses
  list(
    conceptSetJoin = conceptSetJoin,
    where_clause = if (nchar(where_clause) > 0) paste0("AND ", where_clause) else ""
  )
}
