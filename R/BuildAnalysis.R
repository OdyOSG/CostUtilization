#' Generate Dynamic SQL for Cost Analysis
#'
#' @param settings Cost utilization settings object
#' @param cohortIds Vector of cohort IDs to analyze
#' @return Character string containing the complete SQL
generateCostAnalysisSql <- function(settings, cohortIds = -1, .route) {
  domainConceptIds <- NULL
  finalCodesetTable <- purrr::pluck(.route, "finalCodesetTable", .default = "")
  conceptSetPreamble <- purrr::pluck(.route, "sql", .default = "")
  if (is.numeric(.route)) s <- .route
  # Generate window definitions
  windowSql <- generateWindowsSql(settings)
  
  # Generate filters based on the new function
  filters <- generateFilters(settings, domainConceptIds)
  
  # Build the complete SQL
  sql <- SqlRender::loadRenderTranslateSql(
    "CostAnalysis.sql",
    packageName = 'CostUtilization',
    timeWindows = windowSql,
    useInCohortWindow = settings$useInCohortWindow,
    conceptSetPreamble = conceptSetPreamble,
    cohortIds = cohortIds,
    # Pass the new, consolidated filter strings
    dynamic_join = filters$joins,
    dynamic_where_clause = filters$where_clause,
    final_codeset = finalCodesetTable
  )
  return(sql)
}

#' Generate SQL for time windows
#' @param settings A costUtilizationSettings object.
#' @return A SQL string for creating a temporary table of time windows.
generateWindowsSql <- function(settings) {
  # Use the existing tibble-based function to get window definitions
  windowTibble <- generateWindowTibble(settings)
  
  # If there are no windows, return an empty string
  if (nrow(windowTibble) == 0) {
    return("")
  }
  
  # Dynamically construct a SQL VALUES clause from the tibble
  # This is generally more efficient than multiple UNION ALL statements
  valuesClause <- purrr::pmap_chr(windowTibble, function(start, end, window_id) {
    # Handle the 'in-cohort' case where start/end are NA
    if (is.na(start)) {
      # In-cohort window logic remains separate
      return(NULL)
    }
    glue::glue("({window_id}, {start}, {end})")
  }) |>
    purrr::compact() |>
    paste(collapse = ",\n  ")
  
  # Build the final SQL for a temporary table of fixed windows
  fixedWindowSql <- if (nchar(valuesClause) > 0) {
    glue::glue("
      SELECT
        cohort_definition_id,
        subject_id,
        window_id,
        DATEADD(day, start_day, cohort_start_date) as window_start,
        DATEADD(day, end_day, cohort_start_date) as window_end
      FROM #target_tmp_cohorts
      CROSS JOIN (
        VALUES
          {valuesClause}
      ) AS windows(window_id, start_day, end_day)
    ")
  } else {
    ""
  }
  
  # Handle the in-cohort window separately, if specified
  inCohortSql <- if (settings$useInCohortWindow) {
    "
    SELECT
      cohort_definition_id,
      subject_id,
      999 AS window_id, -- Using a distinct ID for the in-cohort window
      cohort_start_date AS window_start,
      cohort_end_date AS window_end
    FROM #target_tmp_cohorts
    "
  } else {
    ""
  }
  
  # Combine the fixed and in-cohort SQL with UNION ALL if both exist
  finalSql <- c(fixedWindowSql, inCohortSql) |>
    purrr::compact() |>
    paste(collapse = "\nUNION ALL\n")
  
  return(finalSql)
}


#' Generate filter conditions based on settings
generateFilters <- function(settings, domainConceptIds) {
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
    # Cost concept ID filter
    if (!is.null(settings$costConceptId)) {
      glue::glue("co.cost_concept_id = {settings$costConceptId}")
    },
    # Domain filter (only when no concept set is used)
    if (!is.null(settings$costDomains) && is.null(settings$conceptSetDefinition)) {
      glue::glue("co.cost_event_field_concept_id IN ({paste(domainConceptIds, collapse = ',')})")
    },
    # Visit concept ID filter
    if (!is.null(settings$restrictVisitConceptIds)) {
      glue::glue("vo.visit_concept_id IN ({paste(settings$restrictVisitConceptIds, collapse = ',')})")
    }
  )
  
  # Use purrr to remove NULLs and combine the conditions with AND
  where_clause <- where_conditions |>
    purrr::compact() |>
    purrr::map_chr(~ paste0("    ", .x)) |>
    paste(collapse = "\nAND ")
  
  # Handle the concept set join and other necessary joins
  joins <- list()
  if (!is.null(settings$conceptSetDefinition)) {
    joins$conceptSet <- "
    JOIN #events_of_interest eoi
      ON co.cost_event_id = eoi.event_id
      AND co.cost_domain_id = eoi.domain_id"
  }
  # Add a join to visit_occurrence if filtering by visit type
  if (!is.null(settings$restrictVisitConceptIds)) {
    joins$visit <- "
    JOIN @cdm_database_schema.visit_occurrence vo
      ON co.visit_occurrence_id = vo.visit_occurrence_id"
  }
  
  # Return a list with the join and where clauses
  list(
    joins = paste(joins, collapse = "\n"),
    where_clause = if (nchar(where_clause) > 0) paste0("AND ", where_clause) else ""
  )
}