#' Generate Dynamic SQL for Cost Analysis
#'
#' @param settings Cost utilization settings object
#' @param cohortIds Vector of cohort IDs to analyze
#' @return Character string containing the complete SQL
generateCostAnalysisSql <- function(settings, cohortIds = -1) {
  
  # Generate window definitions
  windowSql <- generateWindowsSql(settings$timeWindows)
  
  # Generate filters based on scenario
  filters <- generateFilters(settings)
  
  # Generate concept set preamble if needed
  conceptSetPreamble <- ""
  if (!is.null(settings$conceptSetDefinition)) {
    conceptSetPreamble <- .buildConceptSetSql(settings$conceptSetDefinition)
  }
  
  # Build the complete SQL
  sql <- SqlRender::render(
    readSql("inst/sql/sql_server/CostAnalysisDynamic.sql"),
    conceptSetPreamble = conceptSetPreamble,
    timeWindows = windowSql,
    useInCohortWindow = settings$useInCohortWindow,
    cohortIds = cohortIds,
    currencyFilter = filters$currency,
    costTypeFilter = filters$costType,
    domainFilter = filters$domain,
    conceptSetFilter = filters$conceptSet,
    conceptSetJoin = filters$conceptSetJoin
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
generateFilters <- function(settings) {
  filters <- list(
    currency = "",
    costType = "",
    domain = "",
    conceptSet = "",
    conceptSetJoin = ""
  )
  
  # Currency filter
  if (!is.null(settings$currencyConceptId)) {
    filters$currency <- glue::glue(
      "AND co.currency_concept_id = {settings$currencyConceptId}"
    )
  }
  
  # Cost type filter
  if (!is.null(settings$costTypeConceptIds)) {
    filters$costType <- glue::glue(
      "AND co.cost_type_concept_id IN ({paste(settings$costTypeConceptIds, collapse = ',')})"
    )
  }
  
  # Domain filter (Scenario 2)
  if (!is.null(settings$costDomains) && is.null(settings$conceptSetDefinition)) {
    domainConceptIds <- getDomainConceptIds(settings$costDomains)
    filters$domain <- glue::glue(
      "AND co.cost_event_field_concept_id IN ({paste(domainConceptIds, collapse = ',')})"
    )
  }
  
  # Concept set filter (Scenario 3)
  if (!is.null(settings$conceptSetDefinition)) {
    filters$conceptSetJoin <- "
      JOIN #final_codesets cs 
        ON co.cost_event_id = cs.event_id 
        AND co.cost_domain_id = cs.domain_id"
    filters$conceptSet <- "AND 1=1" # Filtering is done via join
  }
  
  return(filters)
}

#' Get domain concept IDs from domain names
getDomainConceptIds <- function(domains) {
  metadata <- cdmMetadata()
  metadata %>%
    filter(tolower(domain_id) %in% tolower(domains)) %>%
    pull(domain_concept_id)
}