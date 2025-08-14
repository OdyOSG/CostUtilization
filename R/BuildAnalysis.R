# Copyright 2025 OHDSI
#
# This file is part of CostUtilization
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Build SQL for a Domain-Based Cost and Utilization Analysis
#'
#' @description
#' This function generates a complete SQL script for a cost and utilization
#' analysis where the events of interest are defined by a list of OMOP domains
#' (e.g., 'Drug', 'Visit'). It does not use concept sets.
#'
#' @param settings A `costUtilizationSettings` object.
#'
#' @return A single character string containing the complete, executable SQL script.
#' @noRd
buildDomainBasedSql <- function(settings) {
  # --- 1. Build Reusable SQL Fragments ---
  
  # Window definitions (fixed time windows and/or in-cohort window)
  windowDefinitions <- if (!is.null(settings$timeWindows)) {
    purrr::map_chr(settings$timeWindows, ~ glue::glue(
      "SELECT cohort_definition_id, subject_id,
       'Fixed: {.x[1]}d to {.x[2]}d' as window_name,
       DATEADD(day, {.x[1]}, cohort_start_date) as window_start,
       DATEADD(day, {.x[2]}, cohort_start_date) as window_end
       FROM target_cohorts"
    )) |> paste(collapse = "\nUNION ALL\n")
  } else {""}
  
  if (settings$useInCohortWindow) {
    inCohortWindowSql <- "
    SELECT cohort_definition_id, subject_id,
           'In-Cohort' as window_name,
           cohort_start_date as window_start,
           cohort_end_date as window_end
    FROM target_cohorts"
    windowDefinitions <- if (nchar(windowDefinitions) > 0) {
      paste(windowDefinitions, "UNION ALL", inCohortWindowSql)
    } else {
      inCohortWindowSql
    }
  }
  
  # Filters for the cost table
  costFilters <- c(
    glue::glue("co.currency_concept_id = {settings$currencyConceptId}"),
    "co.cost IS NOT NULL",
    "co.cost > 0"
  )
  if (!is.null(settings$costTypeConceptIds)) {
    costFilters <- c(costFilters, glue::glue("co.cost_type_concept_id IN ({paste(settings$costTypeConceptIds, collapse=',')})"))
  }
  costFilterClause <- paste(costFilters, collapse = " AND ")
  
  # Filter for cost_domain_id
  domainFilterClause <- if (!is.null(settings$costDomains)) {
    domainValues <- paste0("'", settings$costDomains, "'", collapse = ", ")
    glue::glue("AND co.cost_domain_id IN ({domainValues})")
  } else {
    "" # No domain filter means all domains will be included
  }
  
  # Final aggregation metrics (PPPM, PPPY, etc.)
  aggCalcs <- settings$aggregate |>
    purrr::map_chr(~ {
      multiplier <- switch(.x,
                           "pppd" = 1, "pppm" = 30.4375, "pppq" = 91.3125, "pppy" = 365.25, 1
      )
      glue::glue("COALESCE(ac.total_cost, 0) / NULLIF(s.total_person_days, 0) * {multiplier} AS cost_{.x},
                  COALESCE(ac.event_count, 0) / NULLIF(s.total_person_days, 0) * {multiplier} AS util_{.x}")
    }) |>
    paste(collapse = ",\n")
  
  # --- 2. Assemble the Full Query using CTEs ---
  glue::glue("
    WITH
      target_cohorts AS (
        SELECT
          cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
        FROM @cohort_database_schema.@cohort_table
        {@cohort_ids == -1} ? {WHERE cohort_definition_id IS NOT NULL} : {WHERE cohort_definition_id IN (@cohort_ids)}
      ),
      analysis_windows AS (
        {windowDefinitions}
      ),
      cohort_costs AS (
        SELECT
          aw.cohort_definition_id,
          aw.window_name,
          aw.subject_id AS person_id,
          co.cost,
          co.cost_domain_id,
          DATEDIFF(day, aw.window_start, aw.window_end) + 1 AS person_days_in_window
        FROM analysis_windows aw
        JOIN @cdm_database_schema.cost co ON aw.subject_id = co.person_id
        WHERE
          co.incurred_date >= aw.window_start AND co.incurred_date <= aw.window_end
          AND {costFilterClause}
          {domainFilterClause}
      ),
      aggregated_costs AS (
        SELECT
          cohort_definition_id, window_name, cost_domain_id,
          COUNT(DISTINCT person_id) AS person_count,
          COUNT(cost) AS event_count,
          SUM(cost) AS total_cost
        FROM cohort_costs
        GROUP BY cohort_definition_id, window_name, cost_domain_id
      ),
      window_denominators AS (
        SELECT
          cohort_definition_id, window_name,
          SUM(DISTINCT DATEDIFF(day, window_start, window_end) + 1) AS total_person_days
        FROM analysis_windows
        GROUP BY cohort_definition_id, window_name
      ),
      strata_scaffold AS (
        SELECT
          wd.cohort_definition_id, wd.window_name, d.domain_id, wd.total_person_days
        FROM window_denominators wd
        CROSS JOIN (
            SELECT DISTINCT cost_domain_id AS domain_id FROM @cdm_database_schema.cost
            {@is.null(settings$costDomains)} ? {} : {WHERE cost_domain_id IN ({paste0(\"'\", settings$costDomains, \"'\", collapse = ', ')})}
        ) d
      )
    SELECT
      s.cohort_definition_id, s.window_name, s.domain_id,
      COALESCE(ac.person_count, 0) AS person_count,
      COALESCE(ac.event_count, 0) AS event_count,
      COALESCE(ac.total_cost, 0) AS total_cost,
      s.total_person_days,
      {aggCalcs}
    FROM strata_scaffold s
    LEFT JOIN aggregated_costs ac
      ON s.cohort_definition_id = ac.cohort_definition_id
      AND s.window_name = ac.window_name
      AND s.domain_id = ac.cost_domain_id
    ORDER BY s.cohort_definition_id, s.window_name, s.domain_id;
  ")
}