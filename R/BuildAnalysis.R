




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

#' Build the Main SQL for a Cost and Utilization Analysis
#'
#' @description
#' This function generates a complete, unified SQL script for a cost and
#' utilization analysis. It dynamically adjusts the query based on the
#' provided settings to handle filtering by cost domains, a concept set, or
#' no specific filter.
#'
#' @param settings         A `costUtilizationSettings` object.
#' @param cohortIds        A numeric vector of cohort IDs to analyze.
#' @param eventPreambleSql An optional SQL script to be run before the main query.
#'                         Used for creating temporary tables like `#events_of_interest`
#'                         when a concept set is specified.
#'
#' @return A single character string containing the complete, executable SQL script.
#' @noRd
# buildAnalysisSql <- function(settings, cohortIds, eventPreambleSql = NULL) {
#   # --- 1. Build Reusable SQL Fragments ---
#   
#   # Window definitions using an explicit function for purrr::map_chr
# 
#   filtersCollection <- 
# 
#   glue::glue("
#     <eventPreambleSql>
# 
#     WITH
#       target_cohorts AS (
#         SELECT
#           cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
#         FROM @cohort_database_schema.@cohort_table
#         {@cohortIds == -1} ? {WHERE cohort_definition_id IS NOT NULL} : {WHERE cohort_definition_id IN (@cohortIds)}
#       ),
#       analysis_windows AS (
#         <windowDefinitions>
#       ),
#       cohort_costs AS (
#         SELECT
#           aw.cohort_definition_id,
#           aw.window_name,
#           aw.subject_id AS person_id,
#           co.cost,
#           co.cost_domain_id,
#           DATEDIFF(day, aw.window_start, aw.window_end) + 1 AS person_days_in_window
#         FROM analysis_windows aw
#         JOIN @cdm_database_schema.cost co ON aw.subject_id = co.person_id
#         <costJoinClause>
#         WHERE
#           co.incurred_date >= aw.window_start AND co.incurred_date <= aw.window_end
#           AND <costFilterClause>
#           <costWhereClause>
#       ),
#       aggregated_costs AS (
#         SELECT
#           cohort_definition_id, window_name, cost_domain_id,
#           COUNT(DISTINCT person_id) AS person_count,
#           COUNT(cost) AS event_count,
#           SUM(cost) AS total_cost
#         FROM cohort_costs
#         GROUP BY cohort_definition_id, window_name, cost_domain_id
#       ),
#       window_denominators AS (
#         SELECT
#           cohort_definition_id, window_name,
#           SUM(DISTINCT DATEDIFF(day, window_start, window_end) + 1) AS total_person_days
#         FROM analysis_windows
#         GROUP BY cohort_definition_id, window_name
#       ),
#       strata_scaffold AS (
#         SELECT
#           wd.cohort_definition_id, wd.window_name, d.domain_id, wd.total_person_days
#         FROM window_denominators wd
#         <strataSourceSql>
#       )
#     SELECT
#       s.cohort_definition_id, s.window_name, s.domain_id,
#       COALESCE(ac.person_count, 0) AS person_count,
#       COALESCE(ac.event_count, 0) AS event_count,
#       COALESCE(ac.total_cost, 0) AS total_cost,
#       s.total_person_days,
#       <aggCalcs>
#     FROM strata_scaffold s
#     LEFT JOIN aggregated_costs ac
#       ON s.cohort_definition_id = ac.cohort_definition_id
#       AND s.window_name = ac.window_name
#       AND s.domain_id = ac.cost_domain_id
#     ORDER BY s.cohort_definition_id, s.window_name, s.domain_id;
#   ", .open = "<", .close = ">")
# }