-- Dynamic SQL Template for Cost Analysis
-- This template handles all three scenarios with parameterization

{@conceptSetPreamble}  -- Only populated for scenario 3

WITH target_cohorts AS (
  SELECT
    cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date
  FROM @cohort_database_schema.@cohort_table
  {@cohortIds == -1} ? {WHERE cohort_definition_id IS NOT NULL} : {WHERE cohort_definition_id IN (@cohortIds)}
),

analysis_windows AS (
  -- Dynamic window generation based on settings
  {@timeWindows}
  
  {@useInCohortWindow} ? {
    UNION ALL
    SELECT 
      cohort_definition_id,
      subject_id,
      'In Cohort' as window_name,
      cohort_start_date as window_start,
      cohort_end_date as window_end
    FROM target_cohorts
  }
),

cohort_costs AS (
  SELECT
    aw.cohort_definition_id,
    aw.window_name,
    aw.subject_id AS person_id,
    co.cost,
    co.cost_domain_id,
    co.cost_event_field_concept_id,
    DATEDIFF(day, aw.window_start, aw.window_end) + 1 AS person_days_in_window
  FROM analysis_windows aw
  JOIN @cdm_database_schema.cost co 
    ON aw.subject_id = co.person_id
  {@conceptSetJoin}  -- Additional join for scenario 3
  WHERE
    co.incurred_date >= aw.window_start 
    AND co.incurred_date <= aw.window_end
    {@currencyFilter}
    {@costTypeFilter}
    {@domainFilter}     -- For scenario 2
    {@conceptSetFilter} -- For scenario 3
),

aggregated_costs AS (
  SELECT
    cohort_definition_id,
    window_name,
    cost_domain_id,
    COUNT(DISTINCT person_id) AS person_count,
    COUNT(*) AS event_count,
    SUM(cost) AS total_cost,
    AVG(cost) AS avg_cost,
    STDDEV(cost) AS stddev_cost,
    MIN(cost) AS min_cost,
    MAX(cost) AS max_cost
  FROM cohort_costs
  GROUP BY cohort_definition_id, window_name, cost_domain_id
),

window_denominators AS (
  SELECT
    cohort_definition_id,
    window_name,
    COUNT(DISTINCT subject_id) AS total_persons,
    SUM(DATEDIFF(day, window_start, window_end) + 1) AS total_person_days
  FROM analysis_windows
  GROUP BY cohort_definition_id, window_name
)

SELECT
  ac.cohort_definition_id,
  ac.window_name,
  ac.cost_domain_id,
  ac.person_count,
  ac.event_count,
  ac.total_cost,
  ac.avg_cost,
  ac.stddev_cost,
  ac.min_cost,
  ac.max_cost,
  wd.total_persons,
  wd.total_person_days,
  -- Per-patient metrics
  CASE 
    WHEN wd.total_person_days > 0 
    THEN ac.total_cost / wd.total_person_days 
    ELSE 0 
  END AS cost_per_person_day,
  CASE 
    WHEN wd.total_person_days > 0 
    THEN (ac.total_cost / wd.total_person_days) * 30.44 
    ELSE 0 
  END AS cost_pppm,
  CASE 
    WHEN wd.total_person_days > 0 
    THEN (ac.total_cost / wd.total_person_days) * 365.25 
    ELSE 0 
  END AS cost_pppy
FROM aggregated_costs ac
JOIN window_denominators wd
  ON ac.cohort_definition_id = wd.cohort_definition_id
  AND ac.window_name = wd.window_name
ORDER BY ac.cohort_definition_id, ac.window_name, ac.cost_domain_id;

