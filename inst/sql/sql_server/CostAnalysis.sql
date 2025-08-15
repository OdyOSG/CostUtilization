-- This script performs a cost and utilization analysis using temporary tables
-- for each intermediate step, which can improve performance and readability.

-- Preamble for concept set resolution (if used)
{@conceptSetPreamble}

-- Step 1: Create the target cohort temporary table
-- This table holds the specific cohorts to be analyzed.
DROP TABLE IF EXISTS #target_tmp_cohorts;
SELECT
  cohort_definition_id,
  subject_id,
  cohort_start_date,
  cohort_end_date
INTO #target_tmp_cohorts
FROM @cohort_database_schema.@cohort_table
{@cohortIds == -1} ? {WHERE cohort_definition_id IS NOT NULL} : {WHERE cohort_definition_id IN (@cohortIds)};

-- Step 2: Create the analysis windows temporary table
-- This table defines all the time periods (e.g., -365d to 0d) for each person.
DROP TABLE IF EXISTS #analysis_windows;
SELECT *
INTO #analysis_windows
FROM (
  -- Dynamic window generation based on settings
  {@timeWindows}

  {@useInCohortWindow} ? {
    UNION ALL
    SELECT
      cohort_definition_id,
      subject_id,
      CAST('In Cohort' AS VARCHAR(20)) AS window_name,
      CAST(cohort_start_date AS DATE)  AS window_start,
      CAST(cohort_end_date   AS DATE)  AS window_end
    FROM #target_tmp_cohorts
  }
) AS all_windows;

-- Step 3: Calculate costs that fall within each analysis window
-- This table links costs from the COST table to the defined windows.
DROP TABLE IF EXISTS #cohort_costs;
SELECT
  aw.cohort_definition_id,
  aw.window_name,
  aw.subject_id AS person_id,
  co.cost,
  co.cost_event_field_concept_id,
  DATEDIFF(day, aw.window_start, aw.window_end) + 1 AS person_days_in_window
INTO #cohort_costs
FROM #analysis_windows aw
JOIN @cdm_database_schema.cost co
  ON aw.subject_id = co.person_id
{@dynamic_join}  -- Optional join for concept set analysis
WHERE
  co.incurred_date >= aw.window_start
  AND co.incurred_date <= aw.window_end
  {@dynamic_where_clause}; -- Additional filters for currency, cost type, etc.

-- Step 4: Aggregate the costs for each cohort and window
-- This summarizes the raw costs into statistics like total, average, and counts.
DROP TABLE IF EXISTS #aggregated_costs;
SELECT
  cohort_definition_id,
  window_name,
  cost_event_field_concept_id AS cost_domain_id, -- Assuming this maps to domain
  COUNT(DISTINCT person_id) AS person_count,
  COUNT(*) AS event_count,
  SUM(cost) AS total_cost,
  AVG(cost) AS avg_cost,
  STDEV(cost) AS stddev_cost,
  MIN(cost) AS min_cost,
  MAX(cost) AS max_cost
INTO #aggregated_costs
FROM #cohort_costs
GROUP BY cohort_definition_id, window_name, cost_event_field_concept_id;

-- Step 5: Calculate the denominators (total persons and person-days) for each window
-- This is crucial for calculating per-patient-per-time metrics.
DROP TABLE IF EXISTS #window_denominators;
SELECT
  cohort_definition_id,
  window_name,
  COUNT(DISTINCT subject_id) AS total_persons,
  SUM(DATEDIFF(day, window_start, window_end) + 1) AS total_person_days
INTO #window_denominators
FROM #analysis_windows
GROUP BY cohort_definition_id, window_name;

-- Final Step: Join aggregated results with denominators and calculate final metrics
-- This select statement produces the final output of the analysis.
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
  -- Per-patient metrics calculated using the denominators
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
FROM #aggregated_costs ac
JOIN #window_denominators wd
  ON ac.cohort_definition_id = wd.cohort_definition_id
  AND ac.window_name = wd.window_name
ORDER BY ac.cohort_definition_id, ac.window_name, ac.cost_domain_id;

-- Clean up all temporary tables to free up resources
DROP TABLE IF EXISTS #target_tmp_cohorts;
DROP TABLE IF EXISTS #analysis_windows;
DROP TABLE IF EXISTS #cohort_costs;
DROP TABLE IF EXISTS #aggregated_costs;
DROP TABLE IF EXISTS #window_denominators;
-- Clean up concept set table if it was created
{@final_codeset != ''} ? {DROP TABLE IF EXISTS @final_codeset;}