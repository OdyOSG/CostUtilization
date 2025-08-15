-- This script performs a cost and utilization analysis using temporary tables
-- for each intermediate step, which can improve performance and readability.

-- Preamble for concept set resolution final_codeset should be already created
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
/*
-- Join to visit_occurrence to get visit_concept_id for FAC_IP/FAC_OP mapping
LEFT JOIN @cdm_database_schema.visit_occurrence v
  ON co.visit_occurrence_id = v.visit_occurrence_id

{@useInflationAdjustment} ? {
-- Join to get the inflation factor for the year the cost was incurred
JOIN @inflationTable inf
  ON inf.STD_PRICE_YR = YEAR(co.incurred_date)
  AND inf.TOS1 = (CASE
      WHEN co.cost_domain_id = 'Drug' THEN 'PHARM'
      WHEN co.cost_domain_id = 'Visit' AND v.visit_concept_id = 9201 THEN 'FAC_IP'
      WHEN co.cost_domain_id = 'Visit' AND v.visit_concept_id = 9202 THEN 'FAC_OP'
      WHEN co.cost_domain_id = 'Procedure' THEN 'PROF'
      ELSE 'PROF'
    END)
-- Join to get the target year's inflation factor
JOIN #target_inflation ti
  ON ti.TOS1 = inf.TOS1
}
*/
WHERE
  co.incurred_date >= aw.window_start
  AND co.incurred_date <= aw.window_end
  {@dynamic_where_clause}; -- Additional filters for currency, cost type, etc.

