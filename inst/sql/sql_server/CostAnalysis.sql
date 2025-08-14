-- CostUtilization Package: Core Analysis Script
-- OHDSI SQL Dialect for SqlRender
-- OMOP CDM v5.5

-- This script is designed to be run in a sequence of steps managed by the R wrapper.
-- It avoids CTEs by using temporary tables (#table_name) for modularity.

--------------------------------------------------------------------------------
-- Step 1: Create time-anchored cohort windows
-- This table defines the analysis period for each person in the cohort.
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #cohort_windows;

SELECT
  c.cohort_definition_id,
  c.subject_id,
  c.cohort_start_date,
  c.cohort_end_date,
  CAST(
    DATEADD(
      day,
      @start_window,
      {@start_anchor == 'cohortStart'} ? {c.cohort_start_date} : {c.cohort_end_date}
    ) AS DATE
  ) AS window_start,
  CAST(
    DATEADD(
      day,
      @end_window,
      {@end_anchor == 'cohortStart'} ? {c.cohort_start_date} : {c.cohort_end_date}
    ) AS DATE
  ) AS window_end
INTO #cohort_windows
FROM @cohort_database_schema.@cohort_table c
WHERE c.cohort_definition_id IN (@cohort_ids);


--------------------------------------------------------------------------------
-- Step 2: (Optional) Create cost standardization multiplier table
-- This table is created only if standardization is requested.
--------------------------------------------------------------------------------
{@standardize} ? {
DROP TABLE IF EXISTS #cost_multipliers;

-- The R wrapper will generate the INSERT statements for this table
-- based on the standardizationMultiplierData data frame.
CREATE TABLE #cost_multipliers (
  cost_domain_id VARCHAR(20),
  cost_year INT,
  multiplier FLOAT
);

-- Example of what R will generate:
-- INSERT INTO #cost_multipliers (cost_domain_id, cost_year, multiplier) VALUES ('Drug', 2020, 1.05);
-- INSERT INTO #cost_multipliers (cost_domain_id, cost_year, multiplier) VALUES ('Drug', 2021, 1.02);
-- ...
{@multiplier_insert_sql}

}


--------------------------------------------------------------------------------
-- Step 3: Extract and filter costs within the defined windows
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #filtered_costs;

SELECT
  cw.cohort_definition_id,
  cw.subject_id,
  c.cost_domain_id,
  YEAR(c.incurred_date) AS cost_year,
  c.cost
INTO #filtered_costs
FROM @cdm_database_schema.cost c
INNER JOIN #cohort_windows cw
  ON c.person_id = cw.subject_id
WHERE
  c.incurred_date >= cw.window_start AND c.incurred_date <= cw.window_end
  AND c.cost IS NOT NULL AND c.cost > 0
  {@cost_domain_ids != ''} ? {AND c.cost_domain_id IN (@cost_domain_ids)}
  {@cost_type_concept_ids != ''} ? {AND c.cost_type_concept_id IN (@cost_type_concept_ids)};


--------------------------------------------------------------------------------
-- Step 4: (Optional) Standardize costs to reference year
-- If standardization is enabled, join with multipliers and adjust cost.
-- Otherwise, just pass the costs through.
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #standardized_costs;

SELECT
  fc.cohort_definition_id,
  fc.subject_id,
  fc.cost_domain_id,
  {@standardize} ? {
    CAST(fc.cost * ISNULL(cm.multiplier, 1.0) AS FLOAT) AS standardized_cost
  } : {
    CAST(fc.cost AS FLOAT) AS standardized_cost
  }
INTO #standardized_costs
FROM #filtered_costs fc
{@standardize} ? {
LEFT JOIN #cost_multipliers cm
  ON fc.cost_domain_id = cm.cost_domain_id AND fc.cost_year = cm.cost_year
};


--------------------------------------------------------------------------------
-- Step 5: Aggregate costs per person and calculate total cost
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #person_cost_summary;

SELECT
  cohort_definition_id,
  subject_id,
  'Total' AS cost_category,
  SUM(standardized_cost) AS total_cost
INTO #person_cost_summary
FROM #standardized_costs
GROUP BY
  cohort_definition_id,
  subject_id;

-- Append domain-specific costs
INSERT INTO #person_cost_summary
SELECT
  cohort_definition_id,
  subject_id,
  cost_domain_id AS cost_category,
  SUM(standardized_cost) AS total_cost
FROM #standardized_costs
GROUP BY
  cohort_definition_id,
  subject_id,
  cost_domain_id;


--------------------------------------------------------------------------------
-- Step 6: (Optional) Calculate HRU Metrics
--------------------------------------------------------------------------------
{@calculate_hru} ? {
-- Inpatient Visit Counts
DROP TABLE IF EXISTS #hru_inpatient;

SELECT
  cw.cohort_definition_id,
  cw.subject_id,
  COUNT(DISTINCT vo.visit_occurrence_id) AS inpatient_visit_count,
  SUM(DATEDIFF(day, vo.visit_start_date, vo.visit_end_date)) AS length_of_stay
INTO #hru_inpatient
FROM @cdm_database_schema.visit_occurrence vo
INNER JOIN #cohort_windows cw
  ON vo.person_id = cw.subject_id
WHERE
  vo.visit_start_date >= cw.window_start AND vo.visit_start_date <= cw.window_end
  AND vo.visit_concept_id IN (9201, 262) -- Inpatient Visit, Inpatient Hospital
GROUP BY
  cw.cohort_definition_id,
  cw.subject_id;

-- Outpatient Visit Counts
DROP TABLE IF EXISTS #hru_outpatient;

SELECT
  cw.cohort_definition_id,
  cw.subject_id,
  COUNT(DISTINCT vo.visit_occurrence_id) AS outpatient_visit_count
INTO #hru_outpatient
FROM @cdm_database_schema.visit_occurrence vo
INNER JOIN #cohort_windows cw
  ON vo.person_id = cw.subject_id
WHERE
  vo.visit_start_date >= cw.window_start AND vo.visit_start_date <= cw.window_end
  AND vo.visit_concept_id IN (9202) -- Outpatient Visit
GROUP BY
  cw.cohort_definition_id,
  cw.subject_id;
}


--------------------------------------------------------------------------------
-- Step 7: Final aggregation and calculation of descriptive statistics
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #final_summary;

SELECT
  s.cohort_definition_id,
  s.cost_category,
  'cost' AS analysis_type,
  COUNT(DISTINCT s.subject_id) AS n_persons,
  AVG(s.total_cost) AS mean,
  STDEV(s.total_cost) AS std_dev,
  MIN(s.total_cost) AS min,
  MAX(s.total_cost) AS max,
  -- Using PERCENTILE_CONT as it's standard in SQL Server, Oracle, PG, Redshift
  -- SqlRender will translate this appropriately for other platforms.
  MIN(CASE WHEN p.rn = p.q1_rn THEN s.total_cost ELSE NULL END) as p1,
  MIN(CASE WHEN p.rn = p.q25_rn THEN s.total_cost ELSE NULL END) as p25,
  MIN(CASE WHEN p.rn = p.q50_rn THEN s.total_cost ELSE NULL END) as p50,
  MIN(CASE WHEN p.rn = p.q75_rn THEN s.total_cost ELSE NULL END) as p75,
  MIN(CASE WHEN p.rn = p.q90_rn THEN s.total_cost ELSE NULL END) as p90
INTO #final_summary
FROM #person_cost_summary s
-- Subquery to calculate percentile ranks without CTEs
INNER JOIN (
  SELECT
    cohort_definition_id,
    cost_category,
    -- Calculate the row number for specific percentiles
    CAST(COUNT(*) * 0.01 AS INT) AS q1_rn,
    CAST(COUNT(*) * 0.25 AS INT) AS q25_rn,
    CAST(COUNT(*) * 0.50 AS INT) AS q50_rn,
    CAST(COUNT(*) * 0.75 AS INT) AS q75_rn,
    CAST(COUNT(*) * 0.90 AS INT) AS q90_rn,
    ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, cost_category ORDER BY total_cost) as rn
  FROM #person_cost_summary
) p ON s.cohort_definition_id = p.cohort_definition_id AND s.cost_category = p.cost_category
GROUP BY
  s.cohort_definition_id,
  s.cost_category;


{@calculate_hru} ? {
-- Append HRU results if calculated
-- Inpatient Visits
INSERT INTO #final_summary
SELECT
  h.cohort_definition_id,
  'inpatientVisitCount' AS cost_category,
  'hru' AS analysis_type,
  COUNT(DISTINCT h.subject_id) AS n_persons,
  AVG(CAST(h.inpatient_visit_count AS FLOAT)) AS mean,
  STDEV(h.inpatient_visit_count) AS std_dev,
  MIN(h.inpatient_visit_count) AS min,
  MAX(h.inpatient_visit_count) AS max,
  NULL, NULL, NULL, NULL, NULL -- Percentiles not calculated for counts for simplicity
FROM #hru_inpatient h
GROUP BY h.cohort_definition_id;

-- Length of Stay
INSERT INTO #final_summary
SELECT
  h.cohort_definition_id,
  'lengthOfStay' AS cost_category,
  'hru' AS analysis_type,
  COUNT(DISTINCT h.subject_id) AS n_persons,
  AVG(CAST(h.length_of_stay AS FLOAT)) AS mean,
  STDEV(h.length_of_stay) AS std_dev,
  MIN(h.length_of_stay) AS min,
  MAX(h.length_of_stay) AS max,
  NULL, NULL, NULL, NULL, NULL
FROM #hru_inpatient h
WHERE h.length_of_stay IS NOT NULL
GROUP BY h.cohort_definition_id;

-- Outpatient Visits
INSERT INTO #final_summary
SELECT
  h.cohort_definition_id,
  'outpatientVisitCount' AS cost_category,
  'hru' AS analysis_type,
  COUNT(DISTINCT h.subject_id) AS n_persons,
  AVG(CAST(h.outpatient_visit_count AS FLOAT)) AS mean,
  STDEV(h.outpatient_visit_count) AS std_dev,
  MIN(h.outpatient_visit_count) AS min,
  MAX(h.outpatient_visit_count) AS max,
  NULL, NULL, NULL, NULL, NULL
FROM #hru_outpatient h
GROUP BY h.cohort_definition_id;
}

-- Final select from the summary table
-- This is what will be returned to R
SELECT * FROM #final_summary;

--------------------------------------------------------------------------------
-- Cleanup of temporary tables
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #cohort_windows;
DROP TABLE IF EXISTS #cost_multipliers;
DROP TABLE IF EXISTS #filtered_costs;
DROP TABLE IF EXISTS #standardized_costs;
DROP TABLE IF EXISTS #person_cost_summary;
DROP TABLE IF EXISTS #hru_inpatient;
DROP TABLE IF EXISTS #hru_outpatient;
DROP TABLE IF EXISTS #final_summary;