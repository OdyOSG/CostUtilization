-- Extracts aggregated cost covariates for a cohort

{DEFAULT @cdm_database_schema = 'cdm'}
{DEFAULT @cohort_database_schema = @cdm_database_schema}
{DEFAULT @cohort_table = 'cohort'}
{DEFAULT @cohort_id = 1}
{DEFAULT @temporal_start_days = '-365,-180,-30,0'}
{DEFAULT @temporal_end_days = '-1,-1,-1,0'}
{DEFAULT @cost_type_concept_ids = '44818668,44818669'}
{DEFAULT @cost_domain_ids = NULL}
{DEFAULT @currency_concept_ids = '44818568'}
{DEFAULT @aggregate_method = 'sum'}

-- Create temp table with temporal windows
DROP TABLE IF EXISTS #temporal_windows;

SELECT row_number() OVER (ORDER BY start_day) AS window_id,
       start_day,
       end_day
INTO #temporal_windows
FROM (
  SELECT CAST(value AS INT) AS start_day,
         CAST(LEAD(value) OVER (ORDER BY ordinal) AS INT) AS end_day
  FROM STRING_SPLIT('@temporal_start_days', ',') s1
  CROSS APPLY (
    SELECT value, row_number() OVER (ORDER BY (SELECT NULL)) AS ordinal
    FROM STRING_SPLIT('@temporal_end_days', ',')
  ) s2
  WHERE s1.ordinal = s2.ordinal
) t;

-- Extract cost data with temporal windows
DROP TABLE IF EXISTS #cost_data;

WITH cohort_subjects AS (
  SELECT DISTINCT 
    subject_id,
    cohort_start_date,
    cohort_end_date
  FROM @cohort_database_schema.@cohort_table
  WHERE cohort_definition_id = @cohort_id
),
cost_records AS (
  SELECT 
    c.cost_id,
    c.person_id,
    c.cost_event_id,
    c.cost_domain_id,
    c.cost_type_concept_id,
    c.currency_concept_id,
    c.cost,
    c.cost_source_value,
    c.cost_source_concept_id,
    CASE 
      WHEN c.cost_domain_id = 'Drug' THEN de.drug_exposure_start_date
      WHEN c.cost_domain_id = 'Visit' THEN vo.visit_start_date
      WHEN c.cost_domain_id = 'Procedure' THEN po.procedure_date
      WHEN c.cost_domain_id = 'Device' THEN dev.device_exposure_start_date
      WHEN c.cost_domain_id = 'Measurement' THEN m.measurement_date
      WHEN c.cost_domain_id = 'Observation' THEN o.observation_date
      WHEN c.cost_domain_id = 'Specimen' THEN s.specimen_date
    END AS cost_date
  FROM @cdm_database_schema.cost c
  LEFT JOIN @cdm_database_schema.drug_exposure de 
    ON c.cost_event_id = de.drug_exposure_id AND c.cost_domain_id = 'Drug'
  LEFT JOIN @cdm_database_schema.visit_occurrence vo 
    ON c.cost_event_id = vo.visit_occurrence_id AND c.cost_domain_id = 'Visit'
  LEFT JOIN @cdm_database_schema.procedure_occurrence po 
    ON c.cost_event_id = po.procedure_occurrence_id AND c.cost_domain_id = 'Procedure'
  LEFT JOIN @cdm_database_schema.device_exposure dev 
    ON c.cost_event_id = dev.device_exposure_id AND c.cost_domain_id = 'Device'
  LEFT JOIN @cdm_database_schema.measurement m 
    ON c.cost_event_id = m.measurement_id AND c.cost_domain_id = 'Measurement'
  LEFT JOIN @cdm_database_schema.observation o 
    ON c.cost_event_id = o.observation_id AND c.cost_domain_id = 'Observation'
  LEFT JOIN @cdm_database_schema.specimen s 
    ON c.cost_event_id = s.specimen_id AND c.cost_domain_id = 'Specimen'
  WHERE c.cost IS NOT NULL
    AND c.cost > 0
    {@cost_type_concept_ids != ''} ? {AND c.cost_type_concept_id IN (@cost_type_concept_ids)}
    {@currency_concept_ids != ''} ? {AND c.currency_concept_id IN (@currency_concept_ids)}
    {@cost_domain_ids != ''} ? {AND c.cost_domain_id IN (@cost_domain_ids)}
)
SELECT 
  cs.subject_id,
  cs.cohort_start_date,
  tw.window_id,
  tw.start_day,
  tw.end_day,
  cr.cost_domain_id,
  cr.cost_type_concept_id,
  cr.cost
INTO #cost_data
FROM cohort_subjects cs
INNER JOIN cost_records cr ON cs.subject_id = cr.person_id
CROSS JOIN #temporal_windows tw
WHERE cr.cost_date >= DATEADD(DAY, tw.start_day, cs.cohort_start_date)
  AND cr.cost_date <= DATEADD(DAY, tw.end_day, cs.cohort_start_date);

-- Generate aggregated covariates
WITH cost_aggregates AS (
  SELECT 
    1000 + (window_id * 10) AS covariate_id,
    COUNT(DISTINCT subject_id) AS count_value,
    COUNT(subject_id) AS count_records,
    {@aggregate_method == 'sum'} ? {SUM(cost)} : {
      {@aggregate_method == 'mean'} ? {AVG(cost)} : {
        {@aggregate_method == 'median'} ? {PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cost)} : {
          {@aggregate_method == 'min'} ?
                    {@aggregate_method == 'min'} ? {MIN(cost)} : {
            {@aggregate_method == 'max'} ? {MAX(cost)} : {SUM(cost)}
          }
        }
      }
    } AS mean_value,
    STDDEV(cost) AS sd_value,
    MIN(cost) AS min_value,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cost) AS p25_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cost) AS median_value,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cost) AS p75_value,
    MAX(cost) AS max_value,
    window_id,
    start_day,
    end_day
  FROM #cost_data
  GROUP BY window_id, start_day, end_day
),
domain_aggregates AS (
  SELECT 
    2000 + (window_id * 100) + 
    CASE cost_domain_id
      WHEN 'Drug' THEN 1
      WHEN 'Visit' THEN 2
      WHEN 'Procedure' THEN 3
      WHEN 'Device' THEN 4
      WHEN 'Measurement' THEN 5
      WHEN 'Observation' THEN 6
      WHEN 'Specimen' THEN 7
      ELSE 99
    END AS covariate_id,
    COUNT(DISTINCT subject_id) AS count_value,
    COUNT(subject_id) AS count_records,
    {@aggregate_method == 'sum'} ? {SUM(cost)} : {
      {@aggregate_method == 'mean'} ? {AVG(cost)} : {
        {@aggregate_method == 'median'} ? {PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cost)} : {
          {@aggregate_method == 'min'} ? {MIN(cost)} : {
            {@aggregate_method == 'max'} ? {MAX(cost)} : {SUM(cost)}
          }
        }
      }
    } AS mean_value,
    STDDEV(cost) AS sd_value,
    MIN(cost) AS min_value,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cost) AS p25_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cost) AS median_value,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cost) AS p75_value,
    MAX(cost) AS max_value,
    window_id,
    start_day,
    end_day,
    cost_domain_id
  FROM #cost_data
  GROUP BY window_id, start_day, end_day, cost_domain_id
),
cost_type_aggregates AS (
  SELECT 
    3000 + (window_id * 1000) + (cost_type_concept_id % 1000) AS covariate_id,
    COUNT(DISTINCT subject_id) AS count_value,
    COUNT(subject_id) AS count_records,
    {@aggregate_method == 'sum'} ? {SUM(cost)} : {
      {@aggregate_method == 'mean'} ? {AVG(cost)} : {
        {@aggregate_method == 'median'} ? {PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cost)} : {
          {@aggregate_method == 'min'} ? {MIN(cost)} : {
            {@aggregate_method == 'max'} ? {MAX(cost)} : {SUM(cost)}
          }
        }
      }
    } AS mean_value,
    STDDEV(cost) AS sd_value,
    MIN(cost) AS min_value,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cost) AS p25_value,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cost) AS median_value,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cost) AS p75_value,
    MAX(cost) AS max_value,
    window_id,
    start_day,
    end_day,
    cost_type_concept_id
  FROM #cost_data
  GROUP BY window_id, start_day, end_day, cost_type_concept_id
),
utilization_aggregates AS (
  SELECT 
    4000 + (window_id * 10) AS covariate_id,
    COUNT(DISTINCT subject_id) AS count_value,
    COUNT(DISTINCT CONCAT(subject_id, '_', cost_domain_id, '_', CAST(cost_date AS VARCHAR))) AS count_records,
    COUNT(DISTINCT CASE WHEN cost_domain_id = 'Visit' THEN CONCAT(subject_id, '_', CAST(cost_date AS VARCHAR)) END) AS visit_count,
    COUNT(DISTINCT CASE WHEN cost_domain_id = 'Drug' THEN CONCAT(subject_id, '_', CAST(cost_date AS VARCHAR)) END) AS drug_count,
    COUNT(DISTINCT CASE WHEN cost_domain_id = 'Procedure' THEN CONCAT(subject_id, '_', CAST(cost_date AS VARCHAR)) END) AS procedure_count,
    window_id,
    start_day,
    end_day
  FROM #cost_data
  GROUP BY window_id, start_day, end_day
)
-- Combine all aggregates
SELECT 
  @cohort_id AS cohort_definition_id,
  covariate_id,
  count_value,
  CAST(mean_value AS FLOAT) AS mean_value,
  CAST(sd_value AS FLOAT) AS sd_value,
  CAST(min_value AS FLOAT) AS min_value,
  CAST(p25_value AS FLOAT) AS p25_value,
  CAST(median_value AS FLOAT) AS median_value,
  CAST(p75_value AS FLOAT) AS p75_value,
  CAST(max_value AS FLOAT) AS max_value
FROM (
  SELECT * FROM cost_aggregates
  UNION ALL
  SELECT 
    covariate_id,
    count_value,
    mean_value,
    sd_value,
    min_value,
    p25_value,
    median_value,
    p75_value,
    max_value
  FROM domain_aggregates
  UNION ALL
  SELECT 
    covariate_id,
    count_value,
    mean_value,
    sd_value,
    min_value,
    p25_value,
    median_value,
    p75_value,
    max_value
  FROM cost_type_aggregates
  UNION ALL
  SELECT 
    covariate_id,
    count_value,
    CAST(visit_count AS FLOAT) AS mean_value,
    0 AS sd_value,
    CAST(visit_count AS FLOAT) AS min_value,
    CAST(visit_count AS FLOAT) AS p25_value,
    CAST(visit_count AS FLOAT) AS median_value,
    CAST(visit_count AS FLOAT) AS p75_value,
    CAST(visit_count AS FLOAT) AS max_value
  FROM utilization_aggregates
) all_covariates
WHERE count_value > 0;

-- Clean up temp tables
DROP TABLE IF EXISTS #temporal_windows;
DROP TABLE IF EXISTS #cost_data;