-- Extracts person-level cost covariates for a cohort

{DEFAULT @cdm_database_schema = 'main'}
{DEFAULT @cohort_database_schema = @cdm_database_schema}
{DEFAULT @cohort_table = 'cohort_table'}
{DEFAULT @cohort_id = 1}
{DEFAULT @temporal_start_days = '-365,-180,-30,0'}
{DEFAULT @temporal_end_days = '-1,-1,-1,0'}
{DEFAULT @cost_type_concept_ids = '44818668,44818669'}
{DEFAULT @cost_domain_ids = NULL}
{DEFAULT @currency_concept_ids = '44818568'}

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

-- Extract person-level cost data
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
  @cohort_id AS cohort_definition_id,
  cs.subject_id,
  1000 + (tw.window_id * 10) AS covariate_id,
  SUM(cr.cost) AS covariate_value
FROM cohort_subjects cs
INNER JOIN cost_records cr ON cs.subject_id = cr.person_id
CROSS JOIN #temporal_windows tw
WHERE cr.cost_date >= DATEADD(DAY, tw.start_day, cs.cohort_start_date)
  AND cr.cost_date <= DATEADD(DAY, tw.end_day, cs.cohort_start_date)
GROUP BY cs.subject_id, tw.window_id

UNION ALL

-- Domain-specific costs
SELECT 
  @cohort_id AS cohort_definition_id,
  cs.subject_id,
  2000 + (tw.window_id * 100) + 
  CASE cr.cost_domain_id
    WHEN 'Drug' THEN 1
    WHEN 'Visit' THEN 2
    WHEN 'Procedure' THEN 3
    WHEN 'Device' THEN 4
    WHEN 'Measurement' THEN 5
    WHEN 'Observation' THEN 6
    WHEN 'Specimen' THEN 7
    ELSE 99
  END AS covariate_id,
  SUM(cr.cost) AS covariate_value
FROM cohort_subjects cs
INNER JOIN cost_records cr ON cs.subject_id = cr.person_id
CROSS JOIN #temporal_windows tw
WHERE cr.cost_date >= DATEADD(DAY, tw.start_day, cs.cohort_start_date)
  AND cr.cost_date <= DATEADD(DAY, tw.end_day, cs.cohort_start_date)
GROUP BY cs.subject_id, tw.window_id, cr.cost_domain_id

UNION ALL

-- Cost type-specific costs
SELECT 
  @cohort_id AS cohort_definition_id,
  cs.subject_id,
  3000 + (tw.window_id * 1000) + (cr.cost_type_concept_id % 1000) AS covariate_id,
  SUM(cr.cost) AS covariate_value
FROM cohort_subjects cs
INNER JOIN cost_records cr ON cs.subject_id = cr.person_id
CROSS JOIN #temporal_windows tw
WHERE cr.cost_date >= DATEADD(DAY, tw.start_day, cs.cohort_start_date)
  AND cr.cost_date <= DATEADD(DAY, tw.end_day, cs.cohort_start_date)
GROUP BY cs.subject_id, tw.window_id, cr.cost_type_concept_id

UNION ALL

-- Utilization counts
SELECT 
  @cohort_id AS cohort_definition_id,
  cs.subject_id,
  4000 + (tw.window_id * 10) AS covariate_id,
  COUNT(DISTINCT CONCAT(cr.cost_domain_id, '_', CAST(cr.cost_date AS VARCHAR))) AS covariate_value
FROM cohort_subjects cs
INNER JOIN cost_records cr ON cs.subject_id = cr.person_id
CROSS JOIN #temporal_windows tw
WHERE cr.cost_date >= DATEADD(DAY, tw.start_day, cs.cohort_start_date)
  AND cr.cost_date <= DATEADD(DAY, tw.end_day, cs.cohort_start_date)
GROUP BY cs.subject_id, tw.window_id;

-- Clean up temp tables
DROP TABLE IF EXISTS #temporal_windows;