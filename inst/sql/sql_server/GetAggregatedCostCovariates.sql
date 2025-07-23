/************************************************************************
@file GetCostCovariates.sql

Updated version to extract person-level cost and utilization covariates for a cohort.

- DEPENDENCY: This script requires a temporary table named #temporal_windows
  to be created and populated by the calling R function.
  The #temporal_windows table must have the columns: window_id, start_day, end_day.

- Rationale: Moving the creation of the temporal windows to R simplifies the SQL,
  improves cross-platform compatibility, and makes the system more robust.

************************************************************************/

{DEFAULT @cdm_database_schema = 'cdm'}
{DEFAULT @cohort_database_schema = @cdm_database_schema}
{DEFAULT @cohort_table = 'cohort'}
{DEFAULT @cohort_id = 1}
{DEFAULT @cost_type_concept_ids = ''}
{DEFAULT @cost_domain_ids = ''}
{DEFAULT @currency_concept_ids = ''}

-- Note: The #temporal_windows table is no longer created here.
-- It is expected to be created and populated by the calling R function.

-- Extract and join all relevant cost records with their corresponding event dates
WITH cost_records AS (
  SELECT
    c.person_id,
    c.cost_domain_id,
    c.cost_type_concept_id,
    c.cost,
    -- Determine the date of the clinical event associated with the cost
    CASE c.cost_domain_id
      WHEN 'Drug' THEN de.drug_exposure_start_date
      WHEN 'Visit' THEN vo.visit_start_date
      WHEN 'Procedure' THEN po.procedure_date
      WHEN 'Device' THEN dev.device_exposure_start_date
      WHEN 'Measurement' THEN m.measurement_date
      WHEN 'Observation' THEN o.observation_date
      WHEN 'Specimen' THEN s.specimen_date
      ELSE NULL
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
  WHERE c.cost IS NOT NULL AND c.cost > 0
    {@cost_type_concept_ids != ''} ? {AND c.cost_type_concept_id IN (@cost_type_concept_ids)}
    {@currency_concept_ids != ''} ? {AND c.currency_concept_id IN (@currency_concept_ids)}
    {@cost_domain_ids != ''} ? {AND c.cost_domain_id IN (@cost_domain_ids)}
),
-- Filter cost_records to only include those within the cohort
cohort_cost_records AS (
  SELECT
    cs.subject_id,
    cs.cohort_start_date,
    cr.cost_domain_id,
    cr.cost_type_concept_id,
    cr.cost_date,
    cr.cost
  FROM @cohort_database_schema.@cohort_table cs
  INNER JOIN cost_records cr
    ON cs.subject_id = cr.person_id
  WHERE cs.cohort_definition_id = @cohort_id
)
-- Analysis 1: Total costs per person per time window
SELECT
  @cohort_id AS cohort_definition_id,
  ccr.subject_id,
  1000 + (tw.window_id * 10) AS covariate_id,
  SUM(ccr.cost) AS covariate_value
FROM cohort_cost_records ccr
CROSS JOIN #temporal_windows tw
WHERE ccr.cost_date >= DATEADD(DAY, tw.start_day, ccr.cohort_start_date)
  AND ccr.cost_date <= DATEADD(DAY, tw.end_day, ccr.cohort_start_date)
GROUP BY ccr.subject_id, tw.window_id

UNION ALL

-- Analysis 2: Domain-specific costs per person per time window
SELECT
  @cohort_id AS cohort_definition_id,
  ccr.subject_id,
  2000 + (tw.window_id * 100) +
  CASE ccr.cost_domain_id
    WHEN 'Drug' THEN 1
    WHEN 'Visit' THEN 2
    WHEN 'Procedure' THEN 3
    WHEN 'Device' THEN 4
    WHEN 'Measurement' THEN 5
    WHEN 'Observation' THEN 6
    WHEN 'Specimen' THEN 7
    ELSE 99
  END AS covariate_id,
  SUM(ccr.cost) AS covariate_value
FROM cohort_cost_records ccr
CROSS JOIN #temporal_windows tw
WHERE ccr.cost_date >= DATEADD(DAY, tw.start_day, ccr.cohort_start_date)
  AND ccr.cost_date <= DATEADD(DAY, tw.end_day, ccr.cohort_start_date)
GROUP BY ccr.subject_id, tw.window_id, ccr.cost_domain_id

UNION ALL

-- Analysis 3: Cost type-specific costs per person per time window
SELECT
  @cohort_id AS cohort_definition_id,
  ccr.subject_id,
  3000 + (tw.window_id * 1000) + (ccr.cost_type_concept_id % 1000) AS covariate_id,
  SUM(ccr.cost) AS covariate_value
FROM cohort_cost_records ccr
CROSS JOIN #temporal_windows tw
WHERE ccr.cost_date >= DATEADD(DAY, tw.start_day, ccr.cohort_start_date)
  AND ccr.cost_date <= DATEADD(DAY, tw.end_day, ccr.cohort_start_date)
GROUP BY ccr.subject_id, tw.window_id, ccr.cost_type_concept_id

UNION ALL

-- Analysis 4: Utilization counts per person per time window
-- This counts the number of distinct days a person had any kind of healthcare interaction.
SELECT
  @cohort_id AS cohort_definition_id,
  ccr.subject_id,
  4000 + (tw.window_id * 10) AS covariate_id,
  COUNT(DISTINCT ccr.cost_date) AS covariate_value
FROM cohort_cost_records ccr
CROSS JOIN #temporal_windows tw
WHERE ccr.cost_date >= DATEADD(DAY, tw.start_day, ccr.cohort_start_date)
  AND ccr.cost_date <= DATEADD(DAY, tw.end_day, ccr.cohort_start_date)
GROUP BY ccr.subject_id, tw.window_id;

-- The #temporal_windows table is managed by DatabaseConnector in the calling R session,
-- so it does not need to be dropped here.