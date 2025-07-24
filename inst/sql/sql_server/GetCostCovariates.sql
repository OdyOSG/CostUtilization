/*
  GetCostCovariates.sql
  
  Extracts person-level cost and utilization covariates for a cohort.
  The output is one row per person-covariate combination.
*/

{DEFAULT @cdm_database_schema = 'cdm'}
{DEFAULT @cohort_database_schema = @cdm_database_schema}
{DEFAULT @cohort_table = 'cohort'}
{DEFAULT @cohort_id = 1}
{DEFAULT @row_id_field = 'subject_id'}
{DEFAULT @cost_type_concept_ids = ''}
{DEFAULT @cost_domain_ids = ''}
{DEFAULT @currency_concept_ids = ''}
{DEFAULT @use_total_cost = TRUE}
{DEFAULT @use_cost_by_domain = TRUE}
{DEFAULT @use_cost_by_type = FALSE}
{DEFAULT @use_utilization = TRUE}
{DEFAULT @has_included_concepts = FALSE}
{DEFAULT @has_excluded_concepts = FALSE}

WITH cost_events AS (
  SELECT
    c.person_id, 
    c.cost_domain_id, 
    c.cost_type_concept_id, 
    c.cost,
    CASE c.cost_domain_id
      WHEN 'Drug' THEN de.drug_exposure_start_date 
      WHEN 'Visit' THEN vo.visit_start_date
      WHEN 'Procedure' THEN po.procedure_date 
      WHEN 'Device' THEN dev.device_exposure_start_date
      WHEN 'Measurement' THEN m.measurement_date 
      WHEN 'Observation' THEN o.observation_date
      WHEN 'Specimen' THEN s.specimen_date 
      ELSE NULL
    END AS cost_date,
    CASE c.cost_domain_id
      WHEN 'Drug' THEN de.drug_concept_id 
      WHEN 'Visit' THEN vo.visit_concept_id
      WHEN 'Procedure' THEN po.procedure_concept_id 
      WHEN 'Device' THEN dev.device_concept_id
      WHEN 'Measurement' THEN m.measurement_concept_id 
      WHEN 'Observation' THEN o.observation_concept_id
      WHEN 'Specimen' THEN s.specimen_concept_id 
      ELSE 0
    END AS event_concept_id
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
    {@has_included_concepts} ? {AND event_concept_id IN (SELECT concept_id FROM included_concepts)}
    {@has_excluded_concepts} ? {AND event_concept_id NOT IN (SELECT concept_id FROM excluded_concepts)}
), 
cohort_cost_events AS (
  SELECT 
    cs.@row_id_field AS row_id, 
    cs.cohort_start_date, 
    ce.cost_domain_id, 
    ce.cost_type_concept_id, 
    ce.cost_date, 
    ce.cost
  FROM @cohort_database_schema.@cohort_table cs
  INNER JOIN cost_events ce ON cs.subject_id = ce.person_id
  WHERE cs.cohort_definition_id = @cohort_id
),
total_cost_covariates AS (
  SELECT 
    cce.row_id, 
    1000 + tw.window_id AS covariate_id, 
    SUM(cce.cost) AS covariate_value
  FROM cohort_cost_events cce 
  INNER JOIN temporal_windows tw 
    ON cce.cost_date >= DATEADD(DAY, tw.start_day, cce.cohort_start_date) 
    AND cce.cost_date <= DATEADD(DAY, tw.end_day, cce.cohort_start_date)
  GROUP BY cce.row_id, tw.window_id
),
domain_cost_covariates AS (
  SELECT 
    cce.row_id, 
    2000 + (tw.window_id * 10) + 
    CASE cce.cost_domain_id 
      WHEN 'Drug' THEN 1 
      WHEN 'Visit' THEN 2 
      WHEN 'Procedure' THEN 3 
      WHEN 'Device' THEN 4 
      WHEN 'Measurement' THEN 5 
      WHEN 'Observation' THEN 6 
      WHEN 'Specimen' THEN 7 
      ELSE 9 
    END AS covariate_id, 
    SUM(cce.cost) AS covariate_value
  FROM cohort_cost_events cce 
  INNER JOIN temporal_windows tw 
    ON cce.cost_date >= DATEADD(DAY, tw.start_day, cce.cohort_start_date) 
    AND cce.cost_date <= DATEADD(DAY, tw.end_day, cce.cohort_start_date)
  GROUP BY cce.row_id, tw.window_id, cce.cost_domain_id
),
type_cost_covariates AS (
  SELECT 
    cce.row_id, 
    3000 + (tw.window_id * 100) + (cce.cost_type_concept_id % 100) AS covariate_id, 
    SUM(cce.cost) AS covariate_value
  FROM cohort_cost_events cce 
  INNER JOIN temporal_windows tw 
    ON cce.cost_date >= DATEADD(DAY, tw.start_day, cce.cohort_start_date) 
    AND cce.cost_date <= DATEADD(DAY, tw.end_day, cce.cohort_start_date)
  GROUP BY cce.row_id, tw.window_id, cce.cost_type_concept_id
),
utilization_covariates AS (
  SELECT 
    cce.row_id, 
    4000 + tw.window_id AS covariate_id, 
    COUNT(DISTINCT cce.cost_date) AS covariate_value
  FROM cohort_cost_events cce 
  INNER JOIN temporal_windows tw 
    ON cce.cost_date >= DATEADD(DAY, tw.start_day, cce.cohort_start_date) 
    AND cce.cost_date <= DATEADD(DAY, tw.end_day, cce.cohort_start_date)
  GROUP BY cce.row_id, tw.window_id
)

SELECT row_id, covariate_id, covariate_value FROM total_cost_covariates
WHERE @use_total_cost = 1

{@use_cost_by_domain} ? {
UNION ALL
SELECT row_id, covariate_id, covariate_value FROM domain_cost_covariates
}

{@use_cost_by_type} ? {
UNION ALL
SELECT row_id, covariate_id, covariate_value FROM type_cost_covariates
}

{@use_utilization} ? {
UNION ALL
SELECT row_id, covariate_id, covariate_value FROM utilization_covariates
};