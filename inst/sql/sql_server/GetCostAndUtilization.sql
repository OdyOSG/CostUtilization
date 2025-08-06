/*
  Extracts person-level cost and utilization covariates for one or more cohorts.
  The output is one row per person-covariate combination, stored in a temporary table.
*/

-- Get all relevant cost event information in one place
WITH cost_events AS (
  SELECT
    c.person_id,
    c.cost,
    c.cost_domain_id,
    c.cost_type_concept_id,
    c.incurred_date,
    c.cost_event_id,
    -- Get concept_id from each domain
    CASE
      WHEN c.cost_domain_id = 'Drug' THEN de.drug_concept_id
      WHEN c.cost_domain_id = 'Procedure' THEN po.procedure_concept_id
      WHEN c.cost_domain_id = 'Visit' THEN vo.visit_concept_id
      WHEN c.cost_domain_id = 'Device' THEN d.device_concept_id
      WHEN c.cost_domain_id = 'Measurement' THEN m.measurement_concept_id
      WHEN c.cost_domain_id = 'Observation' THEN o.observation_concept_id
      ELSE 0
    END AS event_concept_id,
    -- Get Length of Stay for inpatient visits
    CASE
      WHEN c.cost_domain_id = 'Visit' AND vo.visit_concept_id IN (9201, 262)
      THEN DATEDIFF(DAY, vo.visit_start_date, vo.visit_end_date)
      ELSE NULL
    END AS length_of_stay
  FROM @cdm_database_schema.cost c
  LEFT JOIN @cdm_database_schema.drug_exposure de ON c.cost_event_id = de.drug_exposure_id AND c.cost_domain_id = 'Drug'
  LEFT JOIN @cdm_database_schema.procedure_occurrence po ON c.cost_event_id = po.procedure_occurrence_id AND c.cost_domain_id = 'Procedure'
  LEFT JOIN @cdm_database_schema.visit_occurrence vo ON c.cost_event_id = vo.visit_occurrence_id AND c.cost_domain_id = 'Visit'
  LEFT JOIN @cdm_database_schema.device_exposure d ON c.cost_event_id = d.device_exposure_id AND c.cost_domain_id = 'Device'
  LEFT JOIN @cdm_database_schema.measurement m ON c.cost_event_id = m.measurement_id AND c.cost_domain_id = 'Measurement'
  LEFT JOIN @cdm_database_schema.observation o ON c.cost_event_id = o.observation_id AND c.cost_domain_id = 'Observation'
  WHERE c.cost > 0
    AND c.currency_concept_id = @currency_concept_id
    {@cost_type_concept_ids != ''} ? {AND c.cost_type_concept_id IN (@cost_type_concept_ids)}
),
-- Link cost events to cohort members
cohort_cost_events AS (
  SELECT
    ch.subject_id,
    ch.cohort_definition_id,
    ch.cohort_start_date,
    ch.cohort_end_date,
    ce.*
  FROM @cohort_database_schema.@cohort_table ch
  INNER JOIN cost_events ce
    ON ch.subject_id = ce.person_id
  WHERE ch.cohort_definition_id IN (@cohort_ids)
),
-- Apply temporal windows
windowed_events AS (
  {@use_fixed_windows} ? {
  -- Fixed windows relative to cohort_start_date
  SELECT
    cce.subject_id,
    cce.cohort_definition_id,
    tw.window_id,
    cce.cost,
    cce.cost_domain_id,
    cce.length_of_stay,
    cce.cost_event_id
    {@use_cost_standardization} ? {, cpi.inflation_factor}
  FROM cohort_cost_events cce
  INNER JOIN #temporal_windows tw
    ON cce.incurred_date >= DATEADD(DAY, tw.start_day, cce.cohort_start_date)
    AND cce.incurred_date <= DATEADD(DAY, tw.end_day, cce.cohort_start_date)
  {@use_cost_standardization} ? {
  LEFT JOIN #cpi_data cpi
    ON YEAR(cce.incurred_date) = cpi.year
  }
  }
  
  {@use_fixed_windows & @use_in_cohort_window} ? {UNION ALL}

  {@use_in_cohort_window} ? {
  -- 'In Cohort' window (from cohort start to end date)
  SELECT
    cce.subject_id,
    cce.cohort_definition_id,
    999 AS window_id, -- Special ID for 'in cohort' window
    cce.cost,
    cce.cost_domain_id,
    cce.length_of_stay,
    cce.cost_event_id
    {@use_cost_standardization} ? {, cpi.inflation_factor}
  FROM cohort_cost_events cce
  {@use_cost_standardization} ? {
  LEFT JOIN #cpi_data cpi
    ON YEAR(cce.incurred_date) = cpi.year
  }
  WHERE cce.incurred_date >= cce.cohort_start_date AND cce.incurred_date <= cce.cohort_end_date
  }
)
-- Store person-level results in a final temp table
SELECT
  subject_id AS row_id,
  covariate_id,
  SUM(value) AS covariate_value
INTO @person_level_table
FROM (
  -- Analysis 1: Total Cost
  {@use_total_cost} ? {
  SELECT
    we.subject_id,
    10000 + we.window_id AS covariate_id,
    we.cost {@use_cost_standardization} ? {* ISNULL(we.inflation_factor, 1)} AS value
  FROM windowed_events we
  }

  -- Analysis 2: Cost By Domain
  {@use_domain_cost} ? {
  {@use_total_cost} ? {UNION ALL}
  SELECT
    we.subject_id,
    20000 + (we.window_id * 100) +
      CASE we.cost_domain_id
        WHEN 'Drug' THEN 1 WHEN 'Procedure' THEN 2 WHEN 'Visit' THEN 3
        WHEN 'Device' THEN 4 WHEN 'Measurement' THEN 5 WHEN 'Observation' THEN 6
        ELSE 0
      END AS covariate_id,
    we.cost {@use_cost_standardization} ? {* ISNULL(we.inflation_factor, 1)} AS value
  FROM windowed_events we
  WHERE we.cost_domain_id IN (@cost_domains)
  }

  -- Analysis 3: Utilization Count
  {@use_utilization} ? {
  {@use_total_cost | @use_domain_cost} ? {UNION ALL}
  SELECT
    we.subject_id,
    30000 + (we.window_id * 100) +
      CASE we.cost_domain_id
        WHEN 'Drug' THEN 1 WHEN 'Procedure' THEN 2 WHEN 'Visit' THEN 3
        WHEN 'Device' THEN 4
        ELSE 0
      END AS covariate_id,
    1 AS value -- count distinct events
  FROM windowed_events we
  WHERE we.cost_domain_id IN (@utilization_domains)
  GROUP BY we.subject_id, we.window_id, we.cost_domain_id, we.cost_event_id
  }

  -- Analysis 4: Length of Stay
  {@use_length_of_stay} ? {
  {@use_total_cost | @use_domain_cost | @use_utilization} ? {UNION ALL}
  SELECT
    we.subject_id,
    40000 + we.window_id AS covariate_id,
    we.length_of_stay AS value
  FROM windowed_events we
  WHERE we.length_of_stay IS NOT NULL
  }
) all_covariates
GROUP BY subject_id, covariate_id;