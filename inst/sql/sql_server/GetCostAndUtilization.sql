/*
  Person-level cost & utilization covariates with #temp tables.
*/

------------------------------------------------------------
-- 0) Safety: drop existing temps
------------------------------------------------------------
IF OBJECT_ID('tempdb..#cost_events') IS NOT NULL DROP TABLE #cost_events;
IF OBJECT_ID('tempdb..#cohort_cost_events') IS NOT NULL DROP TABLE #cohort_cost_events;
IF OBJECT_ID('tempdb..#windowed_events') IS NOT NULL DROP TABLE #windowed_events;

------------------------------------------------------------
-- 1) Build #cost_events (visit-centric)
------------------------------------------------------------
SELECT
    c.person_id,
    c.cost,
    c.cost_domain_id,
    c.cost_type_concept_id,
    c.incurred_date,
    c.cost_event_id,

    -- Event concept id for concept-set joins
    CASE
        WHEN c.cost_domain_id = 'Drug'        THEN de.drug_concept_id
        WHEN c.cost_domain_id = 'Procedure'   THEN po.procedure_concept_id
        WHEN c.cost_domain_id = 'Visit'       THEN vo.visit_concept_id
        WHEN c.cost_domain_id = 'Device'      THEN dx.device_concept_id
        WHEN c.cost_domain_id = 'Measurement' THEN m.measurement_concept_id
        WHEN c.cost_domain_id = 'Observation' THEN o.observation_concept_id
        ELSE 0
    END AS event_concept_id,

    -- Inclusive LoS for specific visit types
    CASE
        WHEN c.cost_domain_id = 'Visit' AND vo.visit_concept_id IN (9201, 262)
        THEN DATEDIFF(DAY, vo.visit_start_date, vo.visit_end_date) + 1
        ELSE NULL
    END AS length_of_stay

    {@use_cost_standardization} ? {, YEAR(c.incurred_date) AS cost_year}

INTO #cost_events
FROM @cdm_database_schema.cost c

LEFT JOIN @cdm_database_schema.drug_exposure        de ON c.cost_event_id = de.drug_exposure_id        AND c.cost_domain_id = 'Drug'
LEFT JOIN @cdm_database_schema.procedure_occurrence po ON c.cost_event_id = po.procedure_occurrence_id  AND c.cost_domain_id = 'Procedure'
LEFT JOIN @cdm_database_schema.visit_occurrence     vo ON c.cost_event_id = vo.visit_occurrence_id      AND c.cost_domain_id = 'Visit'
LEFT JOIN @cdm_database_schema.device_exposure      dx ON c.cost_event_id = dx.device_exposure_id       AND c.cost_domain_id = 'Device'
LEFT JOIN @cdm_database_schema.measurement          m  ON c.cost_event_id = m.measurement_id            AND c.cost_domain_id = 'Measurement'
LEFT JOIN @cdm_database_schema.observation          o  ON c.cost_event_id = o.observation_id            AND c.cost_domain_id = 'Observation'

-- Require a parent visit and enforce incurred_date within visit window
INNER JOIN @cdm_database_schema.visit_occurrence v
        ON v.visit_occurrence_id =
           CASE
             WHEN c.cost_domain_id = 'Visit' THEN vo.visit_occurrence_id
             ELSE COALESCE(de.visit_occurrence_id,
                           po.visit_occurrence_id,
                           dx.visit_occurrence_id,
                           m.visit_occurrence_id,
                           o.visit_occurrence_id)
           END
       AND c.incurred_date >= v.visit_start_date
       AND c.incurred_date <= v.visit_end_date

WHERE c.currency_concept_id = @currency_concept_id
  AND c.cost IS NOT NULL
  {@cost_type_concept_ids != ''} ? {AND c.cost_type_concept_id IN (@cost_type_concept_ids)};

------------------------------------------------------------
-- 2) Link to cohort (per person, per cohort)
------------------------------------------------------------
SELECT
    ch.subject_id,
    ch.cohort_definition_id,
    ch.cohort_start_date,
    ch.cohort_end_date,

    ce.person_id,
    ce.cost,
    ce.cost_domain_id,
    ce.cost_type_concept_id,
    ce.incurred_date,
    ce.cost_event_id,
    ce.event_concept_id,
    ce.length_of_stay
    {@use_cost_standardization} ? {, ce.cost_year}
INTO #cohort_cost_events
FROM @cohort_database_schema.@cohort_table ch
INNER JOIN #cost_events ce
        ON ch.subject_id = ce.person_id
WHERE ch.cohort_definition_id IN (@cohort_ids);

-----------------------------------------------------------
-- 3) Apply temporal windows (fixed windows and/or in-cohort window)
--    Pre-create #windowed_events so we can INSERT from multiple branches.
------------------------------------------------------------
CREATE TABLE #windowed_events (
  subject_id BIGINT NOT NULL,
  cohort_definition_id INT NOT NULL,
  window_id INT NOT NULL,
  cost DECIMAL(38,8) NULL,
  cost_domain_id VARCHAR(50) NULL,
  length_of_stay INT NULL,
  cost_event_id BIGINT NULL,
  event_concept_id BIGINT NULL
  {@use_cost_standardization} ? {, inflation_factor DECIMAL(18,8) NULL}
);

{@use_fixed_windows} ? {
INSERT INTO #windowed_events (
  subject_id, cohort_definition_id, window_id,
  cost, cost_domain_id, length_of_stay, cost_event_id, event_concept_id
  {@use_cost_standardization} ? {, inflation_factor}
)
SELECT
  cce.subject_id,
  cce.cohort_definition_id,
  tw.window_id,
  cce.cost,
  cce.cost_domain_id,
  cce.length_of_stay,
  cce.cost_event_id,
  cce.event_concept_id
  {@use_cost_standardization} ? {, cpi.inflation_factor}
FROM #cohort_cost_events cce
INNER JOIN #temporal_windows tw
  ON cce.incurred_date >= DATEADD(DAY, tw.start_day, cce.cohort_start_date)
 AND cce.incurred_date <= DATEADD(DAY, tw.end_day,   cce.cohort_start_date)
{@use_cost_standardization} ? {
LEFT JOIN #cpi_data cpi
  ON cce.cost_year = cpi.year }
;}

{@use_in_cohort_window} ? {
INSERT INTO #windowed_events (
  subject_id, cohort_definition_id, window_id,
  cost, cost_domain_id, length_of_stay, cost_event_id, event_concept_id
  {@use_cost_standardization} ? {, inflation_factor}
)
SELECT
  cce.subject_id,
  cce.cohort_definition_id,
  999 AS window_id,
  cce.cost,
  cce.cost_domain_id,
  cce.length_of_stay,
  cce.cost_event_id,
  cce.event_concept_id
  {@use_cost_standardization} ? {, cpi.inflation_factor}
FROM #cohort_cost_events cce
{@use_cost_standardization} ? {
LEFT JOIN #cpi_data cpi
  ON cce.cost_year = cpi.year }
WHERE cce.incurred_date >= cce.cohort_start_date
  AND cce.incurred_date <= cce.cohort_end_date
;}

CREATE CLUSTERED INDEX IX_we_subject_window ON #windowed_events(subject_id, cohort_definition_id, window_id);

------------------------------------------------------------
-- 4) Roll up to person-level covariates
------------------------------------------------------------
SELECT
  subject_id AS row_id,
  cohort_definition_id,
  covariate_id,
  SUM(value) AS covariate_value
INTO @person_level_table
FROM (
  -- Analysis 1: Total Cost
  {@use_total_cost} ? {
  SELECT
      we.subject_id,
      we.cohort_definition_id,
      10000 + we.window_id AS covariate_id,
      we.cost {@use_cost_standardization} ? {* ISNULL(we.inflation_factor, 1)} AS value
  FROM #windowed_events we
  }

  -- Analysis 2: Cost By Domain
  {@use_domain_cost} ? {
  {@use_total_cost} ? {UNION ALL}
  SELECT
      we.subject_id,
      we.cohort_definition_id,
      20000 + (we.window_id * 100) +
        CASE we.cost_domain_id
          WHEN 'Drug' THEN 1
          WHEN 'Procedure' THEN 2
          WHEN 'Visit' THEN 3
          WHEN 'Device' THEN 4
          WHEN 'Measurement' THEN 5
          WHEN 'Observation' THEN 6
          ELSE 0
        END AS covariate_id,
      we.cost {@use_cost_standardization} ? {* ISNULL(we.inflation_factor, 1)} AS value
  FROM #windowed_events we
  WHERE we.cost_domain_id IN (@cost_domains)
  }

  -- Analysis 3: Utilization Count
  {@use_utilization} ? {
  {@use_total_cost | @use_domain_cost} ? {UNION ALL}
  SELECT
      we.subject_id,
      we.cohort_definition_id,
      30000 + (we.window_id * 100) +
        CASE we.cost_domain_id
          WHEN 'Drug' THEN 1
          WHEN 'Procedure' THEN 2
          WHEN 'Visit' THEN 3
          WHEN 'Device' THEN 4
          ELSE 0
        END AS covariate_id,
      1 AS value
  FROM #windowed_events we
  WHERE we.cost_domain_id IN (@utilization_domains)
  GROUP BY we.subject_id, we.cohort_definition_id, we.window_id, we.cost_domain_id, we.cost_event_id
  }

  -- Analysis 4: Length of Stay
  {@use_length_of_stay} ? {
  {@use_total_cost | @use_domain_cost | @use_utilization} ? {UNION ALL}
  SELECT
      we.subject_id,
      we.cohort_definition_id,
      40000 + we.window_id AS covariate_id,
      we.length_of_stay AS value
  FROM #windowed_events we
  WHERE we.length_of_stay IS NOT NULL
  }

  -- Analysis 5: Cost by Concept Set
  {@use_concept_set} ? {
  {@use_total_cost | @use_domain_cost | @use_utilization | @use_length_of_stay} ? {UNION ALL}
  SELECT
      we.subject_id,
      we.cohort_definition_id,
      50000 + (we.window_id * 10000) + cs.concept_rank AS covariate_id,
      we.cost {@use_cost_standardization} ? {* ISNULL(we.inflation_factor, 1)} AS value
  FROM #windowed_events we
  INNER JOIN #concept_set_codes cs
    ON we.event_concept_id = cs.concept_id
  }
) AS all_covariates
GROUP BY subject_id, cohort_definition_id, covariate_id;


-- Optional cleanup (left to caller / R wrapper when using temp emulation)
DROP TABLE IF EXISTS #windowed_events, #cohort_cost_events, #cost_events;
