/*
  Person-level cost & utilization covariates with #temp tables.
  This query is designed for a CDM v5.5-like long COST table.
*/

------------------------------------------------------------
-- 0) Safety: drop existing temps
------------------------------------------------------------
IF OBJECT_ID('tempdb..#cohort_cost_events') IS NOT NULL DROP TABLE #cohort_cost_events;
IF OBJECT_ID('tempdb..#windowed_events') IS NOT NULL DROP TABLE #windowed_events;

------------------------------------------------------------
-- 1) Build #cohort_cost_events, deriving domain from the concept table
------------------------------------------------------------
SELECT
    ch.subject_id,
    ch.cohort_definition_id,
    ch.cohort_start_date,
    ch.cohort_end_date,
    c.cost,
    c.cost_id,
    con.domain_id AS cost_domain_id,
    c.cost_type_concept_id,
    c.incurred_date,
    c.cost_concept_id AS event_concept_id,
    vo.visit_occurrence_id,
    CASE
        WHEN vo.visit_concept_id IN (9201, 262) -- Inpatient, ER & Inpatient
        THEN vo.visit_start_date ELSE NULL
    END AS inpatient_start,
    CASE
        WHEN vo.visit_concept_id IN (9201, 262)
        THEN vo.visit_end_date ELSE NULL
    END AS inpatient_end
    {@use_cost_standardization} ? {, YEAR(c.incurred_date) AS cost_year}

INTO #cohort_cost_events
FROM @cohort_database_schema.@cohort_table ch
INNER JOIN @cdm_database_schema.cost c
  ON ch.subject_id = c.person_id
-- All costs must be associated with a visit to be included
INNER JOIN @cdm_database_schema.visit_occurrence vo
  ON c.visit_occurrence_id = vo.visit_occurrence_id
-- Join to concept table to get the domain of the cost event
INNER JOIN @cdm_database_schema.concept con
  ON c.cost_concept_id = con.concept_id
WHERE ch.cohort_definition_id IN (@cohort_ids)
  AND c.currency_concept_id = @currency_concept_id
  AND c.cost IS NOT NULL
  -- Enforce that the cost's incurred_date is within the visit's window
  AND c.incurred_date >= vo.visit_start_date
  AND c.incurred_date <= vo.visit_end_date
  {@cost_type_concept_ids != ''} ? {AND c.cost_type_concept_id IN (@cost_type_concept_ids)};

-----------------------------------------------------------
-- 2) Apply temporal windows (fixed windows and/or in-cohort window)
------------------------------------------------------------
CREATE TABLE #windowed_events (
  subject_id BIGINT NOT NULL,
  cohort_definition_id INT NOT NULL,
  window_id INT NOT NULL,
  cost DECIMAL(38,8) NULL,
  cost_domain_id VARCHAR(50) NULL,
  cost_id BIGINT NULL,
  event_concept_id BIGINT NULL,
  visit_occurrence_id BIGINT NULL,
  inpatient_start DATE NULL,
  inpatient_end DATE NULL
  {@use_cost_standardization} ? {, inflation_factor DECIMAL(18,8) NULL}
);

{@use_fixed_windows} ? {
INSERT INTO #windowed_events (
  subject_id, cohort_definition_id, window_id,
  cost, cost_domain_id, cost_id, event_concept_id,
  visit_occurrence_id, inpatient_start, inpatient_end
  {@use_cost_standardization} ? {, inflation_factor}
)
SELECT
  cce.subject_id,
  cce.cohort_definition_id,
  tw.window_id,
  cce.cost,
  cce.cost_domain_id,
  cce.cost_id,
  cce.event_concept_id,
  cce.visit_occurrence_id,
  cce.inpatient_start,
  cce.inpatient_end
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
  cost, cost_domain_id, cost_id, event_concept_id,
  visit_occurrence_id, inpatient_start, inpatient_end
  {@use_cost_standardization} ? {, inflation_factor}
)
SELECT
  cce.subject_id,
  cce.cohort_definition_id,
  999 AS window_id,
  cce.cost,
  cce.cost_domain_id,
  cce.cost_id,
  cce.event_concept_id,
  cce.visit_occurrence_id,
  cce.inpatient_start,
  cce.inpatient_end
  {@use_cost_standardization} ? {, cpi.inflation_factor}
FROM #cohort_cost_events cce
{@use_cost_standardization} ? {
LEFT JOIN #cpi_data cpi
  ON cce.cost_year = cpi.year }
WHERE cce.incurred_date >= cce.cohort_start_date
  AND cce.incurred_date <= cce.cohort_end_date
;}

------------------------------------------------------------
-- 3) Roll up to person-level covariates
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
          WHEN 'Condition' THEN 7
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
        CASE we.cost_domain_id
          WHEN 'Drug' THEN 1
          WHEN 'Procedure' THEN 2
          WHEN 'Visit' THEN 3
          WHEN 'Device' THEN 4
          WHEN 'Measurement' THEN 5
          WHEN 'Observation' THEN 6
          WHEN 'Condition' THEN 7
          ELSE 0
        END AS covariate_id,
      1 AS value
  FROM #windowed_events we
  WHERE we.cost_domain_id IN (@utilization_domains)
  GROUP BY we.subject_id, we.cohort_definition_id, we.window_id, we.cost_domain_id, we.cost_id
  }

  -- Analysis 4: Length of Stay
  {@use_length_of_stay} ? {
  {@use_total_cost | @use_domain_cost | @use_utilization} ? {UNION ALL}
  -- This subquery calculates LoS for each unique inpatient visit within each window
  -- to prevent double-counting if a visit has multiple cost entries.
  SELECT
    subject_id,
    cohort_definition_id,
    covariate_id,
    los AS value
  FROM (
    SELECT DISTINCT
      we.subject_id,
      we.cohort_definition_id,
      40000 + we.window_id AS covariate_id,
      we.visit_occurrence_id,
      DATEDIFF(DAY, we.inpatient_start, we.inpatient_end) + 1 AS los
    FROM #windowed_events we
    WHERE we.inpatient_start IS NOT NULL
  ) AS distinct_los
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
DROP TABLE IF EXISTS #windowed_events, #cohort_cost_events;