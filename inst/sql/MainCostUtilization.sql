-- Schema and table parameters
{DEFAULT @cdm_database_schema = 'cdm_531'}
{DEFAULT @cohort_database_schema = 'results'}
{DEFAULT @cohort_table = 'cohort'}
{DEFAULT @results_table = 'cost_results'}
{DEFAULT @diag_table = 'cost_diagnostics'}

-- Analysis definition parameters
{DEFAULT @cohort_id = 0}
{DEFAULT @anchor_col = 'cohort_start_date'} -- Can be 'cohort_start_date' or 'cohort_end_date'
{DEFAULT @time_a = 0} -- Start of analysis window relative to anchor date (days)
{DEFAULT @time_b = 365} -- End of analysis window relative to anchor date (days)

-- Costing parameters
{DEFAULT @cost_concept_id = 0}
{DEFAULT @currency_concept_id = 0}

-- Filtering parameters
{DEFAULT @restrict_visit_table = ''} -- Optional: temp table with visit_concept_ids to restrict to
{DEFAULT @event_concepts_table = ''} -- Optional: temp table with concepts for event filtering
{DEFAULT @primary_filter_id = 0} -- Used only when micro_costing is TRUE
{DEFAULT @n_filters = 1} -- Number of distinct filters a visit must have to qualify

-- Boolean flags for conditional logic
{DEFAULT @has_visit_restriction = FALSE}
{DEFAULT @has_event_filters = FALSE}
{DEFAULT @micro_costing = FALSE}


-- 0) Initial cohort & diagnostics table setup
DROP TABLE IF EXISTS @diag_table;
CREATE TABLE @diag_table (step_name VARCHAR(255), n_persons BIGINT, n_events BIGINT);

INSERT INTO @diag_table (step_name, n_persons)
SELECT
  '00_initial_cohort' AS step_name,
  COUNT(DISTINCT subject_id) AS n_persons
FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id = @cohort_id;


-- 1) Create temp tables for cohort and analysis window
DROP TABLE IF EXISTS #cohort_person;
SELECT c.subject_id AS person_id, c.cohort_start_date, c.cohort_end_date
INTO #cohort_person
FROM @cohort_database_schema.@cohort_table c
WHERE c.cohort_definition_id = @cohort_id;

DROP TABLE IF EXISTS #analysis_window;
SELECT
  cp.person_id,
  CASE WHEN op.observation_period_start_date > DATEADD(day, @time_a, cp.@anchor_col)
    THEN op.observation_period_start_date ELSE DATEADD(day, @time_a, cp.@anchor_col) END AS start_date,
  CASE WHEN op.observation_period_end_date   < DATEADD(day, @time_b, cp.@anchor_col)
    THEN op.observation_period_end_date ELSE DATEADD(day, @time_b, cp.@anchor_col) END AS end_date
INTO #analysis_window
FROM #cohort_person cp
JOIN @cdm_database_schema.observation_period op ON op.person_id = cp.person_id
WHERE op.observation_period_start_date <= DATEADD(day, @time_b, cp.@anchor_col)
  AND op.observation_period_end_date   >= DATEADD(day, @time_a, cp.@anchor_col);

DROP TABLE IF EXISTS #analysis_window_clean;
SELECT * INTO #analysis_window_clean FROM #analysis_window WHERE end_date >= start_date;

DROP TABLE IF EXISTS #person_time;
SELECT person_id, SUM(DATEDIFF(day, start_date, end_date) + 1) AS person_days
INTO #person_time
FROM #analysis_window_clean GROUP BY person_id;

-- Diagnostics Step 1 & 2
INSERT INTO @diag_table (step_name, n_persons)
SELECT '01_person_subset' AS step_name, COUNT(DISTINCT person_id) FROM #cohort_person;

INSERT INTO @diag_table (step_name, n_persons)
SELECT '02_valid_window' AS step_name, COUNT(DISTINCT person_id) FROM #analysis_window_clean;


-- 2) Identify all visits occurring within the analysis window
DROP TABLE IF EXISTS #visits_in_window;
SELECT vo.person_id, vo.visit_occurrence_id, vo.visit_start_date, vo.visit_concept_id
INTO #visits_in_window
FROM @cdm_database_schema.visit_occurrence vo
JOIN #analysis_window_clean aw ON aw.person_id = vo.person_id
WHERE vo.visit_start_date BETWEEN aw.start_date AND aw.end_date
{@has_visit_restriction} ? {AND vo.visit_concept_id IN (SELECT visit_concept_id FROM @restrict_visit_table)};


-- 3) Apply event filters to identify qualifying visits (if specified)
{@has_event_filters} ? {
  DROP TABLE IF EXISTS #events_by_filter;
  -- This UNION ALL structure is efficient for finding any event match across domains.
  SELECT ec.filter_id, ec.filter_name, de.person_id, de.visit_occurrence_id, de.visit_detail_id
  INTO #events_by_filter
  FROM @cdm_database_schema.drug_exposure de
  JOIN @event_concepts_table ec ON ec.concept_id = de.drug_concept_id AND (ec.domain_scope IN ('All','Drug'))
  UNION ALL
  SELECT ec.filter_id, ec.filter_name, po.person_id, po.visit_occurrence_id, po.visit_detail_id
  FROM @cdm_database_schema.procedure_occurrence po
  JOIN @event_concepts_table ec ON ec.concept_id = po.procedure_concept_id AND (ec.domain_scope IN ('All','Procedure'))
  UNION ALL
    SELECT ec.filter_id, ec.filter_name, co.person_id, co.visit_occurrence_id, NULL AS visit_detail_id
  FROM @cdm_database_schema.condition_occurrence co
  JOIN @event_concepts_table ec ON ec.concept_id = co.condition_concept_id AND (ec.domain_scope IN ('All','Condition'))
  UNION ALL
  SELECT ec.filter_id, ec.filter_name, ms.person_id, ms.visit_occurrence_id, ms.visit_detail_id
  FROM @cdm_database_schema.measurement ms
  JOIN @event_concepts_table ec ON ec.concept_id = ms.measurement_concept_id AND (ec.domain_scope IN ('All','Measurement'))
  UNION ALL
  SELECT ec.filter_id, ec.filter_name, ob.person_id, ob.visit_occurrence_id, ob.visit_detail_id
  FROM @cdm_database_schema.observation ob
  JOIN @event_concepts_table ec ON ec.concept_id = ob.observation_concept_id AND (ec.domain_scope IN ('All','Observation'));

  DROP TABLE IF EXISTS #event_visits;
  SELECT person_id, visit_occurrence_id
  INTO #event_visits
  FROM #events_by_filter
  WHERE visit_occurrence_id IS NOT NULL
  GROUP BY person_id, visit_occurrence_id
  HAVING COUNT(DISTINCT filter_id) >= @n_filters;

  DROP TABLE IF EXISTS #qualifying_visits;
  SELECT v.*
  INTO #qualifying_visits
  FROM #visits_in_window v
  JOIN #event_visits ev ON ev.person_id = v.person_id AND ev.visit_occurrence_id = v.visit_occurrence_id;

  {@micro_costing} ? {
    DROP TABLE IF EXISTS #primary_filter_details;
    SELECT DISTINCT person_id, visit_occurrence_id, visit_detail_id
    INTO #primary_filter_details
    FROM #events_by_filter
    WHERE filter_id = @primary_filter_id AND visit_detail_id IS NOT NULL;
  }
} : {
  DROP TABLE IF EXISTS #qualifying_visits;
  SELECT * INTO #qualifying_visits FROM #visits_in_window;
}

-- Diagnostics Step 3
INSERT INTO @diag_table (step_name, n_persons, n_events)
SELECT
  '03_with_qualifying_visits' AS step_name,
  COUNT(DISTINCT person_id) AS n_persons,
  COUNT(DISTINCT visit_occurrence_id) AS n_events
FROM #qualifying_visits;


-- 4) Link costs to qualifying events
DROP TABLE IF EXISTS #costs_raw;
SELECT cost_event_id, person_id, cost
INTO #costs_raw
FROM @cdm_database_schema.cost
WHERE cost_concept_id = @cost_concept_id
  AND currency_concept_id = @currency_concept_id;

{@micro_costing} ? {
  DROP TABLE IF EXISTS #line_level_cost;
  SELECT
    qd.person_id,
    qd.visit_occurrence_id,
    vd.visit_detail_start_date,
    qd.visit_detail_id,
    c.cost
  INTO #line_level_cost
  FROM #primary_filter_details qd
  JOIN @cdm_database_schema.visit_detail vd ON vd.visit_detail_id = qd.visit_detail_id
  JOIN #costs_raw c ON c.cost_event_id = qd.visit_detail_id AND c.person_id = qd.person_id;
} : {
  DROP TABLE IF EXISTS #visit_level_cost;
  SELECT qv.person_id, qv.visit_occurrence_id, qv.visit_start_date,
         SUM(c.cost) AS total_cost
  INTO #visit_level_cost
  FROM #qualifying_visits qv
  JOIN #costs_raw c
    ON c.cost_event_id = qv.visit_occurrence_id
   AND c.person_id = qv.person_id
  GROUP BY qv.person_id, qv.visit_occurrence_id, qv.visit_start_date;
}

-- Diagnostics Step 4
{@micro_costing} ? {
  INSERT INTO @diag_table (step_name, n_persons, n_events)
  SELECT
    '04_with_cost' AS step_name,
    COUNT(DISTINCT person_id) AS n_persons,
    COUNT(DISTINCT visit_detail_id) AS n_events
  FROM #line_level_cost;
} : {
  INSERT INTO @diag_table (step_name, n_persons, n_events)
  SELECT
    '04_with_cost' AS step_name,
    COUNT(DISTINCT person_id) AS n_persons,
    COUNT(DISTINCT visit_occurrence_id) AS n_events
  FROM #visit_level_cost;
}


-- 5) Calculate denominator (total person-time)
DROP TABLE IF EXISTS #denominator;
SELECT
  SUM(person_days) AS total_person_days,
  SUM(person_days) / 30.4375 AS total_person_months,
  SUM(person_days) / 91.3125 AS total_person_quarters,
  SUM(person_days) / 365.25  AS total_person_years
INTO #denominator
FROM #person_time pt
JOIN (SELECT DISTINCT person_id FROM #analysis_window_clean) p ON p.person_id = pt.person_id;


-- 6) Calculate numerators (costs and event counts)
DROP TABLE IF EXISTS #numerators;
{@micro_costing} ? {
  SELECT
    'line_level' AS metric_type,
    SUM(cost) AS total_cost,
    COUNT(DISTINCT person_id) AS n_persons_with_cost,
    -1 AS distinct_visits,
    -1 AS distinct_visit_dates,
    COUNT(DISTINCT visit_detail_id) AS distinct_visit_details
  INTO #numerators
  FROM #line_level_cost;
} : {
  SELECT
    'visit_level' AS metric_type,
    SUM(total_cost) AS total_cost,
        COUNT(DISTINCT person_id) AS n_persons_with_cost,
    COUNT(DISTINCT visit_occurrence_id) AS distinct_visits,
    COUNT(DISTINCT visit_start_date) AS distinct_visit_dates,
    -1 AS distinct_visit_details
  INTO #numerators
  FROM #visit_level_cost;
}


-- 7) Combine numerators and denominator for final results
INSERT INTO @results_table
SELECT
  d.total_person_days,
  d.total_person_months,
  d.total_person_years,
  n.*,
  -- Safety: Prevent division by zero if person-time is zero
  CASE WHEN d.total_person_months > 0 THEN CAST(n.total_cost AS FLOAT) / d.total_person_months ELSE 0 END AS cost_pppm,
  CASE WHEN d.total_person_years > 0 THEN (CAST(n.distinct_visits AS FLOAT) * 1000.0) / d.total_person_years ELSE 0 END AS visits_per_1000_py,
  CASE WHEN d.total_person_years > 0 THEN (CAST(n.distinct_visit_dates AS FLOAT) * 1000.0) / d.total_person_years ELSE 0 END AS visit_dates_per_1000_py,
  CASE WHEN d.total_person_years > 0 THEN (CAST(n.distinct_visit_details AS FLOAT) * 1000.0) / d.total_person_years ELSE 0 END AS visit_details_per_1000_py
FROM #denominator d
CROSS JOIN #numerators n;


-- 8) Clean up all temporary tables
DROP TABLE IF EXISTS #cohort_person;
DROP TABLE IF EXISTS #analysis_window;
DROP TABLE IF EXISTS #analysis_window_clean;
DROP TABLE IF EXISTS #person_time;
DROP TABLE IF EXISTS #visits_in_window;
{@has_event_filters} ? {
  DROP TABLE IF EXISTS #events_by_filter;
  DROP TABLE IF EXISTS #event_visits;
  {@micro_costing} ? {
    DROP TABLE IF EXISTS #primary_filter_details;
  }
}
DROP TABLE IF EXISTS #qualifying_visits;
DROP TABLE IF EXISTS #costs_raw;
{@micro_costing} ? {
  DROP TABLE IF EXISTS #line_level_cost;
} : {
  DROP TABLE IF EXISTS #visit_level_cost;
}
DROP TABLE IF EXISTS #denominator;
DROP TABLE IF EXISTS #numerators;
    