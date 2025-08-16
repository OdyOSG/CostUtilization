{DEFAULT @diagTable = ''}
{DEFAULT @resultsTable = ''}

-- 0) Initial cohort & diagnostics
DROP TABLE IF EXISTS @diagTable;
CREATE TABLE @diagTable (step_name VARCHAR(255), n_persons BIGINT, n_events BIGINT);

INSERT INTO @diagTable (step_name, n_persons)
SELECT
  '00_initial_cohort' AS step_name,
  COUNT(DISTINCT subject_id) AS n_persons
FROM @cohortSchema.@cohortTable
WHERE cohort_definition_id = @cohortId;

-- 1) Create temp tables instead of CTEs
-- Cohort person temp table
DROP TABLE IF EXISTS #cohort_person;
CREATE TABLE #cohort_person AS
SELECT c.subject_id AS person_id, c.cohort_start_date, c.cohort_end_date
FROM @cohortSchema.@cohortTable c
INNER JOIN @cdmSchema.person p ON p.person_id = c.subject_id
WHERE c.cohort_definition_id = @cohortId;

-- Analysis window temp table
DROP TABLE IF EXISTS #analysis_window;
CREATE TABLE #analysis_window AS
SELECT
  cp.person_id,
  CASE WHEN op.observation_period_start_date > DATEADD(day, @startOffsetDays, cp.@anchorCol)
    THEN op.observation_period_start_date ELSE DATEADD(day, @startOffsetDays, cp.@anchorCol) END AS start_date,
  CASE WHEN op.observation_period_end_date   < DATEADD(day, @endOffsetDays,   cp.@anchorCol)
    THEN op.observation_period_end_date ELSE DATEADD(day, @endOffsetDays,   cp.@anchorCol) END AS end_date
FROM #cohort_person cp
JOIN @cdmSchema.observation_period op ON op.person_id = cp.person_id
WHERE op.observation_period_start_date <= DATEADD(day, @endOffsetDays, cp.@anchorCol)
  AND op.observation_period_end_date   >= DATEADD(day, @startOffsetDays, cp.@anchorCol);

-- Clean analysis window temp table
DROP TABLE IF EXISTS #analysis_window_clean;
CREATE TABLE #analysis_window_clean AS
SELECT * FROM #analysis_window WHERE end_date >= start_date;

-- Person time temp table
DROP TABLE IF EXISTS #person_time;
CREATE TABLE #person_time AS
SELECT person_id, SUM(DATEDIFF(day, start_date, end_date) + 1) AS person_days
FROM #analysis_window_clean GROUP BY person_id;

-- Diagnostics Step 1
INSERT INTO @diagTable (step_name, n_persons)
SELECT
  '01_person_subset' AS step_name,
  COUNT(DISTINCT person_id) AS n_persons
FROM #cohort_person;

-- Diagnostics Step 2
INSERT INTO @diagTable (step_name, n_persons)
SELECT
  '02_valid_window' AS step_name,
  COUNT(DISTINCT person_id) AS n_persons
FROM #analysis_window_clean;

-- 2) Visits in window temp table
DROP TABLE IF EXISTS #visits_in_window;
CREATE TABLE #visits_in_window AS
SELECT vo.person_id, vo.visit_occurrence_id, vo.visit_start_date, vo.visit_concept_id
FROM @cdmSchema.visit_occurrence vo
JOIN #analysis_window_clean aw ON aw.person_id = vo.person_id
WHERE vo.visit_start_date BETWEEN aw.start_date AND aw.end_date
  {@if hasVisitRestriction} AND vo.visit_concept_id IN (SELECT visit_concept_id FROM @restrictVisitTable) {@endif};

-- 3) Event filters (optional)
{@if hasEventFilters}
DROP TABLE IF EXISTS #events_by_filter;
CREATE TABLE #events_by_filter AS
-- Note: This UNION ALL structure is efficient for finding any event match.
SELECT ec.filter_id, ec.filter_name, de.person_id, de.visit_occurrence_id, de.visit_detail_id
FROM @cdmSchema.drug_exposure de
JOIN @eventConceptsTable ec ON ec.concept_id = de.drug_concept_id AND (ec.domain_scope IN ('All','Drug'))
UNION ALL
SELECT ec.filter_id, ec.filter_name, po.person_id, po.visit_occurrence_id, po.visit_detail_id
FROM @cdmSchema.procedure_occurrence po
JOIN @eventConceptsTable ec ON ec.concept_id = po.procedure_concept_id AND (ec.domain_scope IN ('All','Procedure'))
UNION ALL
SELECT ec.filter_id, ec.filter_name, co.person_id, co.visit_occurrence_id, NULL
FROM @cdmSchema.condition_occurrence co
JOIN @eventConceptsTable ec ON ec.concept_id = co.condition_concept_id AND (ec.domain_scope IN ('All','Condition'))
UNION ALL
SELECT ec.filter_id, ec.filter_name, ms.person_id, ms.visit_occurrence_id, ms.visit_detail_id
FROM @cdmSchema.measurement ms
JOIN @eventConceptsTable ec ON ec.concept_id = ms.measurement_concept_id AND (ec.domain_scope IN ('All','Measurement'))
UNION ALL
SELECT ec.filter_id, ec.filter_name, ob.person_id, ob.visit_occurrence_id, ob.visit_detail_id
FROM @cdmSchema.observation ob
JOIN @eventConceptsTable ec ON ec.concept_id = ob.observation_concept_id AND (ec.domain_scope IN ('All','Observation'));

DROP TABLE IF EXISTS #event_visits;
CREATE TABLE #event_visits AS
SELECT person_id, visit_occurrence_id
FROM #events_by_filter
WHERE visit_occurrence_id IS NOT NULL
GROUP BY person_id, visit_occurrence_id
HAVING COUNT(DISTINCT filter_id) = @nFilters;

DROP TABLE IF EXISTS #qualifying_visits;
CREATE TABLE #qualifying_visits AS
SELECT v.* FROM #visits_in_window v
JOIN #event_visits ev ON ev.person_id = v.person_id AND ev.visit_occurrence_id = v.visit_occurrence_id;

{@if microCosting}
DROP TABLE IF EXISTS #primary_filter_details;
CREATE TABLE #primary_filter_details AS
SELECT DISTINCT person_id, visit_occurrence_id, visit_detail_id
FROM #events_by_filter
WHERE filter_id = @primaryFilterId AND visit_detail_id IS NOT NULL;
{@endif}
{@else}
DROP TABLE IF EXISTS #qualifying_visits;
CREATE TABLE #qualifying_visits AS SELECT * FROM #visits_in_window;
{@endif}

-- Diagnostics Step 3
INSERT INTO @diagTable (step_name, n_persons, n_events)
SELECT
  '03_with_qualifying_visits' AS step_name,
  COUNT(DISTINCT person_id) AS n_persons,
  COUNT(DISTINCT visit_occurrence_id) AS n_events
FROM #qualifying_visits;

-- 4) Cost linkage temp tables
DROP TABLE IF EXISTS #costs_raw;
CREATE TABLE #costs_raw AS
SELECT cost_event_id, person_id, cost
FROM @cdmSchema.cost
WHERE cost_concept_id = @costConceptId
  AND currency_concept_id = @currencyConceptId;

{@if not microCosting}
DROP TABLE IF EXISTS #visit_level_cost;
CREATE TABLE #visit_level_cost AS
SELECT qv.person_id, qv.visit_occurrence_id, qv.visit_start_date,
       SUM(c.cost) AS total_cost
FROM #qualifying_visits qv
JOIN #costs_raw c
  ON c.cost_event_id = qv.visit_occurrence_id
 AND c.person_id = qv.person_id
GROUP BY qv.person_id, qv.visit_occurrence_id, qv.visit_start_date;
{@endif}

{@if microCosting}
DROP TABLE IF EXISTS #line_level_cost;
CREATE TABLE #line_level_cost AS
SELECT
  qd.person_id,
  qd.visit_occurrence_id,
  vd.visit_detail_start_date,
  qd.visit_detail_id,
  c.cost
FROM #primary_filter_details qd
JOIN @cdmSchema.visit_detail vd ON vd.visit_detail_id = qd.visit_detail_id
JOIN #costs_raw c ON c.cost_event_id = qd.visit_detail_id AND c.person_id = qd.person_id;
{@endif}

-- Diagnostics Step 4
{@if not microCosting}
INSERT INTO @diagTable (step_name, n_persons, n_events)
SELECT
  '04_with_cost' AS step_name,
  COUNT(DISTINCT person_id) AS n_persons,
  COUNT(DISTINCT visit_occurrence_id) AS n_events
FROM #visit_level_cost;
{@endif}
{@if microCosting}
INSERT INTO @diagTable (step_name, n_persons, n_events)
SELECT
  '04_with_cost' AS step_name,
  COUNT(DISTINCT person_id) AS n_persons,
  COUNT(DISTINCT visit_detail_id) AS n_events
FROM #line_level_cost;
{@endif}

-- 5) Denominator temp table
DROP TABLE IF EXISTS #denominator;
CREATE TABLE #denominator AS
SELECT
  SUM(person_days) AS total_person_days,
  SUM(person_days) / 30.4375 AS total_person_months,
  SUM(person_days) / 365.25  AS total_person_years
FROM #person_time pt
JOIN (SELECT DISTINCT person_id FROM #analysis_window_clean) p ON p.person_id = pt.person_id;

-- 6) Numerators temp table
DROP TABLE IF EXISTS #numerators;
CREATE TABLE #numerators AS
{@if not microCosting}
SELECT
  'visitLevel' AS metric_type,
  SUM(total_cost) AS total_cost,
  COUNT(DISTINCT person_id) AS n_persons_with_cost,
  COUNT(DISTINCT visit_occurrence_id) AS distinct_visits,
  COUNT(DISTINCT visit_start_date) AS distinct_visit_dates,
  -1 AS distinct_visit_details
FROM #visit_level_cost
{@endif}
{@if microCosting}
SELECT
  'lineLevel' AS metric_type,
  SUM(cost) AS total_cost,
  COUNT(DISTINCT person_id) AS n_persons_with_cost,
  -1 AS distinct_visits,
  -1 AS distinct_visit_dates,
  COUNT(DISTINCT visit_detail_id) AS distinct_visit_details
FROM #line_level_cost
{@endif};

-- 7) Final results
INSERT INTO @resultsTable
SELECT
  d.total_person_days,
  d.total_person_months,
  d.total_person_years,
  n.*,
  CAST(n.total_cost AS FLOAT) / d.total_person_months AS cost_pppm,
  (CAST(n.distinct_visits AS FLOAT) * 1000.0) / d.total_person_years AS visits_per_1000_py,
  (CAST(n.distinct_visit_dates AS FLOAT) * 1000.0) / d.total_person_years AS visit_dates_per_1000_py,
  (CAST(n.distinct_visit_details AS FLOAT) * 1000.0) / d.total_person_years AS visit_details_per_1000_py
FROM #denominator d
CROSS JOIN #numerators n;

-- Clean up temp tables
DROP TABLE IF EXISTS #cohort_person;
DROP TABLE IF EXISTS #analysis_window;
DROP TABLE IF EXISTS #analysis_window_clean;
DROP TABLE IF EXISTS #person_time;
DROP TABLE IF EXISTS #visits_in_window;
{@if hasEventFilters}
DROP TABLE IF EXISTS #events_by_filter;
DROP TABLE IF EXISTS #event_visits;
DROP TABLE IF EXISTS #primary_filter_details;
{@endif}
DROP TABLE IF EXISTS #qualifying_visits;
DROP TABLE IF EXISTS #costs_raw;
{@if not microCosting}
DROP TABLE IF EXISTS #visit_level_cost;
{@endif}
{@if microCosting}
DROP TABLE IF EXISTS #line_level_cost;
{@endif}
DROP TABLE IF EXISTS #denominator;
DROP TABLE IF EXISTS #numerators;