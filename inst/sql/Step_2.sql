/* ============================================================================
   SECTION 4: FINAL CALCULATIONS
   ============================================================================ */

-- 4.1) Denominator totals (person-time)
DROP TABLE IF EXISTS #denominator;
CREATE TABLE #denominator (
  total_person_days      BIGINT,
  total_person_months    DECIMAL(19,4),
  total_person_quarters  DECIMAL(19,4),
  total_person_years     DECIMAL(19,4)
);

INSERT INTO #denominator
SELECT
  SUM(pt.person_days)                           AS total_person_days,
  SUM(pt.person_days) / 30.4375                 AS total_person_months,
  SUM(pt.person_days) / 91.3125                 AS total_person_quarters, -- 365.25 / 4
  SUM(pt.person_days) / 365.25                  AS total_person_years
FROM #person_time pt
WHERE EXISTS (
  SELECT 1
  FROM #analysis_window_clean awc
  WHERE awc.person_id = pt.person_id
);

-- 4.2) Numerators
DROP TABLE IF EXISTS #numerators;
CREATE TABLE #numerators (
  metric_type            VARCHAR(50),
  total_cost             DECIMAL(19,4),
  total_adjusted_cost    DECIMAL(19,4),
  n_persons_with_cost    BIGINT,
  distinct_visits        BIGINT,
  distinct_events        BIGINT
);

{@micro_costing} ? {
  INSERT INTO #numerators
  SELECT
    'line_level',
    SUM(cost),
    SUM(adjusted_cost),
    COUNT(DISTINCT person_id),
    CAST(-1 AS BIGINT),
    COUNT(DISTINCT visit_detail_id)
  FROM #line_level_cost;
} : {
  INSERT INTO #numerators
  SELECT
    'visit_level',
    SUM(cost),
    SUM(adjusted_cost),
    COUNT(DISTINCT person_id),
    COUNT(DISTINCT visit_occurrence_id),
    COUNT(DISTINCT visit_occurrence_id)
  FROM #visit_level_cost;
};

-- 4.3) Results
DROP TABLE IF EXISTS @results_table;
CREATE TABLE @results_table (
  total_person_days        BIGINT,
  total_person_months      DECIMAL(19,4),
  total_person_quarters    DECIMAL(19,4),
  total_person_years       DECIMAL(19,4),
  metric_type              VARCHAR(50),
  total_cost               DECIMAL(19,4),
  total_adjusted_cost      DECIMAL(19,4),
  n_persons_with_cost      BIGINT,
  distinct_visits          BIGINT,
  distinct_events          BIGINT,
  -- Per-person-per-month (PPPM)
  cost_pppm                DECIMAL(19,4),
  adjusted_cost_pppm       DECIMAL(19,4),
  -- Per-person-per-quarter (PPPQ)
  cost_pppq                DECIMAL(19,4),
  adjusted_cost_pppq       DECIMAL(19,4),
  -- Per-person-per-year (PPPY)
  cost_pppy                DECIMAL(19,4),
  adjusted_cost_pppy       DECIMAL(19,4),
  -- Events rate (kept as per 1000 PY)
  events_per_1000_py       DECIMAL(19,4)
);

INSERT INTO @results_table
SELECT
  d.total_person_days,
  d.total_person_months,
  d.total_person_quarters,
  d.total_person_years,
  n.metric_type,
  n.total_cost,
  n.total_adjusted_cost,
  n.n_persons_with_cost,
  n.distinct_visits,
  n.distinct_events,
  -- PPPM
  CASE WHEN d.total_person_months   > 0 THEN n.total_cost          / d.total_person_months   ELSE 0 END AS cost_pppm,
  CASE WHEN d.total_person_months   > 0 THEN n.total_adjusted_cost / d.total_person_months   ELSE 0 END AS adjusted_cost_pppm,
  -- PPPQ
  CASE WHEN d.total_person_quarters > 0 THEN n.total_cost          / d.total_person_quarters ELSE 0 END AS cost_pppq,
  CASE WHEN d.total_person_quarters > 0 THEN n.total_adjusted_cost / d.total_person_quarters ELSE 0 END AS adjusted_cost_pppq,
  -- PPPY
  CASE WHEN d.total_person_years    > 0 THEN n.total_cost          / d.total_person_years    ELSE 0 END AS cost_pppy,
  CASE WHEN d.total_person_years    > 0 THEN n.total_adjusted_cost / d.total_person_years    ELSE 0 END AS adjusted_cost_pppy,
  -- Events per 1000 PY (unchanged)
  CASE WHEN d.total_person_years    > 0 THEN (n.distinct_events * 1000.0) / d.total_person_years ELSE 0 END AS events_per_1000_py
FROM #denominator d
CROSS JOIN #numerators n;


/* ============================================================================
   SECTION 5: CLEANUP
   ============================================================================ */
DROP TABLE IF EXISTS #cohort_person;
DROP TABLE IF EXISTS #analysis_window;
DROP TABLE IF EXISTS #analysis_window_clean;
DROP TABLE IF EXISTS #person_time;
DROP TABLE IF EXISTS #visits_in_window;
DROP TABLE IF EXISTS #qualifying_visits;

{@has_event_filters} ? {
  DROP TABLE IF EXISTS #events_by_filter;
  DROP TABLE IF EXISTS #event_visits;
  {@micro_costing} ? { DROP TABLE IF EXISTS #primary_filter_details; }
}

DROP TABLE IF EXISTS #costs_raw;
{@micro_costing} ? { DROP TABLE IF EXISTS #line_level_cost; } : { DROP TABLE IF EXISTS #visit_level_cost; }
DROP TABLE IF EXISTS #denominator;
DROP TABLE IF EXISTS #numerators;

-- Final diagnostic
INSERT INTO @diag_table (step_name, n_persons, n_events)
VALUES ('99_completed', NULL, NULL);


