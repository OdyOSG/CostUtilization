/* ============================================================================
 Part 2 — AGGREGATION (Database rollups)
 Consumes: @work_schema.fe_person_time, @work_schema.fe_events
 Produces: @results_table (PMPM, adjusted PMPM, events/1000 PY) + diagnostics
============================================================================ */

/* 0) If needed, append to same diag table */
INSERT INTO @diag_table (step_name, n_persons, n_events)
SELECT '21_features_ready',
       (SELECT COUNT(*) FROM @work_schema.fe_person_time),
       (SELECT COUNT(*) FROM @work_schema.fe_events);

/* 1) Denominators */
DROP TABLE IF EXISTS #denominator;
CREATE TABLE #denominator (
  total_person_days    BIGINT,
  total_person_months  DECIMAL(19,4),
  total_person_years   DECIMAL(19,4)
);

INSERT INTO #denominator
SELECT
  SUM(pt.person_days)                          AS total_person_days,
  SUM(pt.person_days) / 30.4375                AS total_person_months,
  SUM(pt.person_days) / 365.25                 AS total_person_years
FROM @work_schema.fe_person_time pt;

/* 2) Numerators */
DROP TABLE IF EXISTS #numerators;
CREATE TABLE #numerators (
  metric_type            VARCHAR(50),
  total_cost             DECIMAL(19,4),
  total_adjusted_cost    DECIMAL(19,4),
  n_persons_with_cost    BIGINT,
  distinct_visits        BIGINT,
  distinct_events        BIGINT
);

INSERT INTO #numerators
SELECT
  CASE WHEN MIN(event_type) = 'line_level' THEN 'line_level' ELSE 'visit_level' END AS metric_type,
  SUM(cost)                                AS total_cost,
  SUM(adjusted_cost)                        AS total_adjusted_cost,
  COUNT(DISTINCT person_id)                 AS n_persons_with_cost,
  COUNT(DISTINCT visit_occurrence_id)       AS distinct_visits,
  COUNT(DISTINCT event_id)                  AS distinct_events
FROM @work_schema.fe_events;

/* 3) Final results */
DROP TABLE IF EXISTS @results_table;
CREATE TABLE @results_table (
  total_person_days        BIGINT,
  total_person_months      DECIMAL(19,4),
  total_person_years       DECIMAL(19,4),
  metric_type              VARCHAR(50),
  total_cost               DECIMAL(19,4),
  total_adjusted_cost      DECIMAL(19,4),
  n_persons_with_cost      BIGINT,
  distinct_visits          BIGINT,
  distinct_events          BIGINT,
  cost_pppm                DECIMAL(19,4),
  adjusted_cost_pppm       DECIMAL(19,4),
  events_per_1000_py       DECIMAL(19,4),
  calculation_date         DATETIME
);

INSERT INTO @results_table
SELECT
  d.total_person_days,
  d.total_person_months,
  d.total_person_years,
  n.metric_type,
  n.total_cost,
  n.total_adjusted_cost,
  n.n_persons_with_cost,
  n.distinct_visits,
  n.distinct_events,
  CASE WHEN d.total_person_months > 0 THEN n.total_cost          / d.total_person_months ELSE 0 END,
  CASE WHEN d.total_person_months > 0 THEN n.total_adjusted_cost / d.total_person_months ELSE 0 END,
  CASE WHEN d.total_person_years  > 0 THEN (n.distinct_events * 1000.0) / d.total_person_years ELSE 0 END,
  GETDATE()
FROM #denominator d
CROSS JOIN #numerators n;

/* 4) Diagnostics + cleanup */
INSERT INTO @diag_table (step_name, n_persons, n_events)
SELECT '29_agg_complete',
       (SELECT COUNT(*) FROM @work_schema.fe_person_time),
       (SELECT COUNT(*) FROM @work_schema.fe_events);

DROP TABLE IF EXISTS #denominator;
DROP TABLE IF EXISTS #numerators;

INSERT INTO @diag_table VALUES ('99_completed', NULL, NULL);
