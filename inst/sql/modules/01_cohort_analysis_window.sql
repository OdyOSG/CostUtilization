-- ============================================================================
-- Module: Cohort and Analysis Window Setup
-- Purpose: Create cohort person table and define analysis windows
-- ============================================================================

-- 1.1) Cohort with anchor date (use anchor flag, not dynamic column name)
DROP TABLE IF EXISTS #cohort_person;

CREATE TABLE #cohort_person (
  person_id         BIGINT NOT NULL,
  cohort_start_date DATE   NOT NULL,
  cohort_end_date   DATE   NULL,
  anchor_date       DATE   NOT NULL
);

INSERT INTO #cohort_person
SELECT
  c.subject_id AS person_id,
  c.cohort_start_date,
  c.cohort_end_date,
  CASE WHEN @anchor_on_end = 1 THEN c.cohort_end_date ELSE c.cohort_start_date END AS anchor_date
FROM @cohort_database_schema.@cohort_table c
WHERE c.cohort_definition_id = @cohort_id;

-- 1.2) Analysis windows constrained by observation period
DROP TABLE IF EXISTS #analysis_window;

CREATE TABLE #analysis_window (
  person_id  BIGINT NOT NULL,
  start_date DATE   NOT NULL,
  end_date   DATE   NOT NULL
);

INSERT INTO #analysis_window
SELECT
  cp.person_id,
  CASE
    WHEN op.observation_period_start_date > DATEADD(day, @time_a, cp.anchor_date)
      THEN op.observation_period_start_date
    ELSE DATEADD(day, @time_a, cp.anchor_date)
  END AS start_date,
  CASE
    WHEN op.observation_period_end_date < DATEADD(day, @time_b, cp.anchor_date)
      THEN op.observation_period_end_date
    ELSE DATEADD(day, @time_b, cp.anchor_date)
  END AS end_date
FROM #cohort_person cp
JOIN @cdm_database_schema.observation_period op
  ON op.person_id = cp.person_id
WHERE op.observation_period_start_date <= DATEADD(day, @time_b, cp.anchor_date)
  AND op.observation_period_end_date   >= DATEADD(day, @time_a, cp.anchor_date);

-- 1.3) Valid windows + person-time
DROP TABLE IF EXISTS #analysis_window_clean;

CREATE TABLE #analysis_window_clean (
  person_id   BIGINT NOT NULL,
  start_date  DATE   NOT NULL,
  end_date    DATE   NOT NULL,
  person_days INT    NOT NULL
);

INSERT INTO #analysis_window_clean
SELECT
  person_id,
  start_date,
  end_date,
  DATEDIFF(day, start_date, end_date) + 1 AS person_days
FROM #analysis_window
WHERE end_date >= start_date;

-- Person-time aggregation
DROP TABLE IF EXISTS #person_time;

CREATE TABLE #person_time (
  person_id   BIGINT NOT NULL PRIMARY KEY,
  person_days INT    NOT NULL
);

INSERT INTO #person_time
SELECT 
  person_id, 
  SUM(person_days) AS person_days
FROM #analysis_window_clean
GROUP BY person_id;

-- Log diagnostics
INSERT INTO @diag_table (step_name, n_persons, n_events)
VALUES
  ('01_person_subset', (SELECT COUNT(DISTINCT person_id) FROM #cohort_person), NULL),
  ('02_valid_window',  (SELECT COUNT(DISTINCT person_id) FROM #analysis_window_clean), NULL);