-- Create analysis windows with observation period clipping

CREATE TABLE #analysis_window (
  person_id BIGINT,
  cohort_definition_id BIGINT,
  start_date DATETIME,
  end_date DATETIME
);

INSERT INTO #analysis_window (
  person_id,
  cohort_definition_id,
  start_date,
  end_date
)
SELECT
  c.subject_id AS person_id,
  c.cohort_definition_id,
  CASE
    WHEN op.observation_period_start_date >
      DATEADD(day, @start_offset,
        CASE WHEN '@start_with' = 'start' THEN c.cohort_start_date ELSE c.cohort_end_date END)
    THEN op.observation_period_start_date
    ELSE DATEADD(day, @start_offset,
      CASE WHEN '@start_with' = 'start' THEN c.cohort_start_date ELSE c.cohort_end_date END)
  END AS start_date,
  CASE
    WHEN op.observation_period_end_date <
      DATEADD(day, @end_offset,
        CASE WHEN '@end_with' = 'start' THEN c.cohort_start_date ELSE c.cohort_end_date END)
    THEN op.observation_period_end_date
    ELSE DATEADD(day, @end_offset,
      CASE WHEN '@end_with' = 'start' THEN c.cohort_start_date ELSE c.cohort_end_date END)
  END AS end_date
FROM (SELECT * FROM @cohort_table WHERE
cohort_definition_id IN (@target_ids)) c
INNER JOIN @cdm_database_schema.observation_period op
  ON op.person_id = c.subject_id
WHERE
  -- Ensure window overlaps with observat ion period
  DATEADD(day, @start_offset,
    CASE WHEN '@start_with' = 'start' THEN c.cohort_start_date ELSE c.cohort_end_date END)
    <= op.observation_period_end_date
  AND
  DATEADD(day, @end_offset,
    CASE WHEN '@end_with' = 'start' THEN c.cohort_start_date ELSE c.cohort_end_date END)
    >= op.observation_period_start_date
  -- Ensure window is valid (start <= end)
  AND CASE
    WHEN op.observation_period_start_date >
      DATEADD(day, @start_offset,
        CASE WHEN '@start_with' = 'start' THEN c.cohort_start_date ELSE c.cohort_end_date END)
    THEN op.observation_period_start_date
    ELSE DATEADD(day, @start_offset,
      CASE WHEN '@start_with' = 'start' THEN c.cohort_start_date ELSE c.cohort_end_date END)
  END <=
  CASE
    WHEN op.observation_period_end_date <
      DATEADD(day, @end_offset,
        CASE WHEN '@end_with' = 'start' THEN c.cohort_start_date ELSE c.cohort_end_date END)
    THEN op.observation_period_end_date
    ELSE DATEADD(day, @end_offset,
      CASE WHEN '@end_with' = 'start' THEN c.cohort_start_date ELSE c.cohort_end_date END)
  END;
