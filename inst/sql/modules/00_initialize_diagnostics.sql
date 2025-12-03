-- ============================================================================
-- Module: Initialize Diagnostics
-- Purpose: Create and initialize the diagnostics tracking table
-- ============================================================================

DROP TABLE IF EXISTS @diag_table;

CREATE TABLE @diag_table (
  step_name      VARCHAR(255),
  n_persons      BIGINT,
  n_events       BIGINT
);

INSERT INTO @diag_table (step_name, n_persons, n_events)
SELECT
  '00_initial_cohort' AS step_name,
  COUNT(DISTINCT subject_id) AS n_persons,
  COUNT(*) AS n_events
FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id = @cohort_id;