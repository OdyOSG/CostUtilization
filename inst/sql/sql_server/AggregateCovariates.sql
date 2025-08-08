/*
  Aggregates person-level results from a temporary table into summary statistics.
  FIXES:
  - Adds cohort_definition_id so multiple cohorts don’t mix.
  - Uses FLOAT for sum_value to avoid truncating money.
*/
SELECT
  cohort_definition_id,
  covariate_id,
  COUNT(DISTINCT row_id) AS n,
  COUNT_BIG(row_id) AS entries,
  MIN(covariate_value) AS min_value,
  MAX(covariate_value) AS max_value,
  AVG(CAST(covariate_value AS FLOAT)) AS average_value,
  STDEV(covariate_value) AS standard_deviation,
  SUM(CAST(covariate_value AS FLOAT)) AS sum_value
FROM @person_level_table
GROUP BY cohort_definition_id, covariate_id;