/*
  Aggregates person-level results from a temporary table into summary statistics.
*/
SELECT
  cohort_definition_id,
  covariate_id,
  COUNT(DISTINCT row_id)                    AS count_value,
  CAST(COUNT(*) AS BIGINT)                  AS entries,
  MIN(CAST(covariate_value AS DECIMAL(38,8))) AS min_value,
  MAX(CAST(covariate_value AS DECIMAL(38,8))) AS max_value,
  AVG(CAST(covariate_value AS DECIMAL(38,8))) AS average_value,
  STDEV(CAST(covariate_value AS FLOAT))        AS standard_deviation,
  SUM(CAST(covariate_value AS DECIMAL(38,8)))  AS sum_value
FROM @person_level_table
GROUP BY cohort_definition_id, covariate_id;
