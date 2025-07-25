/*
  Aggregates person-level results from a temporary table into summary statistics.
*/
SELECT
  covariate_id,
  COUNT(DISTINCT row_id) AS n,
  COUNT_BIG(row_id) AS entries,
  MIN(covariate_value) AS min_value,
  MAX(covariate_value) AS max_value,
  AVG(CAST(covariate_value AS FLOAT)) AS average_value,
  STDEV(covariate_value) AS standard_deviation,
  SUM(CAST(covariate_value AS BIGINT)) as sum_value -- For counts
FROM @person_level_table
GROUP BY covariate_id;