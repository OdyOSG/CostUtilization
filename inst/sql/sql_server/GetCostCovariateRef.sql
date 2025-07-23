-- Generates covariate reference table for cost covariates

{DEFAULT @covariate_settings = '{}'}

WITH covariate_definitions AS (
  -- Total costs covariates
  SELECT 
    1000 + (window_id * 10) AS covariate_id,
    CONCAT('Total costs during day ', start_day, ' to ', end_day, ' relative to index') AS covariate_name,
    1001 AS analysis_id,
    NULL AS concept_id
  FROM (
    SELECT 1 AS window_id, -365 AS start_day, -1 AS end_day
    UNION ALL
    SELECT 2 AS window_id, -180 AS start_day, -1 AS end_day
    UNION ALL
    SELECT 3 AS window_id, -30 AS start_day, -1 AS end_day
    UNION ALL
    SELECT 4 AS window_id, 0 AS start_day, 0 AS end_day
  ) windows
  
  UNION ALL
  
  -- Domain-specific costs
  SELECT 
    2000 + (window_id * 100) + domain_id AS covariate_id,
    CONCAT(domain_name, ' costs during day ', start_day, ' to ', end_day, ' relative to index') AS covariate_name,
    2001 AS analysis_id,
    NULL AS concept_id
  FROM (
    SELECT 1 AS window_id, -365 AS start_day, -1 AS end_day
    UNION ALL
    SELECT 2 AS window_id, -180 AS start_day, -1 AS end_day
    UNION ALL
    SELECT 3 AS window_id, -30 AS start_day, -1 AS end_day
    UNION ALL
    SELECT 4 AS window_id, 0 AS start_day, 0 AS end_day
  ) windows
  CROSS JOIN (
    SELECT 1 AS domain_id, 'Drug' AS domain_name
    UNION ALL
    SELECT 2 AS domain_id, 'Visit' AS domain_name
    UNION ALL
    SELECT 3 AS domain_id, 'Procedure' AS domain_name
    UNION ALL
    SELECT 4 AS domain_id, 'Device' AS domain_name
    UNION ALL
        SELECT 5 AS domain_id, 'Measurement' AS domain_name
    UNION ALL
    SELECT 6 AS domain_id, 'Observation' AS domain_name
    UNION ALL
    SELECT 7 AS domain_id, 'Specimen' AS domain_name
  ) domains
  
  UNION ALL
  
  -- Utilization counts
  SELECT 
    4000 + (window_id * 10) AS covariate_id,
    CONCAT('Healthcare utilization count during day ', start_day, ' to ', end_day, ' relative to index') AS covariate_name,
    4001 AS analysis_id,
    NULL AS concept_id
  FROM (
    SELECT 1 AS window_id, -365 AS start_day, -1 AS end_day
    UNION ALL
    SELECT 2 AS window_id, -180 AS start_day, -1 AS end_day
    UNION ALL
    SELECT 3 AS window_id, -30 AS start_day, -1 AS end_day
    UNION ALL
    SELECT 4 AS window_id, 0 AS start_day, 0 AS end_day
  ) windows
)
SELECT 
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
FROM covariate_definitions
ORDER BY covariate_id;