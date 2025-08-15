-- Step 4: Aggregate the costs for each cohort and window
-- This summarizes the raw costs into statistics like total, average, and counts.
-- drop target if it exists (SqlRender will emulate for non-MSSQL)
DROP TABLE IF EXISTS #aggregated_costs;
WITH base AS (
  SELECT cohort_definition_id, window_name, person_id, cost
  FROM #cohort_costs
),
-- compute group sizes and ranks
ranks AS (
  SELECT
    cohort_definition_id,
    window_name,
    person_id,
    cost,
    COUNT(*)        OVER (PARTITION BY cohort_definition_id, window_name)      AS n,
    ROW_NUMBER()    OVER (PARTITION BY cohort_definition_id, window_name
                          ORDER BY cost)                                       AS rn,
    SUM(cost)       OVER (PARTITION BY cohort_definition_id, window_name)      AS total_cost,
    AVG(cost)       OVER (PARTITION BY cohort_definition_id, window_name)      AS mean_cost,
    STDDEV(cost)    OVER (PARTITION BY cohort_definition_id, window_name)      AS stddev_cost,  -- SqlRender will map STDDEV/STDEV
    MIN(cost)       OVER (PARTITION BY cohort_definition_id, window_name)      AS min_cost,
    MAX(cost)       OVER (PARTITION BY cohort_definition_id, window_name)      AS max_cost
  FROM base
),
-- choose nearest-rank indices for quartiles (discrete percentiles)
idx AS (
  SELECT
    cohort_definition_id,
    window_name,
    -- nearest-rank definition: ceil(p * n)
    CEIL(0.25 * MAX(n)) AS q1_idx,
    CEIL(0.50 * MAX(n)) AS q2_idx,
    CEIL(0.75 * MAX(n)) AS q3_idx
  FROM ranks
  GROUP BY cohort_definition_id, window_name
),
-- pick values at those indices
q AS (
  SELECT
    r.cohort_definition_id,
    r.window_name,
    -- aggregate once per group
    MAX(CASE WHEN r.rn = i.q1_idx THEN r.cost END) AS q1_cost,
    MAX(CASE WHEN r.rn = i.q2_idx THEN r.cost END) AS median_cost,
    MAX(CASE WHEN r.rn = i.q3_idx THEN r.cost END) AS q3_cost
  FROM ranks r
  JOIN idx i
    ON i.cohort_definition_id = r.cohort_definition_id
   AND i.window_name          = r.window_name
  GROUP BY r.cohort_definition_id, r.window_name
),
-- person counts per group
pc AS (
  SELECT cohort_definition_id, window_name, COUNT(DISTINCT person_id) AS person_count
  FROM base
  GROUP BY cohort_definition_id, window_name
),
-- collapse to one row per group
agg AS (
  SELECT DISTINCT
    cohort_definition_id,
    window_name,
    total_cost,
    mean_cost,
    stddev_cost,
    min_cost,
    max_cost
  FROM ranks
)
-- create the output temp table using INSERT (portable with SqlRender temp emulation)
SELECT
  a.cohort_definition_id,
  a.window_name,
  pc.person_count,
  a.total_cost,
  a.mean_cost,
  a.stddev_cost,
  a.min_cost,
  a.max_cost,
  q.median_cost,
  q.q1_cost,
  q.q3_cost,
  (q.q3_cost - q.q1_cost) AS iqr_cost
INTO #aggregated_costs
FROM agg a
JOIN q   ON q.cohort_definition_id  = a.cohort_definition_id
        AND q.window_name           = a.window_name
JOIN pc  ON pc.cohort_definition_id = a.cohort_definition_id
        AND pc.window_name          = a.window_name;