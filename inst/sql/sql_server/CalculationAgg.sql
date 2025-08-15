-- Step 5: Calculate denominators (persons & person-days) per window
DROP TABLE IF EXISTS #window_denominators;
SELECT
  cohort_definition_id,
  window_name,
  COUNT(DISTINCT subject_id) AS total_persons,
  -- inclusive day count; SqlRender translates DATEDIFF across dialects
  SUM(
    CASE 
      WHEN window_start IS NOT NULL AND window_end IS NOT NULL 
        THEN DATEDIFF(day, window_start, window_end) + 1
      ELSE 0
    END
  ) AS total_person_days
INTO #window_denominators
FROM #analysis_windows
GROUP BY cohort_definition_id, window_name;

-- Final: join with aggregates and compute PPPD only
SELECT
  ac.cohort_definition_id,
  ac.window_name,
  ac.cost_domain_concept_id,
  ac.person_count,
  ac.event_count,
  ac.total_cost,
  ac.avg_cost,
  ac.stddev_cost,
  ac.min_cost,
  ac.max_cost,
  wd.total_persons,
  wd.total_person_days,
  -- per-person-per-day (PPPD); force non-integer math and avoid /0
  CASE 
    WHEN COALESCE(wd.total_person_days, 0) > 0 
      THEN (ac.total_cost * 1.0) / NULLIF(wd.total_person_days, 0)
    ELSE 0.0
  END AS cost_pppd
FROM #aggregated_costs ac
JOIN #window_denominators wd
  ON ac.cohort_definition_id = wd.cohort_definition_id
 AND ac.window_name          = wd.window_name
ORDER BY ac.cohort_definition_id, ac.window_name, ac.cost_domain_concept_id;
