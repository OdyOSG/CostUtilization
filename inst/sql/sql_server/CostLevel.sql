-- 3.2) Aggregate to visit or visit-detail level
{@micro_costing} ? {
  DROP TABLE IF EXISTS #line_level_cost;
  SELECT
    qd.person_id,
    qd.visit_occurrence_id,
    qd.visit_detail_id,
    vd.visit_detail_start_date,
    SUM(cr.cost)          AS cost,
    SUM(cr.adjusted_cost) AS adjusted_cost
  INTO #line_level_cost
  FROM #primary_filter_details qd
  JOIN @cdm_database_schema.visit_detail vd
    ON vd.visit_detail_id = qd.visit_detail_id
  JOIN #costs_raw cr
    ON cr.person_id = qd.person_id
   AND cr.visit_detail_id = qd.visit_detail_id
  GROUP BY qd.person_id, qd.visit_occurrence_id, qd.visit_detail_id, vd.visit_detail_start_date;
} : {
  DROP TABLE IF EXISTS #visit_level_cost;
  SELECT
    qv.person_id,
    qv.visit_occurrence_id,
    qv.visit_start_date,
    SUM(cr.cost)          AS cost,
    SUM(cr.adjusted_cost) AS adjusted_cost
  INTO #visit_level_cost
  FROM #qualifying_visits qv
  JOIN #costs_raw cr
    ON cr.person_id = qv.person_id
   AND cr.visit_occurrence_id = qv.visit_occurrence_id
  GROUP BY qv.person_id, qv.visit_occurrence_id, qv.visit_start_date;
};