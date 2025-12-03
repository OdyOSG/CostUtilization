-- ============================================================================
-- Module: Cost Calculation (CDM v5.5)
-- Purpose: Extract and aggregate costs at visit or visit-detail level
-- ============================================================================

-- 3.1) Extract costs with optional filters; compute cost_date; optional CPI adj
DROP TABLE IF EXISTS #costs_raw;

CREATE TABLE #costs_raw (
  person_id           BIGINT NOT NULL,
  visit_occurrence_id BIGINT NULL,
  visit_detail_id     BIGINT NULL,
  cost                DECIMAL(19,4) NULL,
  adjusted_cost       DECIMAL(19,4) NULL,
  cost_date           DATE   NULL,
  currency_concept_id INT    NULL,
  cost_concept_id     INT    NULL
);

INSERT INTO #costs_raw
SELECT
  c.person_id,
  c.visit_occurrence_id,
  c.visit_detail_id,
  c.cost,
  {@cpi_adjustment} ? { c.cost * COALESCE(cpi.adj_factor, 1.0) } : { c.cost } AS adjusted_cost,
  COALESCE(c.incurred_date, c.paid_date, c.billed_date, c.effective_date) AS cost_date,
  c.currency_concept_id,
  c.cost_concept_id
FROM @cdm_database_schema.cost c
{@cpi_adjustment} ? {
  LEFT JOIN @cpi_adj_table cpi
    ON cpi.year = YEAR(COALESCE(c.incurred_date, c.paid_date, c.billed_date, c.effective_date))
}
WHERE (@cost_concept_id       IS NULL OR c.cost_concept_id      = @cost_concept_id)
  AND (@currency_concept_id   IS NULL OR c.currency_concept_id  = @currency_concept_id)
  AND c.cost IS NOT NULL;

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

-- Diagnostics
{@micro_costing} ? {
  INSERT INTO @diag_table (step_name, n_persons, n_events)
  SELECT '04_with_cost', COUNT(DISTINCT person_id), COUNT(DISTINCT visit_detail_id)
  FROM #line_level_cost;
} : {
  INSERT INTO @diag_table (step_name, n_persons, n_events)
  SELECT '04_with_cost', COUNT(DISTINCT person_id), COUNT(DISTINCT visit_occurrence_id)
  FROM #visit_level_cost;
};