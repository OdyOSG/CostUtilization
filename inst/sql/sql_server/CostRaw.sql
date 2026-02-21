-- 3.1) Extract costs with optional filters; compute cost_date; optional CPI adj
DROP TABLE IF EXISTS #costs_raw;
CREATE TABLE #costs_raw (
  cohort_definition_id BIGINT    NOT NULL,
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
  cohort_definition_id,
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
JOIN #qualifying_visits qv
ON  c.visit_occurrence_id = qv.visit_occurrence_id
AND (c.visit_detail_id = #qualifying_visits.visit_detail_id
WHERE (@cost_concept_id       IS NULL OR c.cost_concept_id      = @cost_concept_id)
  AND (@currency_concept_id   IS NULL OR c.currency_concept_id  = @currency_concept_id)
  AND c.cost IS NOT NULL;