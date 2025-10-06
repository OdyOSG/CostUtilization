-- ============================================================================
-- Healthcare Cost Analysis Query (Consolidated, CDM v5.5 compliant)
/* 0) Initialize diagnostics table */

-- Return results and diagnostics tables to keep # consistent with other modules
SELECT * FROM #temp_results_table;

SELECT * FROM #diag_table ORDER BY step_name;
-- Collect tables



DROP TABLE IF EXISTS #diag_table;
CREATE TABLE #diag_table (
  step_name      VARCHAR(255),
  n_persons      BIGINT,
  n_events       BIGINT
);

INSERT INTO #diag_table (step_name, n_persons, n_events)
SELECT
  '00_initial_cohort',
  COUNT(DISTINCT subject_id),
  COUNT(*)
FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id = @cohort_id;

/* ============================================================================
   SECTION 1: COHORT AND ANALYSIS WINDOW SETUP
   ============================================================================ */

-- 1.1) Cohort with anchor date (use anchor flag, not dynamic column name)
DROP TABLE IF EXISTS #cohort_person;
CREATE TABLE #cohort_person (
  person_id         BIGINT NOT NULL,
  cohort_start_date DATE   NOT NULL,
  cohort_end_date   DATE   NULL,
  anchor_date       DATE   NOT NULL
);

INSERT INTO #cohort_person
SELECT
  c.subject_id AS person_id,
  c.cohort_start_date,
  c.cohort_end_date,
  CASE WHEN @anchor_on_end = 1 THEN c.cohort_end_date ELSE c.cohort_start_date END AS anchor_date
FROM @cohort_database_schema.@cohort_table c
WHERE c.cohort_definition_id = @cohort_id;

-- 1.2) Analysis windows constrained by observation period
DROP TABLE IF EXISTS #analysis_window;
CREATE TABLE #analysis_window (
  person_id  BIGINT NOT NULL,
  start_date DATE   NOT NULL,
  end_date   DATE   NOT NULL
);

INSERT INTO #analysis_window
SELECT
  cp.person_id,
  CASE
    WHEN op.observation_period_start_date > DATEADD(day, @time_a, cp.anchor_date)
      THEN op.observation_period_start_date
    ELSE DATEADD(day, @time_a, cp.anchor_date)
  END AS start_date,
  CASE
    WHEN op.observation_period_end_date < DATEADD(day, @time_b, cp.anchor_date)
      THEN op.observation_period_end_date
    ELSE DATEADD(day, @time_b, cp.anchor_date)
  END AS end_date
FROM #cohort_person cp
JOIN @cdm_database_schema.observation_period op
  ON op.person_id = cp.person_id
WHERE op.observation_period_start_date <= DATEADD(day, @time_b, cp.anchor_date)
  AND op.observation_period_end_date   >= DATEADD(day, @time_a, cp.anchor_date);

-- 1.3) Valid windows + person-time
DROP TABLE IF EXISTS #analysis_window_clean;
CREATE TABLE #analysis_window_clean (
  person_id   BIGINT NOT NULL,
  start_date  DATE   NOT NULL,
  end_date    DATE   NOT NULL,
  person_days INT    NOT NULL
);

INSERT INTO #analysis_window_clean
SELECT
  person_id,
  start_date,
  end_date,
  DATEDIFF(day, start_date, end_date) + 1
FROM #analysis_window
WHERE end_date >= start_date;

DROP TABLE IF EXISTS #person_time;
CREATE TABLE #person_time (
  person_id   BIGINT NOT NULL PRIMARY KEY,
  person_days INT    NOT NULL
);

INSERT INTO #person_time
SELECT person_id, SUM(person_days)
FROM #analysis_window_clean
GROUP BY person_id;

-- Log diagnostics (same 3 columns as the table)
INSERT INTO #diag_table (step_name, n_persons, n_events)
SELECT
  '01_person_subset' AS step_name,
  COUNT(DISTINCT person_id) AS n_persons,
  CAST(NULL AS BIGINT) AS n_events
FROM #cohort_person
UNION ALL
SELECT
  '02_valid_window',
  COUNT(DISTINCT person_id),
  CAST(NULL AS BIGINT)
FROM #analysis_window_clean;;
/* ============================================================================
   SECTION 2: IDENTIFY QUALIFYING VISITS
   ============================================================================ */

-- 2.1) Visits overlapping analysis windows (use overlap rule)
DROP TABLE IF EXISTS #visits_in_window;
CREATE TABLE #visits_in_window (
  person_id           BIGINT NOT NULL,
  visit_occurrence_id BIGINT NOT NULL,
  visit_start_date    DATE   NOT NULL,
  visit_end_date      DATE   NOT NULL,
  visit_concept_id    INT    NULL
);

INSERT INTO #visits_in_window
SELECT
  vo.person_id,
  vo.visit_occurrence_id,
  vo.visit_start_date,
  vo.visit_end_date,
  vo.visit_concept_id
FROM @cdm_database_schema.visit_occurrence vo
JOIN #analysis_window_clean aw
  ON aw.person_id = vo.person_id
WHERE vo.visit_end_date   >= aw.start_date
  AND vo.visit_start_date <= aw.end_date
{@has_visit_restriction} ? {
  AND vo.visit_concept_id IN (SELECT visit_concept_id FROM @restrict_visit_table)
};

-- 2.2) Optional event filters -> qualifying visits
{@has_event_filters} ? {

  DROP TABLE IF EXISTS #events_by_filter;
  CREATE TABLE #events_by_filter (
    filter_id          INT          NOT NULL,
    filter_name        VARCHAR(255) NULL,
    person_id          BIGINT       NOT NULL,
    visit_occurrence_id BIGINT      NULL,
    visit_detail_id    BIGINT       NULL
  );

  INSERT INTO #events_by_filter
  SELECT ec.filter_id, ec.filter_name, de.person_id, de.visit_occurrence_id, de.visit_detail_id
  FROM @cdm_database_schema.drug_exposure de
  JOIN @event_concepts_table ec
    -- MODIFIED: Handle NULL concept_id to mean all concepts in domain
    ON (ec.concept_id = de.drug_concept_id OR ec.concept_id IS NULL)
   AND ec.domain_scope IN ('All','Drug')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, po.person_id, po.visit_occurrence_id, po.visit_detail_id
  FROM @cdm_database_schema.procedure_occurrence po
  JOIN @event_concepts_table ec
    -- MODIFIED: Handle NULL concept_id to mean all concepts in domain
    ON (ec.concept_id = po.procedure_concept_id OR ec.concept_id IS NULL)
   AND ec.domain_scope IN ('All','Procedure')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, co.person_id, co.visit_occurrence_id, NULL
  FROM @cdm_database_schema.condition_occurrence co
  JOIN @event_concepts_table ec
    -- MODIFIED: Handle NULL concept_id to mean all concepts in domain
    ON (ec.concept_id = co.condition_concept_id OR ec.concept_id IS NULL)
   AND ec.domain_scope IN ('All','Condition')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, ms.person_id, ms.visit_occurrence_id, ms.visit_detail_id
  FROM @cdm_database_schema.measurement ms
  JOIN @event_concepts_table ec
    -- MODIFIED: Handle NULL concept_id to mean all concepts in domain
    ON (ec.concept_id = ms.measurement_concept_id OR ec.concept_id IS NULL)
   AND ec.domain_scope IN ('All','Measurement')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, ob.person_id, ob.visit_occurrence_id, ob.visit_detail_id
  FROM @cdm_database_schema.observation ob
  JOIN @event_concepts_table ec
    -- MODIFIED: Handle NULL concept_id to mean all concepts in domain
    ON (ec.concept_id = ob.observation_concept_id OR ec.concept_id IS NULL)
   AND ec.domain_scope IN ('All','Observation')
  ;
  
  DROP TABLE IF EXISTS #event_visits;
  CREATE TABLE #event_visits (
    person_id           BIGINT NOT NULL,
    visit_occurrence_id BIGINT NOT NULL
  );

  INSERT INTO #event_visits
  SELECT person_id, visit_occurrence_id
  FROM #events_by_filter
  WHERE visit_occurrence_id IS NOT NULL
  GROUP BY person_id, visit_occurrence_id
  HAVING COUNT(DISTINCT filter_id) >= @n_filters;

  DROP TABLE IF EXISTS #qualifying_visits;
  SELECT v.*
  INTO #qualifying_visits
  FROM #visits_in_window v
  JOIN #event_visits ev
    ON ev.person_id = v.person_id
   AND ev.visit_occurrence_id = v.visit_occurrence_id;

  {@micro_costing} ? {
    DROP TABLE IF EXISTS #primary_filter_details;
    CREATE TABLE #primary_filter_details (
      person_id           BIGINT NOT NULL,
      visit_occurrence_id BIGINT NOT NULL,
      visit_detail_id     BIGINT NOT NULL
    );

    INSERT INTO #primary_filter_details
    SELECT DISTINCT person_id, visit_occurrence_id, visit_detail_id
    FROM #events_by_filter
    WHERE filter_id = @primary_filter_id
      AND visit_detail_id IS NOT NULL;
  }

} : {

  DROP TABLE IF EXISTS #qualifying_visits;
  SELECT *
  INTO #qualifying_visits
  FROM #visits_in_window;
};


-- Diagnostics
INSERT INTO #diag_table (step_name, n_persons, n_events)
SELECT
  '03_with_qualifying_visits',
  COALESCE(COUNT(DISTINCT person_id), 0),
  COALESCE(COUNT(DISTINCT visit_occurrence_id), 0)
FROM #qualifying_visits;

/* ============================================================================
   SECTION 3: COST CALCULATION (CDM v5.5)
   ============================================================================ */

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

{@aggregated} ? {
{@micro_costing} ? {
  INSERT INTO #diag_table (step_name, n_persons, n_events)
  SELECT '04_with_cost', COUNT(DISTINCT person_id), COUNT(DISTINCT visit_detail_id)
  FROM #line_level_cost;
} : {
  INSERT INTO #diag_table (step_name, n_persons, n_events)
  SELECT '04_with_cost', COUNT(DISTINCT person_id), COUNT(DISTINCT visit_occurrence_id)
  FROM #visit_level_cost;
};

/* ============================================================================
   SECTION 4: FINAL CALCULATIONS (LONG FORMAT)
   ============================================================================ */

-- 4.1) Denominator totals (person-time)
DROP TABLE IF EXISTS #denominator;
CREATE TABLE #denominator (
  total_person_days      BIGINT,
  total_person_months    DECIMAL(19,4),
  total_person_quarters  DECIMAL(19,4),
  total_person_years     DECIMAL(19,4)
);

INSERT INTO #denominator
SELECT
  SUM(pt.person_days)                           AS total_person_days,
  SUM(pt.person_days) / 30.4375                 AS total_person_months,
  SUM(pt.person_days) / 91.3125                 AS total_person_quarters, -- 365.25 / 4
  SUM(pt.person_days) / 365.25                  AS total_person_years
FROM #person_time pt
WHERE EXISTS (
  SELECT 1
  FROM #analysis_window_clean awc
  WHERE awc.person_id = pt.person_id
);

-- 4.2) Numerators
DROP TABLE IF EXISTS #numerators;
CREATE TABLE #numerators (
  metric_type            VARCHAR(50),
  total_cost             DECIMAL(19,4),
  total_adjusted_cost    DECIMAL(19,4),
  n_persons_with_cost    BIGINT,
  distinct_visits        BIGINT,
  distinct_events        BIGINT
);

{@micro_costing} ? {
  INSERT INTO #numerators
  SELECT
    'line_level',
    SUM(cost),
    SUM(adjusted_cost),
    COUNT(DISTINCT person_id),
    CAST(-1 AS BIGINT),
    COUNT(DISTINCT visit_detail_id)
  FROM #line_level_cost;
} : {
  INSERT INTO #numerators
  SELECT
    'visit_level',
    SUM(cost),
    SUM(adjusted_cost),
    COUNT(DISTINCT person_id),
    COUNT(DISTINCT visit_occurrence_id),
    COUNT(DISTINCT visit_occurrence_id)
  FROM #visit_level_cost;
};

-- 4.3) Results (long format)
DROP TABLE IF EXISTS #temp_results_table;
CREATE TABLE #temp_results_table (
  metric_type   VARCHAR(50),
  metric_name   VARCHAR(255),
  metric_value  DECIMAL(19,4)
);

INSERT INTO #temp_results_table
SELECT
  n.metric_type,
  'total_person_days' AS metric_name,
  d.total_person_days AS metric_value
FROM #denominator d, #numerators n
UNION ALL
SELECT
  n.metric_type,
  'total_person_months' AS metric_name,
  d.total_person_months AS metric_value
FROM #denominator d, #numerators n
UNION ALL
SELECT
  n.metric_type,
  'total_person_quarters' AS metric_name,
  d.total_person_quarters AS metric_value
FROM #denominator d, #numerators n
UNION ALL
SELECT
  n.metric_type,
  'total_person_years' AS metric_name,
  d.total_person_years AS metric_value
FROM #denominator d, #numerators n
UNION ALL
SELECT
  n.metric_type,
  'total_cost' AS metric_name,
  n.total_cost AS metric_value
FROM #numerators n
UNION ALL
SELECT
  n.metric_type,
  'total_adjusted_cost' AS metric_name,
  n.total_adjusted_cost AS metric_value
FROM #numerators n
UNION ALL
SELECT
  n.metric_type,
  'n_persons_with_cost' AS metric_name,
  n.n_persons_with_cost AS metric_value
FROM #numerators n
UNION ALL
SELECT
  n.metric_type,
  'distinct_visits' AS metric_name,
  n.distinct_visits AS metric_value
FROM #numerators n
UNION ALL
SELECT
  n.metric_type,
  'distinct_events' AS metric_name,
  n.distinct_events AS metric_value
FROM #numerators n
UNION ALL
SELECT
  n.metric_type,
  'cost_pppm' AS metric_name,
  CASE WHEN d.total_person_months > 0 THEN n.total_cost / d.total_person_months ELSE 0 END AS metric_value
FROM #denominator d, #numerators n
UNION ALL
SELECT
  n.metric_type,
  'adjusted_cost_pppm' AS metric_name,
  CASE WHEN d.total_person_months > 0 THEN n.total_adjusted_cost / d.total_person_months ELSE 0 END AS metric_value
FROM #denominator d, #numerators n
UNION ALL
SELECT
  n.metric_type,
  'cost_pppq' AS metric_name,
  CASE WHEN d.total_person_quarters > 0 THEN n.total_cost / d.total_person_quarters ELSE 0 END AS metric_value
FROM #denominator d, #numerators n
UNION ALL
SELECT
  n.metric_type,
  'adjusted_cost_pppq' AS metric_name,
  CASE WHEN d.total_person_quarters > 0 THEN n.total_adjusted_cost / d.total_person_quarters ELSE 0 END AS metric_value
FROM #denominator d, #numerators n
UNION ALL
SELECT
  n.metric_type,
  'cost_pppy' AS metric_name,
  CASE WHEN d.total_person_years > 0 THEN n.total_cost / d.total_person_years ELSE 0 END AS metric_value
FROM #denominator d, #numerators n
UNION ALL
SELECT
  n.metric_type,
  'adjusted_cost_pppy' AS metric_name,
  CASE WHEN d.total_person_years > 0 THEN n.total_adjusted_cost / d.total_person_years ELSE 0 END AS metric_value
FROM #denominator d, #numerators n
UNION ALL
SELECT
  n.metric_type,
  'events_per_1000_py' AS metric_name,
  CASE WHEN d.total_person_years > 0 THEN (n.distinct_events * 1000.0) / d.total_person_years ELSE 0 END AS metric_value
FROM #denominator d, #numerators n;
} : {
-- 3.2) Aggregate cost to person level
DROP TABLE IF EXISTS #temp_results_table;
CREATE TABLE #temp_results_table (
    person_id       BIGINT NOT NULL PRIMARY KEY,
    cost            DECIMAL(19,4) NOT NULL
    {@cpi_adjustment} ? { , adjusted_cost DECIMAL(19,4) NOT NULL } : { }
);

{@micro_costing} ? {
    INSERT INTO #temp_results_table (person_id, cost {@cpi_adjustment} ? { , adjusted_cost } : { })
    SELECT
        qd.person_id,
        SUM(cr.cost)          AS cost
        {@cpi_adjustment} ? { , SUM(cr.adjusted_cost) AS adjusted_cost} : { }
    FROM #primary_filter_details qd
    JOIN #costs_raw cr
        ON cr.person_id = qd.person_id
       AND cr.visit_detail_id = qd.visit_detail_id
    GROUP BY qd.person_id;
} : {
    INSERT INTO #temp_results_table (person_id, cost {@cpi_adjustment} ? { , adjusted_cost } : { })
    SELECT
        qv.person_id,
        SUM(cr.cost)          AS cost
        {@cpi_adjustment} ? { , SUM(cr.adjusted_cost) AS adjusted_cost} : { }
    FROM #qualifying_visits qv
    JOIN #costs_raw cr
        ON cr.person_id = qv.person_id
       AND cr.visit_occurrence_id = qv.visit_occurrence_id
    GROUP BY qv.person_id;
};
}

/* ============================================================================
   SECTION 5: CLEANUP
   ============================================================================ */
DROP TABLE IF EXISTS #cohort_person;
DROP TABLE IF EXISTS #analysis_window;
DROP TABLE IF EXISTS #analysis_window_clean;
DROP TABLE IF EXISTS #person_time;
DROP TABLE IF EXISTS #visits_in_window;
DROP TABLE IF EXISTS #qualifying_visits;

{@has_event_filters} ? {
  DROP TABLE IF EXISTS #events_by_filter;
  DROP TABLE IF EXISTS #event_visits;
  {@micro_costing} ? { DROP TABLE IF EXISTS #primary_filter_details; }
}

{@aggregated} ? {
DROP TABLE IF EXISTS #costs_raw;
{@micro_costing} ? { DROP TABLE IF EXISTS #line_level_cost; } : { DROP TABLE IF EXISTS #visit_level_cost; }
DROP TABLE IF EXISTS #denominator;
DROP TABLE IF EXISTS #numerators;
}
-- Final diagnostic
INSERT INTO #diag_table (step_name, n_persons, n_events)
VALUES ('99_completed', NULL, NULL);

