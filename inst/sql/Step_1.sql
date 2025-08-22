/* ============================================================================
 Part 1 — FEATURE EXTRACTION (Non-aggregated)
 Purpose: produce reusable atomic features + person-time
 Inputs:
   - @cdm_database_schema
   - @cohort_database_schema, @cohort_table, @cohort_id
   - @anchor_on_end (0/1), @time_a, @time_b
   - @has_visit_restriction, @restrict_visit_table
   - @has_event_filters, @event_concepts_table, @n_filters, @primary_filter_id
   - @micro_costing (0/1)
   - @cpi_adjustment (0/1), @cpi_adj_table
   - @cost_concept_id, @currency_concept_id, @cost_type_concept_id (nullable filters)
   - @work_schema (schema where feature tables live)
 Outputs (persisted, overwrite each run):
   - @work_schema.fe_person_time(person_id, person_days)
   - @work_schema.fe_events(person_id, event_id, event_date, event_type, visit_occurrence_id,
                            visit_detail_id, cost, adjusted_cost)
============================================================================ */

/* 0) Optional: fresh diag staging for Part 1 only (Part 2 will also log) */
DROP TABLE IF EXISTS @diag_table;
CREATE TABLE @diag_table (
  step_name   VARCHAR(255),
  n_persons   BIGINT,
  n_events    BIGINT
);

INSERT INTO @diag_table (step_name, n_persons, n_events)
SELECT '00_initial_cohort', COUNT(DISTINCT subject_id), NULL
FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id = @cohort_id;

/* 1) Cohort + anchor */
DROP TABLE IF EXISTS #cohort_person;
CREATE TABLE #cohort_person (
  person_id         BIGINT NOT NULL,
  cohort_start_date DATE   NOT NULL,
  cohort_end_date   DATE   NULL,
  anchor_date       DATE   NOT NULL
);

INSERT INTO #cohort_person
SELECT
  c.subject_id,
  c.cohort_start_date,
  c.cohort_end_date,
  CASE WHEN @anchor_on_end = 1 THEN c.cohort_end_date ELSE c.cohort_start_date END
FROM @cohort_database_schema.@cohort_table c
WHERE c.cohort_definition_id = @cohort_id;

INSERT INTO @diag_table
SELECT '01_person_subset', COUNT(DISTINCT person_id), NULL FROM #cohort_person;

/* 2) Analysis windows clipped to OP */
DROP TABLE IF EXISTS #analysis_window;
CREATE TABLE #analysis_window (
  person_id  BIGINT NOT NULL,
  start_date DATE   NOT NULL,
  end_date   DATE   NOT NULL
);

INSERT INTO #analysis_window
SELECT
  cp.person_id,
  CASE WHEN op.observation_period_start_date > DATEADD(day, @time_a, cp.anchor_date)
       THEN op.observation_period_start_date ELSE DATEADD(day, @time_a, cp.anchor_date) END,
  CASE WHEN op.observation_period_end_date   < DATEADD(day, @time_b, cp.anchor_date)
       THEN op.observation_period_end_date   ELSE DATEADD(day, @time_b, cp.anchor_date) END
FROM #cohort_person cp
JOIN @cdm_database_schema.observation_period op
  ON op.person_id = cp.person_id
WHERE op.observation_period_start_date <= DATEADD(day, @time_b, cp.anchor_date)
  AND op.observation_period_end_date   >= DATEADD(day, @time_a, cp.anchor_date);

DROP TABLE IF EXISTS #analysis_window_clean;
CREATE TABLE #analysis_window_clean (
  person_id   BIGINT NOT NULL,
  start_date  DATE   NOT NULL,
  end_date    DATE   NOT NULL,
  person_days INT    NOT NULL
);

INSERT INTO #analysis_window_clean
SELECT person_id, start_date, end_date, DATEDIFF(day, start_date, end_date) + 1
FROM #analysis_window
WHERE end_date >= start_date;

INSERT INTO @diag_table
SELECT '02_valid_window', COUNT(DISTINCT person_id), NULL FROM #analysis_window_clean;

/* 3) Visits overlapping analysis windows (+ optional restriction) */
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

/* 4) Optional event filters → qualifying visits */
{@has_event_filters} ? {

  DROP TABLE IF EXISTS #events_by_filter;
  CREATE TABLE #events_by_filter (
    filter_id           INT          NOT NULL,
    filter_name         VARCHAR(255) NULL,
    person_id           BIGINT       NOT NULL,
    visit_occurrence_id BIGINT       NULL,
    visit_detail_id     BIGINT       NULL
  );

  INSERT INTO #events_by_filter
  SELECT ec.filter_id, ec.filter_name, de.person_id, de.visit_occurrence_id, de.visit_detail_id
  FROM @cdm_database_schema.drug_exposure de
  JOIN @event_concepts_table ec
    ON ec.concept_id = de.drug_concept_id
   AND ec.domain_scope IN ('All','Drug')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, po.person_id, po.visit_occurrence_id, po.visit_detail_id
  FROM @cdm_database_schema.procedure_occurrence po
  JOIN @event_concepts_table ec
    ON ec.concept_id = po.procedure_concept_id
   AND ec.domain_scope IN ('All','Procedure')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, co.person_id, co.visit_occurrence_id, NULL
  FROM @cdm_database_schema.condition_occurrence co
  JOIN @event_concepts_table ec
    ON ec.concept_id = co.condition_concept_id
   AND ec.domain_scope IN ('All','Condition')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, ms.person_id, ms.visit_occurrence_id, ms.visit_detail_id
  FROM @cdm_database_schema.measurement ms
  JOIN @event_concepts_table ec
    ON ec.concept_id = ms.measurement_concept_id
   AND ec.domain_scope IN ('All','Measurement')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, ob.person_id, ob.visit_occurrence_id, ob.visit_detail_id
  FROM @cdm_database_schema.observation ob
  JOIN @event_concepts_table ec
    ON ec.concept_id = ob.observation_concept_id
   AND ec.domain_scope IN ('All','Observation');

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
    ON ev.person_id = v.person_id AND ev.visit_occurrence_id = v.visit_occurrence_id;

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
  SELECT * INTO #qualifying_visits FROM #visits_in_window;
};

INSERT INTO @diag_table
SELECT '03_with_qualifying_visits',
       COUNT(DISTINCT person_id),
       COUNT(DISTINCT visit_occurrence_id)
FROM #qualifying_visits;

/* 5) Costs raw (+ CPI adj + filters) */
DROP TABLE IF EXISTS #costs_raw;
CREATE TABLE #costs_raw (
  person_id            BIGINT NOT NULL,
  visit_occurrence_id  BIGINT NULL,
  visit_detail_id      BIGINT NULL,
  cost                 DECIMAL(19,4) NULL,
  adjusted_cost        DECIMAL(19,4) NULL,
  cost_date            DATE   NULL,
  currency_concept_id  INT    NULL,
  cost_concept_id      INT    NULL,
  cost_type_concept_id INT    NULL
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
  c.cost_concept_id,
  c.cost_type_concept_id
FROM @cdm_database_schema.cost c
{@cpi_adjustment} ? {
  LEFT JOIN @cpi_adj_table cpi
    ON cpi.year = YEAR(COALESCE(c.incurred_date, c.paid_date, c.billed_date, c.effective_date))
}
WHERE (@cost_concept_id       IS NULL OR c.cost_concept_id      = @cost_concept_id)
  AND (@currency_concept_id   IS NULL OR c.currency_concept_id  = @currency_concept_id)
  AND (@cost_type_concept_id  IS NULL OR c.cost_type_concept_id = @cost_type_concept_id)
  AND c.cost IS NOT NULL;

/* 6) Build the FEATURE tables */

/* 6a) Person-time feature (persisted) */
DROP TABLE IF EXISTS #person_time;
CREATE TABLE #person_time (
  person_id   BIGINT NOT NULL PRIMARY KEY,
  person_days INT    NOT NULL
);

INSERT INTO #person_time
SELECT person_id, SUM(person_days)
FROM #analysis_window_clean
GROUP BY person_id;

DROP TABLE IF EXISTS @work_schema.fe_person_time;
SELECT person_id, person_days
INTO @work_schema.fe_person_time
FROM #person_time;

/* 6b) Event feature (persisted): one row per atomic event */
{@micro_costing} ? {

  /* event grain = visit_detail lines linked by primary filter */
  DROP TABLE IF EXISTS #line_level_cost;
  SELECT
    qd.person_id,
    qd.visit_occurrence_id,
    qd.visit_detail_id,
    vd.visit_detail_start_date AS event_date,
    SUM(cr.cost)               AS cost,
    SUM(cr.adjusted_cost)      AS adjusted_cost
  INTO #line_level_cost
  FROM #primary_filter_details qd
  JOIN @cdm_database_schema.visit_detail vd
    ON vd.visit_detail_id = qd.visit_detail_id
  JOIN #costs_raw cr
    ON cr.person_id = qd.person_id
   AND cr.visit_detail_id = qd.visit_detail_id
  GROUP BY qd.person_id, qd.visit_occurrence_id, qd.visit_detail_id, vd.visit_detail_start_date;

  DROP TABLE IF EXISTS @work_schema.fe_events;
  SELECT
    llc.person_id,
    /* event_id: stable surrogate for re-aggregation */
    CAST(llc.visit_detail_id AS BIGINT)           AS event_id,
    llc.event_date,
    CAST('line_level' AS VARCHAR(20))             AS event_type,
    llc.visit_occurrence_id,
    llc.visit_detail_id,
    llc.cost,
    llc.adjusted_cost
  INTO @work_schema.fe_events
  FROM #line_level_cost llc;

  INSERT INTO @diag_table
  SELECT '04_with_cost', COUNT(DISTINCT person_id), COUNT(DISTINCT visit_detail_id)
  FROM #line_level_cost;

} : {

  /* event grain = visit-level costs across qualifying visits */
  DROP TABLE IF EXISTS #visit_level_cost;
  SELECT
    qv.person_id,
    qv.visit_occurrence_id,
    qv.visit_start_date       AS event_date,
    SUM(cr.cost)              AS cost,
    SUM(cr.adjusted_cost)     AS adjusted_cost
  INTO #visit_level_cost
  FROM #qualifying_visits qv
  JOIN #costs_raw cr
    ON cr.person_id = qv.person_id
   AND cr.visit_occurrence_id = qv.visit_occurrence_id
  GROUP BY qv.person_id, qv.visit_occurrence_id, qv.visit_start_date;

  DROP TABLE IF EXISTS @work_schema.fe_events;
  SELECT
    vlc.person_id,
    CAST(vlc.visit_occurrence_id AS BIGINT)       AS event_id,
    vlc.event_date,
    CAST('visit_level' AS VARCHAR(20))            AS event_type,
    vlc.visit_occurrence_id,
    CAST(NULL AS BIGINT)                          AS visit_detail_id,
    vlc.cost,
    vlc.adjusted_cost
  INTO @work_schema.fe_events
  FROM #visit_level_cost vlc;

  INSERT INTO @diag_table
  SELECT '04_with_cost', COUNT(DISTINCT person_id), COUNT(DISTINCT visit_occurrence_id)
  FROM #visit_level_cost;
}

/* 7) Cleanup (local temps only) */
DROP TABLE IF EXISTS #cohort_person;
DROP TABLE IF EXISTS #analysis_window;
DROP TABLE IF EXISTS #analysis_window_clean;
DROP TABLE IF EXISTS #person_time;
DROP TABLE IF EXISTS #visits_in_window;
DROP TABLE IF EXISTS #qualifying_visits;
DROP TABLE IF EXISTS #costs_raw;
{@has_event_filters} ? {
  DROP TABLE IF EXISTS #events_by_filter;
  DROP TABLE IF EXISTS #event_visits;
  {@micro_costing} ? { DROP TABLE IF EXISTS #primary_filter_details; }
}
{@micro_costing} ? { DROP TABLE IF EXISTS #line_level_cost; } : { DROP TABLE IF EXISTS #visit_level_cost; }

INSERT INTO @diag_table VALUES ('19_part1_completed', NULL, NULL);

/* (Optional) Helpful indexes on feature tables */
-- CREATE INDEX IX_fe_pt_person        ON @work_schema.fe_person_time(person_id);
-- CREATE INDEX IX_fe_events_person    ON @work_schema.fe_events(person_id, event_date);
-- CREATE INDEX IX_fe_events_visit     ON @work_schema.fe_events(visit_occurrence_id, visit_detail_id);
