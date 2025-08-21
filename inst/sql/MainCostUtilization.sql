-- ============================================================================
-- Healthcare Cost Analysis Query
-- Purpose: Calculate per-member-per-month costs and event rates for a cohort
-- ============================================================================

-- 0) Initialize diagnostics table
DROP TABLE IF EXISTS @diag_table;
CREATE TABLE @diag_table (
    step_name VARCHAR(255), 
    n_persons BIGINT, 
    n_events BIGINT,
    execution_time DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO @diag_table (step_name, n_persons)
SELECT 
    '00_initial_cohort' AS step_name,
    COUNT(DISTINCT subject_id) AS n_persons
FROM @cohort_database_schema.@cohort_table
WHERE cohort_definition_id = @cohort_id;

-- ============================================================================
-- SECTION 1: COHORT AND ANALYSIS WINDOW SETUP
-- ============================================================================

-- 1.1) Extract cohort members
DROP TABLE IF EXISTS #cohort_person;
CREATE TABLE #cohort_person (
    person_id BIGINT NOT NULL,
    cohort_start_date DATE NOT NULL,
    cohort_end_date DATE
);

INSERT INTO #cohort_person
SELECT 
    c.subject_id AS person_id, 
    c.cohort_start_date, 
    c.cohort_end_date
FROM @cohort_database_schema.@cohort_table c
WHERE c.cohort_definition_id = @cohort_id;

-- 1.2) Define analysis windows based on observation periods
DROP TABLE IF EXISTS #analysis_window;
CREATE TABLE #analysis_window (
    person_id BIGINT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

INSERT INTO #analysis_window
SELECT
    cp.person_id,
    GREATEST(
        op.observation_period_start_date,
        DATEADD(day, @time_a, cp.@anchor_col)
    ) AS start_date,
    LEAST(
        op.observation_period_end_date,
        DATEADD(day, @time_b, cp.@anchor_col)
    ) AS end_date
FROM #cohort_person cp
INNER JOIN @cdm_database_schema.observation_period op 
    ON op.person_id = cp.person_id
    AND op.observation_period_start_date <= DATEADD(day, @time_b, cp.@anchor_col)
    AND op.observation_period_end_date >= DATEADD(day, @time_a, cp.@anchor_col);

-- 1.3) Filter to valid windows and calculate person-time
DROP TABLE IF EXISTS #analysis_window_clean;
CREATE TABLE #analysis_window_clean (
    person_id BIGINT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    person_days INT
);

INSERT INTO #analysis_window_clean
SELECT 
    person_id,
    start_date,
    end_date,
    DATEDIFF(day, start_date, end_date) + 1 AS person_days
FROM #analysis_window 
WHERE end_date >= start_date;

-- 1.4) Aggregate person-time by individual
DROP TABLE IF EXISTS #person_time;
CREATE TABLE #person_time (
    person_id BIGINT NOT NULL PRIMARY KEY,
    person_days INT NOT NULL
);

INSERT INTO #person_time
SELECT 
    person_id, 
    SUM(person_days) AS person_days
FROM #analysis_window_clean 
GROUP BY person_id;

-- Log diagnostics
INSERT INTO @diag_table (step_name, n_persons)
VALUES 
    ('01_person_subset', (SELECT COUNT(DISTINCT person_id) FROM #cohort_person)),
    ('02_valid_window', (SELECT COUNT(DISTINCT person_id) FROM #analysis_window_clean));

-- ============================================================================
-- SECTION 2: IDENTIFY QUALIFYING VISITS
-- ============================================================================

-- 2.1) Find all visits within analysis windows
DROP TABLE IF EXISTS #visits_in_window;
CREATE TABLE #visits_in_window (
    person_id BIGINT NOT NULL,
    visit_occurrence_id BIGINT NOT NULL,
    visit_start_date DATE NOT NULL,
    visit_concept_id INT
);

INSERT INTO #visits_in_window
SELECT 
    vo.person_id, 
    vo.visit_occurrence_id, 
    vo.visit_start_date, 
    vo.visit_concept_id
FROM @cdm_database_schema.visit_occurrence vo
INNER JOIN #analysis_window_clean aw 
    ON aw.person_id = vo.person_id
    AND vo.visit_start_date BETWEEN aw.start_date AND aw.end_date
{@has_visit_restriction} ? {
    INNER JOIN @restrict_visit_table rvt 
        ON rvt.visit_concept_id = vo.visit_concept_id
};

-- 2.2) Apply event filters if specified
{@has_event_filters} ? {
    DROP TABLE IF EXISTS #events_by_filter;
    CREATE TABLE #events_by_filter (
        filter_id INT NOT NULL,
        filter_name VARCHAR(255),
        person_id BIGINT NOT NULL,
        visit_occurrence_id BIGINT,
        visit_detail_id BIGINT
    );

    -- Efficiently collect events across all domains
    INSERT INTO #events_by_filter
    SELECT 
        ec.filter_id, 
        ec.filter_name, 
        de.person_id, 
        de.visit_occurrence_id, 
        de.visit_detail_id
    FROM @cdm_database_schema.drug_exposure de
    INNER JOIN @event_concepts_table ec 
        ON ec.concept_id = de.drug_concept_id 
        AND ec.domain_scope IN ('All', 'Drug')
    
    UNION ALL
    
    SELECT 
        ec.filter_id, 
        ec.filter_name, 
        po.person_id, 
        po.visit_occurrence_id, 
        po.visit_detail_id
    FROM @cdm_database_schema.procedure_occurrence po
    INNER JOIN @event_concepts_table ec 
        ON ec.concept_id = po.procedure_concept_id 
        AND ec.domain_scope IN ('All', 'Procedure')
    
    UNION ALL
    
    SELECT 
        ec.filter_id, 
        ec.filter_name, 
        co.person_id, 
        co.visit_occurrence_id, 
        NULL AS visit_detail_id
    FROM @cdm_database_schema.condition_occurrence co
    INNER JOIN @event_concepts_table ec 
        ON ec.concept_id = co.condition_concept_id 
        AND ec.domain_scope IN ('All', 'Condition')
    
    UNION ALL
    
    SELECT 
        ec.filter_id, 
        ec.filter_name, 
        ms.person_id, 
        ms.visit_occurrence_id, 
        ms.visit_detail_id
    FROM @cdm_database_schema.measurement ms
    INNER JOIN @event_concepts_table ec 
        ON ec.concept_id = ms.measurement_concept_id 
        AND ec.domain_scope IN ('All', 'Measurement')
    
    UNION ALL
    
    SELECT 
        ec.filter_id, 
        ec.filter_name, 
        ob.person_id, 
        ob.visit_occurrence_id, 
        ob.visit_detail_id
    FROM @cdm_database_schema.observation ob
    INNER JOIN @event_concepts_table ec 
        ON ec.concept_id = ob.observation_concept_id 
        AND ec.domain_scope IN ('All', 'Observation');

    -- Identify visits meeting filter criteria
    DROP TABLE IF EXISTS #event_visits;
    CREATE TABLE #event_visits (
        person_id BIGINT NOT NULL,
        visit_occurrence_id BIGINT NOT NULL
    );

    INSERT INTO #event_visits
    SELECT 
        person_id, 
        visit_occurrence_id
    FROM #events_by_filter
    WHERE visit_occurrence_id IS NOT NULL
    GROUP BY person_id, visit_occurrence_id
    HAVING COUNT(DISTINCT filter_id) >= @n_filters;

    -- Create final qualifying visits table
    DROP TABLE IF EXISTS #qualifying_visits;
    CREATE TABLE #qualifying_visits AS
    SELECT v.*
    FROM #visits_in_window v
    INNER JOIN #event_visits ev 
        ON ev.person_id = v.person_id 
        AND ev.visit_occurrence_id = v.visit_occurrence_id;

    {@micro_costing} ? {
        DROP TABLE IF EXISTS #primary_filter_details;
        CREATE TABLE #primary_filter_details (
            person_id BIGINT NOT NULL,
            visit_occurrence_id BIGINT NOT NULL,
            visit_detail_id BIGINT NOT NULL
        );

        INSERT INTO #primary_filter_details
        SELECT DISTINCT 
            person_id, 
            visit_occurrence_id, 
            visit_detail_id
        FROM #events_by_filter
        WHERE filter_id = @primary_filter_id 
            AND visit_detail_id IS NOT NULL;
    }
} : {
    DROP TABLE IF EXISTS #qualifying_visits;
    CREATE TABLE #qualifying_visits AS
    SELECT * FROM #visits_in_window;
}

-- Log diagnostics
INSERT INTO @diag_table (step_name, n_persons, n_events)
SELECT
    '03_with_qualifying_visits' AS step_name,
    COUNT(DISTINCT person_id) AS n_persons,
    COUNT(DISTINCT visit_occurrence_id) AS n_events
FROM #qualifying_visits;

-- ============================================================================
-- SECTION 3: COST CALCULATION
-- ============================================================================

-- 3.1) Extract and adjust costs from OMOP CDM v5.5 cost table
DROP TABLE IF EXISTS #costs_raw;
CREATE TABLE #costs_raw (
    cost_id BIGINT NOT NULL,
    person_id BIGINT NOT NULL,
    cost_event_id BIGINT NOT NULL,
    cost_domain_id VARCHAR(20),
    cost DECIMAL(19, 4),
    adjusted_cost DECIMAL(19, 4),
    incurred_date DATE
);

INSERT INTO #costs_raw
SELECT
    c.cost_id,
    c.person_id,
    c.cost_event_id,
    c.cost_domain_id,
    c.cost,
    {@cpi_adjustment} ? {
        c.cost * COALESCE(cpi.adj_factor, 1.0) AS adjusted_cost
    } : {
        c.cost AS adjusted_cost
    },
    c.incurred_date
FROM @cdm_database_schema.cost c
{@cpi_adjustment} ? {
    LEFT JOIN @cpi_adj_table cpi
        ON cpi.year = YEAR(c.incurred_date)
}
WHERE c.cost_concept_id = @cost_concept_id
    AND c.currency_concept_id = @currency_concept_id
    AND c.cost_domain_id = 'visit'
    AND c.cost IS NOT NULL;

-- 3.2) Aggregate costs at appropriate level
{@micro_costing} ? {
    DROP TABLE IF EXISTS #line_level_cost;
    CREATE TABLE #line_level_cost (
        person_id BIGINT NOT NULL,
        visit_occurrence_id BIGINT NOT NULL,
        visit_detail_start_date DATE,
        visit_detail_id BIGINT NOT NULL,
        cost DECIMAL(19, 4),
        adjusted_cost DECIMAL(19, 4)
    );

    -- For micro-costing, get visit detail costs from the cost table
    INSERT INTO #line_level_cost
    SELECT
        qd.person_id,
        qd.visit_occurrence_id,
        vd.visit_detail_start_date,
        qd.visit_detail_id,
        c.cost,
        c.adjusted_cost
    FROM #primary_filter_details qd
    INNER JOIN @cdm_database_schema.visit_detail vd 
        ON vd.visit_detail_id = qd.visit_detail_id
    INNER JOIN @cdm_database_schema.cost c
        ON c.cost_event_id = qd.visit_detail_id 
        AND c.person_id = qd.person_id
        AND c.cost_domain_id = 'visit_detail'
        AND c.cost_concept_id = @cost_concept_id
        AND c.currency_concept_id = @currency_concept_id
        AND c.cost IS NOT NULL
    {@cpi_adjustment} ? {
        LEFT JOIN @cpi_adj_table cpi
            ON cpi.year = YEAR(c.incurred_date)
    };
} : {
    DROP TABLE IF EXISTS #visit_level_cost;
    CREATE TABLE #visit_level_cost (
        person_id BIGINT NOT NULL,
        visit_occurrence_id BIGINT NOT NULL,
        visit_start_date DATE,
        cost DECIMAL(19, 4),
        adjusted_cost DECIMAL(19, 4)
    );

    -- Join qualifying visits with costs using visit_occurrence_id
    INSERT INTO #visit_level_cost
    SELECT 
        qv.person_id, 
        qv.visit_occurrence_id, 
        qv.visit_start_date,
        SUM(c.cost) AS cost,
        SUM(c.adjusted_cost) AS adjusted_cost
    FROM #qualifying_visits qv
    INNER JOIN #costs_raw c
        ON c.cost_event_id = qv.visit_occurrence_id
        AND c.person_id = qv.person_id
    GROUP BY qv.person_id, qv.visit_occurrence_id, qv.visit_start_date;
}

-- Log diagnostics
{@micro_costing} ? {
    INSERT INTO @diag_table (step_name, n_persons, n_events)
    SELECT
        '04_with_cost' AS step_name,
        COUNT(DISTINCT person_id) AS n_persons,
        COUNT(DISTINCT visit_detail_id) AS n_events
    FROM #line_level_cost;
} : {
    INSERT INTO @diag_table (step_name, n_persons, n_events)
    SELECT
        '04_with_cost' AS step_name,
        COUNT(DISTINCT person_id) AS n_persons,
        COUNT(DISTINCT visit_occurrence_id) AS n_events
    FROM #visit_level_cost;
}

-- ============================================================================
-- SECTION 4: FINAL CALCULATIONS
-- ============================================================================

-- 4.1) Calculate denominator (total person-time)
DROP TABLE IF EXISTS #denominator;
CREATE TABLE #denominator (
    total_person_days BIGINT,
    total_person_months DECIMAL(19, 4),
    total_person_years DECIMAL(19, 4)
);

INSERT INTO #denominator
SELECT
    SUM(pt.person_days) AS total_person_days,
    SUM(pt.person_days) / 30.4375 AS total_person_months,
    SUM(pt.person_days) / 365.25 AS total_person_years
FROM #person_time pt
WHERE EXISTS (
    SELECT 1 FROM #analysis_window_clean awc 
    WHERE awc.person_id = pt.person_id
);

-- 4.2) Calculate numerators
DROP TABLE IF EXISTS #numerators;
CREATE TABLE #numerators (
    metric_type VARCHAR(50),
    total_cost DECIMAL(19, 4),
    total_adjusted_cost DECIMAL(19, 4),
    n_persons_with_cost BIGINT,
    distinct_visits BIGINT,
    distinct_events BIGINT
);

{@micro_costing} ? {
    INSERT INTO #numerators
    SELECT
        'line_level' AS metric_type,
        SUM(cost) AS total_cost,
        SUM(adjusted_cost) AS total_adjusted_cost,
        COUNT(DISTINCT person_id) AS n_persons_with_cost,
        -1 AS distinct_visits,
        COUNT(DISTINCT visit_detail_id) AS distinct_events
    FROM #line_level_cost;
} : {
    INSERT INTO #numerators
    SELECT
        'visit_level' AS metric_type,
        SUM(cost) AS total_cost,
        SUM(adjusted_cost) AS total_adjusted_cost,
        COUNT(DISTINCT person_id) AS n_persons_with_cost,
        COUNT(DISTINCT visit_occurrence_id) AS distinct_visits,
        COUNT(DISTINCT visit_occurrence_id) AS distinct_events
    FROM #visit_level_cost;
}

-- 4.3) Generate final results
DROP TABLE IF EXISTS @results_table;
CREATE TABLE @results_table (
    total_person_days BIGINT,
    total_person_months DECIMAL(19, 4),
    total_person_years DECIMAL(19, 4),
    metric_type VARCHAR(50),
    total_cost DECIMAL(19, 4),
    total_adjusted_cost DECIMAL(19, 4),
    n_persons_with_cost BIGINT,
    distinct_visits BIGINT,
    distinct_events BIGINT,
    cost_pppm DECIMAL(19, 4),
    adjusted_cost_pppm DECIMAL(19, 4),
    events_per_1000_py DECIMAL(19, 4),
    calculation_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO @results_table
SELECT
    d.total_person_days,
    d.total_person_months,
    d.total_person_years,
    n.metric_type,
    n.total_cost,
    n.total_adjusted_cost,
    n.n_persons_with_cost,
    n.distinct_visits,
    n.distinct_events,
    -- Calculate per-member-per-month costs with safety checks
    CASE 
        WHEN d.total_person_months > 0 
        THEN n.total_cost / d.total_person_months 
        ELSE 0 
    END AS cost_pppm,
    CASE 
        WHEN d.total_person_months > 0 
        THEN n.total_adjusted_cost / d.total_person_months 
        ELSE 0 
    END AS adjusted_cost_pppm,
    -- Calculate event rate per 1000 person-years
    CASE 
        WHEN d.total_person_years > 0 
        THEN (n.distinct_events * 1000.0) / d.total_person_years 
        ELSE 0 
    END AS events_per_1000_py,
    CURRENT_TIMESTAMP AS calculation_date
FROM #denominator d
CROSS JOIN #numerators n;

-- ============================================================================
-- SECTION 5: CLEANUP
-- ============================================================================

-- Drop all temporary tables
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
    {@micro_costing} ? {
        DROP TABLE IF EXISTS #primary_filter_details;
    }
}

{@micro_costing} ? {
    DROP TABLE IF EXISTS #line_level_cost;
} : {
    DROP TABLE IF EXISTS #visit_level_cost;
}

DROP TABLE IF EXISTS #denominator;
DROP TABLE IF EXISTS #numerators;

-- Log completion
INSERT INTO @diag_table (step_name, n_persons)
VALUES ('99_completed', NULL);