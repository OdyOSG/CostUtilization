-- MainCostUtilizationWithCpi.sql
-- Enhanced version with CPI adjustment integrated into SQL

{DEFAULT @cdm_database_schema = 'cdm'}
{DEFAULT @cohort_database_schema = 'results'}
{DEFAULT @cohort_table = 'cohort'}
{DEFAULT @cohort_id = 1}
{DEFAULT @start_offset_days = 0}
{DEFAULT @end_offset_days = 365}
{DEFAULT @has_visit_restriction = FALSE}
{DEFAULT @visit_concept_ids = ''}
{DEFAULT @has_event_filter = FALSE}
{DEFAULT @event_filter_table = ''}
{DEFAULT @micro_costing = FALSE}
{DEFAULT @cpi_adjustment = FALSE}
{DEFAULT @cpi_table = 'cpi_factors'}
{DEFAULT @cpi_target_year = 2023}
{DEFAULT @temp_database_schema = 'temp'}

-- Create cohort person table with analysis window
DROP TABLE IF EXISTS #cohort_person;

SELECT 
    c.subject_id,
    c.cohort_start_date,
    c.cohort_end_date,
    DATEADD(day, @start_offset_days, c.cohort_start_date) AS window_start_date,
    DATEADD(day, @end_offset_days, c.cohort_start_date) AS window_end_date
INTO #cohort_person
FROM @cohort_database_schema.@cohort_table c
WHERE c.cohort_definition_id = @cohort_id;

-- Create analysis window table
DROP TABLE IF EXISTS #analysis_window;

SELECT 
    cp.subject_id,
    cp.window_start_date,
    cp.window_end_date,
    DATEDIFF(day, cp.window_start_date, cp.window_end_date) + 1 AS window_days
INTO #analysis_window
FROM #cohort_person cp;

-- Create CPI factors table if adjustment is enabled
{@cpi_adjustment} ? {
DROP TABLE IF EXISTS #cpi_factors;

SELECT 
    year,
    cpi_value,
    (SELECT cpi_value FROM @temp_database_schema.@cpi_table WHERE year = @cpi_target_year) / cpi_value AS adjustment_factor
INTO #cpi_factors
FROM @temp_database_schema.@cpi_table;

CREATE INDEX idx_cpi_year ON #cpi_factors (year);
}

-- Get visits within analysis window
DROP TABLE IF EXISTS #visits;

SELECT DISTINCT
    vo.person_id,
    vo.visit_occurrence_id,
    vo.visit_concept_id,
    vo.visit_start_date,
    vo.visit_end_date,
    YEAR(vo.visit_start_date) AS visit_year
INTO #visits
FROM @cdm_database_schema.visit_occurrence vo
INNER JOIN #analysis_window aw ON vo.person_id = aw.subject_id
WHERE vo.visit_start_date >= aw.window_start_date
    AND vo.visit_start_date <= aw.window_end_date
{@has_visit_restriction} ? {
    AND vo.visit_concept_id IN (@visit_concept_ids)
}
{@has_event_filter} ? {
    AND EXISTS (
        SELECT 1 
        FROM @temp_database_schema.@event_filter_table ef
        WHERE ef.person_id = vo.person_id
            AND ef.event_date >= vo.visit_start_date
            AND ef.event_date <= COALESCE(vo.visit_end_date, vo.visit_start_date)
    )
};

-- Calculate costs with optional CPI adjustment
{@micro_costing} ? {
-- Micro-costing: aggregate at visit detail level
DROP TABLE IF EXISTS #cost_summary;

SELECT 
    v.person_id,
    v.visit_occurrence_id,
    vd.visit_detail_id,
    c.cost_domain_id,
    c.cost_type_concept_id,
    {@cpi_adjustment} ? {
        SUM(c.total_cost * COALESCE(cpi.adjustment_factor, 1.0)) AS total_cost,
        SUM(c.total_paid * COALESCE(cpi.adjustment_factor, 1.0)) AS total_paid,
        SUM(c.paid_by_payer * COALESCE(cpi.adjustment_factor, 1.0)) AS paid_by_payer,
        SUM(c.paid_by_patient * COALESCE(cpi.adjustment_factor, 1.0)) AS paid_by_patient
    } : {
        SUM(c.total_cost) AS total_cost,
        SUM(c.total_paid) AS total_paid,
        SUM(c.paid_by_payer) AS paid_by_payer,
        SUM(c.paid_by_patient) AS paid_by_patient
    }
INTO #cost_summary
FROM #visits v
INNER JOIN @cdm_database_schema.visit_detail vd ON v.visit_occurrence_id = vd.visit_occurrence_id
INNER JOIN @cdm_database_schema.cost c ON vd.visit_detail_id = c.cost_event_id
    AND c.cost_domain_id = 27 -- Visit detail domain
{@cpi_adjustment} ? {
LEFT JOIN #cpi_factors cpi ON cpi.year = v.visit_year
}
GROUP BY v.person_id, v.visit_occurrence_id, vd.visit_detail_id, c.cost_domain_id, c.cost_type_concept_id;

} : {
-- Standard costing: aggregate at visit level
DROP TABLE IF EXISTS #cost_summary;

SELECT 
    v.person_id,
    v.visit_occurrence_id,
    NULL AS visit_detail_id,
    c.cost_domain_id,
    c.cost_type_concept_id,
    {@cpi_adjustment} ? {
        SUM(c.total_cost * COALESCE(cpi.adjustment_factor, 1.0)) AS total_cost,
        SUM(c.total_paid * COALESCE(cpi.adjustment_factor, 1.0)) AS total_paid,
        SUM(c.paid_by_payer * COALESCE(cpi.adjustment_factor, 1.0)) AS paid_by_payer,
        SUM(c.paid_by_patient * COALESCE(cpi.adjustment_factor, 1.0)) AS paid_by_patient
    } : {
        SUM(c.total_cost) AS total_cost,
        SUM(c.total_paid) AS total_paid,
        SUM(c.paid_by_payer) AS paid_by_payer,
        SUM(c.paid_by_patient) AS paid_by_patient
    }
INTO #cost_summary
FROM #visits v
INNER JOIN @cdm_database_schema.cost c ON v.visit_occurrence_id = c.cost_event_id
    AND c.cost_domain_id = 8 -- Visit occurrence domain
{@cpi_adjustment} ? {
LEFT JOIN #cpi_factors cpi ON cpi.year = v.visit_year
}
GROUP BY v.person_id, v.visit_occurrence_id, c.cost_domain_id, c.cost_type_concept_id;
}

-- Final aggregation by person
DROP TABLE IF EXISTS #person_cost_summary;

SELECT 
    cs.person_id,
    cs.cost_domain_id,
    cs.cost_type_concept_id,
    COUNT(DISTINCT cs.visit_occurrence_id) AS visit_count,
    SUM(cs.total_cost) AS total_cost,
    SUM(cs.total_paid) AS total_paid,
    SUM(cs.paid_by_payer) AS paid_by_payer,
    SUM(cs.paid_by_patient) AS paid_by_patient,
    AVG(cs.total_cost) AS avg_cost_per_visit,
    MIN(cs.total_cost) AS min_cost,
    MAX(cs.total_cost) AS max_cost
INTO #person_cost_summary
FROM #cost_summary cs
GROUP BY cs.person_id, cs.cost_domain_id, cs.cost_type_concept_id;

-- Create final results
SELECT 
    pcs.person_id,
    aw.window_days,
    pcs.cost_domain_id,
    pcs.cost_type_concept_id,
    pcs.visit_count,
    pcs.total_cost,
    pcs.total_paid,
    pcs.paid_by_payer,
    pcs.paid_by_patient,
    pcs.avg_cost_per_visit,
    pcs.min_cost,
    pcs.max_cost,
    pcs.total_cost / CAST(aw.window_days AS FLOAT) * 365.25 AS annualized_cost,
    {@cpi_adjustment} ? {
        @cpi_target_year AS cpi_adjusted_to_year
    } : {
        NULL AS cpi_adjusted_to_year
    }
FROM #person_cost_summary pcs
INNER JOIN #analysis_window aw ON pcs.person_id = aw.subject_id;

-- Clean up
DROP TABLE IF EXISTS #cohort_person;
DROP TABLE IF EXISTS #analysis_window;
DROP TABLE IF EXISTS #visits;
DROP TABLE IF EXISTS #cost_summary;
DROP TABLE IF EXISTS #person_cost_summary;
{@cpi_adjustment} ? {
DROP TABLE IF EXISTS #cpi_factors;
}