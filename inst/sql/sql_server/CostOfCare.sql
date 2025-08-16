-- Technical Guide: Cost of Care Analysis Framework
-- This SQL script implements a multi-scenario framework for cost analysis.

-- Clean up previous temp tables
DROP TABLE IF EXISTS #t_analysis_window;
DROP TABLE IF EXISTS #t_person_time;
DROP TABLE IF EXISTS #t_visit_occurrence_id;
DROP TABLE IF EXISTS #t_cost;
DROP TABLE IF EXISTS #t_denominator;
DROP TABLE IF EXISTS #t_numerators_visit;
DROP TABLE IF EXISTS #t_numerators_detail;
DROP TABLE IF EXISTS #final_results;
-- Event filter temp tables (example for up to 2 filters)
DROP TABLE IF EXISTS #event_concept_id_01;
DROP TABLE IF EXISTS #event_concept_id_02;
DROP TABLE IF EXISTS #t_event_records_01;
DROP TABLE IF EXISTS #t_event_records_02;


--- Step 0: Calculate Person-Time at Risk (Denominator) ---

-- Step 0a: Create a Censored Analysis Window for Each Person
SELECT
    c.subject_id AS person_id,
    CASE
        WHEN op.observation_period_start_date > DATEADD(day, @start_offset, c.@anchor_date)
        THEN op.observation_period_start_date
        ELSE DATEADD(day, @start_offset, c.@anchor_date)
    END AS start_date,
    CASE
        WHEN op.observation_period_end_date < DATEADD(day, @end_offset, c.@anchor_date)
        THEN op.observation_period_end_date
        ELSE DATEADD(day, @end_offset, c.@anchor_date)
    END AS end_date
INTO #t_analysis_window
FROM @cohort_database_schema.@cohort_table c
JOIN @cdm_database_schema.observation_period op ON c.subject_id = op.person_id
WHERE c.cohort_definition_id IN (@cohort_definition_ids)
  AND op.observation_period_start_date <= DATEADD(day, @end_offset, c.@anchor_date)
  AND op.observation_period_end_date >= DATEADD(day, @start_offset, c.@anchor_date);

-- Step 0b: Aggregate Person-Time
SELECT
    person_id,
    SUM(DATEDIFF(day, start_date, end_date) + 1) AS person_days
INTO #t_person_time
FROM #t_analysis_window
GROUP BY person_id;


--- Scenarios 1-5: Identify Qualifying Encounters and Costs ---

-- This section is controlled by SqlRender parameters to handle different scenarios.
-- {@event_filter_logic} -- This placeholder will be replaced by dynamically generated SQL in R for event filters.

-- Step 1: Identify All Encounters Within the Time Window
SELECT
    vo.person_id,
    vo.visit_occurrence_id,
    vo.visit_start_date
INTO #t_visit_occurrence_id
FROM #t_analysis_window taw
JOIN @cdm_database_schema.visit_occurrence vo ON taw.person_id = vo.person_id
WHERE
    vo.visit_start_date >= taw.start_date AND vo.visit_start_date <= taw.end_date
    -- Scenario 2: Filter by Encounter Type
    {@restrict_visit != NULL} ? { AND vo.visit_concept_id IN (@visit_concept_ids) }
    -- Scenarios 3 & 4: Filter by Clinical Events
    {@has_event_filters} ? { AND vo.visit_occurrence_id IN (SELECT visit_occurrence_id FROM #t_filtered_visits) }
;


-- Step 2 & 5: Link Encounters to Costs and Aggregate
-- This block handles both visit-level costing (Scenarios 1-4) and micro-costing (Scenario 5)
{@micro_costing == FALSE} ? {
    -- Scenarios 1-4: Aggregate costs per visit
    SELECT
        t_vo.person_id,
        t_vo.visit_occurrence_id,
        t_vo.visit_start_date,
        SUM(c.total_charge) AS total_cost_per_visit -- Assuming total_charge, adjust as needed
    INTO #t_cost
    FROM #t_visit_occurrence_id t_vo
    JOIN @cdm_database_schema.cost c ON t_vo.visit_occurrence_id = c.cost_event_id AND t_vo.person_id = c.person_id
    WHERE c.cost_concept_id = @cost_concept_id
      AND c.currency_concept_id = @currency_concept_id
    GROUP BY t_vo.person_id, t_vo.visit_occurrence_id, t_vo.visit_start_date;
} : {
    -- Scenario 5: Micro-Costing - Get line-level costs for specific events
    SELECT
        qd.person_id,
        qd.visit_occurrence_id,
        vo.visit_start_date,
        qd.visit_detail_id,
        c.total_charge AS cost_value -- Assuming total_charge
    INTO #t_line_level_cost
    FROM #t_qualifying_details qd -- This table would be created by the event filter logic
    JOIN @cdm_database_schema.visit_occurrence vo ON qd.visit_occurrence_id = vo.visit_occurrence_id
    JOIN @cdm_database_schema.cost c ON qd.visit_detail_id = c.cost_event_id AND qd.person_id = c.person_id
    WHERE c.cost_event_table_concept_id = 5032 -- Cost is for a Visit Detail
      AND c.cost_concept_id = @cost_concept_id
      AND c.currency_concept_id = @currency_concept_id;
}


--- Step 6: Aggregation and Rate Calculation ---

-- Step 6a: Calculate Total Person-Time
SELECT
    SUM(person_days) AS total_person_days,
    SUM(person_days) / 30.44 AS total_person_months,
    SUM(person_days) / 365.25 AS total_person_years
INTO #t_denominator
FROM #t_person_time;

-- Step 6b: Calculate Numerators
{@micro_costing == FALSE} ? {
    SELECT
        SUM(total_cost_per_visit) AS total_cost,
        COUNT(DISTINCT visit_occurrence_id) AS distinct_visits
    INTO #t_numerators_visit
    FROM #t_cost;
} : {
    SELECT
        SUM(cost_value) AS total_line_item_cost,
        COUNT(DISTINCT visit_detail_id) AS distinct_visit_details
    INTO #t_numerators_detail
    FROM #t_line_level_cost;
}

-- Step 6c: Calculate Final Rates
SELECT
    'Overall' as strata,
    {@micro_costing == FALSE} ? {
        n.total_cost / d.total_person_months AS cost_pppm,
        (n.distinct_visits * 1000) / d.total_person_years AS visits_per_1000_py
    } : {
        n.total_line_item_cost / d.total_person_months AS line_item_cost_pppm,
        (n.distinct_visit_details * 1000) / d.total_person_years AS visit_details_per_1000_py
    }
INTO #final_results
FROM
    {@micro_costing == FALSE} ? { #t_numerators_visit n } : { #t_numerators_detail n },
    #t_denominator d;

-- TODO: Add logic for stratification based on @stratification_columns

-- Final cleanup
DROP TABLE IF EXISTS #t_analysis_window;
DROP TABLE IF EXISTS #t_person_time;
DROP TABLE IF EXISTS #t_visit_occurrence_id;
DROP TABLE IF EXISTS #t_cost;
DROP TABLE IF EXISTS #t_denominator;
DROP TABLE IF EXISTS #t_numerators_visit;
DROP TABLE IF EXISTS #t_numerators_detail;
-- Event filter temp tables
DROP TABLE IF EXISTS #event_concept_id_01;
DROP TABLE IF EXISTS #event_concept_id_02;
DROP TABLE IF EXISTS #t_event_records_01;
DROP TABLE IF EXISTS #t_event_records_02;
