WITH
      target_cohorts AS (
        SELECT
          cohort_definition_id, 
          subject_id, 
          cohort_start_date, 
          cohort_end_date
        FROM @cohort_database_schema.@cohort_table
        {@cohortIds == -1} ? {WHERE cohort_definition_id IS NOT NULL} : {WHERE cohort_definition_id IN (@cohortIds)}
      ),
      analysis_windows AS (
      
        SELECT cohort_definition_id, subject_id,
      'Fixed: -365d to 0d' as window_name,
      DATEADD(day, -365, cohort_start_date) as window_start,
      DATEADD(day, 0, cohort_start_date) as window_end
      
FROM target_cohorts
      ),
      cohort_costs AS (
        SELECT
          aw.cohort_definition_id,
          aw.window_name,
          aw.subject_id AS person_id,
          co.cost,
          co.cost_domain_id,
          DATEDIFF(day, aw.window_start, aw.window_end) + 1 AS person_days_in_window
        FROM analysis_windows aw
        JOIN @cdm_database_schema.cost co ON aw.subject_id = co.person_id
        <costJoinClause>
        WHERE
          co.incurred_date >= aw.window_start AND 
          co.incurred_date <= aw.window_end

      ),