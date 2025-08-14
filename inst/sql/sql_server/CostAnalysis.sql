

WITH
  -- Step 1: Select the cohort of interest
  target_cohorts AS (
    SELECT
      cohort_definition_id,
      subject_id,
      cohort_start_date,
      cohort_end_date
    FROM @cohort_database_schema.@cohort_table
    WHERE
      @cohort_definition_id IN (@concept_ids)
  ),
  -- Step 2: Join Costs to Cohort members within the cohort time window
  cohort_costs AS (
    SELECT
      c.cohort_definition_id,
      c.subject_id AS person_id,
      c.cohort_start_date,
      c.cohort_end_date,
      co.cost_id,
      co.cost,
      co.currency_concept_id,
      co.incurred_date,
      co.cost_concept_id,
      co.cost_source_value,
      co.cost_event_field_concept_id
      co.cost_event_id,
      co.visit_occurrence_id
    FROM target_cohort AS c
    JOIN @cdm_database_schema.cost AS co
      ON c.subject_id = co.person_id
    WHERE --- here windows logic