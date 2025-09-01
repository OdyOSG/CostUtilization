WITH source_visits AS (
  SELECT
    v.person_id,
    v.@visit_id_col AS visit_id,
    v.visit_concept_id,
    v.visit_start_date,
    v.visit_end_date
  FROM @cdm_database_schema.@visit_table v
  INNER JOIN @cohort_database_schema.@cohort_table c
    ON v.person_id = c.subject_id
    AND v.visit_start_date >= DATEADD(day, @time_a, c.cohort_start_date)
    AND v.visit_start_date <= DATEADD(day, @time_b, c.cohort_start_date)
  WHERE
    c.cohort_definition_id = @cohort_id
    {@visit_concept_ids != ''} ? {AND v.visit_concept_id IN (@visit_concept_ids)}
),
normalized_dates AS (
  SELECT
    person_id,
    visit_concept_id,
    DATEDIFF(day, CAST(visit_start_date AS DATE), CAST(visit_end_date AS DATE))) + 1 AS los_days
  FROM source_visits
)
SELECT
    person_id, sum(los_days), 0 as visit_concept_id
  FROM normalized_dates group by 1, 3

UNION ALL 

  SELECT
    person_id, sum(los_days), visit_concept_id
  FROM normalized_dates group by 1, 3
  
  ;