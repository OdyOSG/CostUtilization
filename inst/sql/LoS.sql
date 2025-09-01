WITH source_visits AS (
  SELECT
    v.person_id,
    v.@visit_id_col AS visit_id,
    v.visit_concept_id,
    v.visit_start_date,
    v.visit_end_date,
    c.cohort_end_date
  FROM @cdm_database_schema.@visit_table v
  INNER JOIN @cohort_database_schema.@cohort_table c
    ON v.person_id = c.subject_id
    -- Dynamically select anchor date for the window
    AND v.visit_start_date >= DATEADD(day, @time_a, CASE WHEN @use_cohort_end_date = 1 THEN c.cohort_end_date ELSE c.cohort_start_date END)
    AND v.visit_start_date <= DATEADD(day, @time_b, CASE WHEN @use_cohort_end_date = 1 THEN c.cohort_end_date ELSE c.cohort_start_date END)
  WHERE
    c.cohort_definition_id = @cohort_id
    {@visit_concept_ids != ''} ? {AND v.visit_concept_id IN (@visit_concept_ids)}
),
normalized_dates AS (
  SELECT
    person_id,
    visit_concept_id,
    DATEDIFF(
      day,
      CAST(visit_start_date AS DATE),
      -- Conditionally select the earlier of visit_end_date or cohort_end_date for censoring
      CAST(
        {@censor_on_cohort_end_date == 1} ? {
          CASE
            WHEN visit_end_date > cohort_end_date THEN cohort_end_date
            ELSE visit_end_date
          END
        } : {
          visit_end_date
        }
      AS DATE)
    ) + 1 AS los_days
  FROM source_visits
)
SELECT
  person_id,
  SUM(los_days) AS total_los_days,
  0 AS visit_concept_id
FROM normalized_dates
GROUP BY person_id

UNION ALL

SELECT
  person_id,
  SUM(los_days) AS total_los_days,
  visit_concept_id
FROM normalized_dates
GROUP BY person_id, visit_concept_id;