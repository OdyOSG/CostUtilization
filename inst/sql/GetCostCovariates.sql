{DEFAULT @filter_by_event_concepts = FALSE}

-- Find all visits for the target cohort within the specified time window
WITH cohort_visits AS (
  SELECT
    v.person_id,
    v.visit_occurrence_id,
    c.cohort_start_date
  FROM @cdm_database_schema.visit_occurrence v
  JOIN @cohort_database_schema.@cohort_table c ON v.person_id = c.subject_id -- Adjusted schema
  WHERE c.cohort_definition_id = @cohort_id
    AND v.visit_start_date >= DATEADD(day, @start_day, c.cohort_start_date)
    AND v.visit_start_date <= DATEADD(day, @end_day, c.cohort_start_date)
),

-- Optional: Filter visits based on associated clinical events
filtered_visits AS (
  SELECT
    cv.person_id,
    cv.visit_occurrence_id
  FROM cohort_visits cv
  {@filter_by_event_concepts} ? {
  WHERE EXISTS (
    -- Check for associated procedures
    SELECT 1 FROM @cdm_database_schema.procedure_occurrence po WHERE po.visit_occurrence_id = cv.visit_occurrence_id AND po.procedure_concept_id IN (@event_concept_ids)
    UNION ALL
    -- Check for associated drug exposures
    SELECT 1 FROM @cdm_database_schema.drug_exposure de WHERE de.visit_occurrence_id = cv.visit_occurrence_id AND de.drug_concept_id IN (@event_concept_ids)
    UNION ALL
    -- Check for associated condition occurrences
    SELECT 1 FROM @cdm_database_schema.condition_occurrence co WHERE co.visit_occurrence_id = cv.visit_occurrence_id AND co.condition_concept_id IN (@event_concept_ids)
    -- Add other domains as needed (device, measurement, observation)
  )
  }
  {@!filter_by_event_concepts} ? {
  -- If no filter is specified, include all cohort visits
  }
),

-- Calculate the sum of costs for the filtered visits
cost_summary AS (
  SELECT
    fv.person_id,
    SUM(c.cost) AS sum_value
  FROM @cdm_database_schema.cost c
  JOIN filtered_visits fv ON c.cost_event_id = fv.visit_occurrence_id -- This assumes cost_event_id links to visit_occurrence_id
  WHERE c.cost_concept_id IN (@cost_concept_id)
    AND c.cost_type_concept_id IN (@cost_type_concept_id)
    AND c.cost_event_table = 'visit_occurrence' -- Enforcing visit-centric linkage
  GROUP BY fv.person_id
)

-- Final output, label with covariateId and join back to the full cohort
-- to ensure all cohort members are represented (with a value of 0 if no costs).
SELECT
  c.cohort_definition_id AS cohort_id,
  c.subject_id,
  @covariate_id AS covariate_id,
  COALESCE(cs.sum_value, 0) AS sum_value
FROM @cohort_database_schema.@cohort_table c -- Adjusted schema
LEFT JOIN cost_summary cs
  ON c.subject_id = cs.person_id
WHERE c.cohort_definition_id = @cohort_id;