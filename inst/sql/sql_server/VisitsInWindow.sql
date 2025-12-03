DROP TABLE IF EXISTS #visits_in_window;
CREATE TABLE #visits_in_window (
  cohort_definition_id BIGINT    NOT NULL,
  person_id           BIGINT NOT NULL,
  visit_occurrence_id BIGINT NOT NULL,
  visit_start_date    DATE   NOT NULL,
  visit_end_date      DATE   NOT NULL,
  visit_concept_id    INT    NULL
);

INSERT INTO #visits_in_window
SELECT
  aw.cohort_definition_id,
  vo.person_id,
  vo.visit_occurrence_id,
  vo.visit_start_date,
  vo.visit_end_date,
  vo.visit_concept_id
FROM @cdm_database_schema.visit_occurrence vo
JOIN #analysis_window aw
  ON aw.person_id = vo.person_id
WHERE vo.visit_end_date   >= aw.start_date
  AND vo.visit_start_date <= aw.end_date
{@has_visit_restriction} ? {
  AND vo.visit_concept_id IN (SELECT visit_concept_id FROM @restrict_visit_table)
};