-- 2.2) Optional event filters -> qualifying visits
{@has_event_filters} ? {

  DROP TABLE IF EXISTS #events_by_filter;
  CREATE TABLE #events_by_filter (
    filter_id          INT          NOT NULL,
    filter_name        VARCHAR(255) NULL,
    person_id          BIGINT       NOT NULL,
    visit_occurrence_id BIGINT      NULL,
    visit_detail_id    BIGINT       NULL
  );

  INSERT INTO #events_by_filter
  SELECT ec.filter_id, ec.filter_name, de.person_id, de.visit_occurrence_id, de.visit_detail_id
  FROM @cdm_database_schema.drug_exposure de
  JOIN @event_concepts_table ec
    -- MODIFIED: Handle NULL concept_id to mean all concepts in domain
    ON (ec.concept_id = de.drug_concept_id OR ec.concept_id IS NULL)
   AND ec.domain_scope IN ('All','Drug')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, po.person_id, po.visit_occurrence_id, po.visit_detail_id
  FROM @cdm_database_schema.procedure_occurrence po
  JOIN @event_concepts_table ec
    -- MODIFIED: Handle NULL concept_id to mean all concepts in domain
    ON (ec.concept_id = po.procedure_concept_id OR ec.concept_id IS NULL)
   AND ec.domain_scope IN ('All','Procedure')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, co.person_id, co.visit_occurrence_id, NULL
  FROM @cdm_database_schema.condition_occurrence co
  JOIN @event_concepts_table ec
    -- MODIFIED: Handle NULL concept_id to mean all concepts in domain
    ON (ec.concept_id = co.condition_concept_id OR ec.concept_id IS NULL)
   AND ec.domain_scope IN ('All','Condition')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, ms.person_id, ms.visit_occurrence_id, ms.visit_detail_id
  FROM @cdm_database_schema.measurement ms
  JOIN @event_concepts_table ec
    -- MODIFIED: Handle NULL concept_id to mean all concepts in domain
    ON (ec.concept_id = ms.measurement_concept_id OR ec.concept_id IS NULL)
   AND ec.domain_scope IN ('All','Measurement')

  UNION ALL
  SELECT ec.filter_id, ec.filter_name, ob.person_id, ob.visit_occurrence_id, ob.visit_detail_id
  FROM @cdm_database_schema.observation ob
  JOIN @event_concepts_table ec
    -- MODIFIED: Handle NULL concept_id to mean all concepts in domain
    ON (ec.concept_id = ob.observation_concept_id OR ec.concept_id IS NULL)
   AND ec.domain_scope IN ('All','Observation')
  ;
  
  DROP TABLE IF EXISTS #event_visits;
  CREATE TABLE #event_visits (
    cohort_definition_id BIGINT       NULL,
    person_id           BIGINT NOT NULL,
    visit_occurrence_id BIGINT NOT NULL
  );

  INSERT INTO #event_visits
  SELECT cohort_definition_id,  person_id, visit_occurrence_id
  FROM #events_by_filter
  WHERE visit_occurrence_id IS NOT NULL
  GROUP BY cohort_definition_id, person_id, visit_occurrence_id
  HAVING COUNT(DISTINCT filter_id) >= @n_filters;

  DROP TABLE IF EXISTS #qualifying_visits;
  SELECT v.*
  INTO #qualifying_visits
  FROM #visits_in_window v
  JOIN #event_visits ev
    ON ev.person_id = v.person_id
   AND ev.visit_occurrence_id = v.visit_occurrence_id;

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
  SELECT *
  INTO #qualifying_visits
  FROM #visits_in_window;
};