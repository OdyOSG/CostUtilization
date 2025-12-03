-- ============================================================================
-- Module: Cleanup
-- Purpose: Drop temporary tables and finalize diagnostics
-- ============================================================================

-- Drop temporary tables
DROP TABLE IF EXISTS #cohort_person;
DROP TABLE IF EXISTS #analysis_window;
DROP TABLE IF EXISTS #analysis_window_clean;
DROP TABLE IF EXISTS #person_time;
DROP TABLE IF EXISTS #visits_in_window;
DROP TABLE IF EXISTS #qualifying_visits;

{@has_event_filters} ? {
  DROP TABLE IF EXISTS #events_by_filter;
  DROP TABLE IF EXISTS #event_visits;
  {@micro_costing} ? { DROP TABLE IF EXISTS #primary_filter_details; }
}

DROP TABLE IF EXISTS #costs_raw;
{@micro_costing} ? { DROP TABLE IF EXISTS #line_level_cost; } : { DROP TABLE IF EXISTS #visit_level_cost; }
DROP TABLE IF EXISTS #denominator;
DROP TABLE IF EXISTS #numerators;

-- Final diagnostic
INSERT INTO @diag_table (step_name, n_persons, n_events)
VALUES ('99_completed', NULL, NULL);