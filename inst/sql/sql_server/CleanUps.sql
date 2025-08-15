-- Clean up all temporary tables to free up resources
DROP TABLE IF EXISTS #target_tmp_cohorts;
DROP TABLE IF EXISTS #analysis_windows;
DROP TABLE IF EXISTS #cohort_costs;
DROP TABLE IF EXISTS #aggregated_costs;
DROP TABLE IF EXISTS #window_denominators;
-- Clean up concept set table if it was created
{@final_codeset != ''} ? {DROP TABLE IF EXISTS @final_codeset;}
-- Cleanup
{@useInflationAdjustment} ? {
DROP TABLE IF EXISTS #target_inflation;
}