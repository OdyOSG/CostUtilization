#' Calculate Length of Stay (LOS) for a Cohort
#'
#' @description
#' Calculates the length of stay (LOS) in days for visits that occur within a
#' specified time window relative to a cohort's anchor date. The function returns
#' LOS aggregated by person and visit concept type, plus a total aggregation
#' for each person (where `visit_concept_id = 0`).
#'
#' @param connection A live `DBI` or `DatabaseConnector` connection object.
#' @param cdmDatabaseSchema The schema containing the OMOP CDM tables.
#' @param cohortDatabaseSchema The schema containing the cohort table.
#' @param cohortTable The name of the cohort table.
#' @param cohortId The numeric cohort definition ID to analyze.
#' @param anchorCol The date column in the cohort table to use as the anchor for
#'   the time window. Must be one of `"cohort_start_date"` or `"cohort_end_date"`.
#' @param startOffsetDays Integer, the start day of the observation window relative to the
#'   anchor date (e.g., 0 for the anchor date, -30 for 30 days prior).
#' @param endOffsetDays Integer, the end day of the observation window relative to the
#'   anchor date (e.g., 365 for 365 days after).
#' @param visitTable The visit-level table to use for the calculation. Must be
#'   one of `"visit_occurrence"` or `"visit_detail"`.
#' @param visitConceptIds An Integer vector of visit concept IDs 
#' @param censorOnCohortEndDate Logical. If `TRUE`, the length of stay for visits
#'   that extend beyond the `cohort_end_date` will be censored at the
#'   `cohort_end_date`. Default is `FALSE`.
#' @param verbose A logical flag to enable or disable progress messages.
#'
#' @return A dplyr `tibble` with three columns: `person_id`, `visit_concept_id`,
#'   and `total_los_days`.
#'
#' @export
calculateLos <- function(connection,
                         cdmDatabaseSchema,
                         cohortDatabaseSchema,
                         cohortTable,
                         cohortId,
                         anchorCol = "cohort_start_date",
                         startOffsetDays = 0,
                         endOffsetDays = 365,
                         visitTable = c("visit_occurrence", "visit_detail"),
                         visitConceptIds = c(9201),
                         censorOnCohortEndDate = FALSE,
                         verbose = TRUE) {
  
  # --- Validation ---
  anchorCol <- rlang::arg_match(anchorCol, values = c("cohort_start_date", "cohort_end_date"))
  visitTable <- rlang::arg_match(visitTable)
  errorMessages <- checkmate::makeAssertCollection()
  
  checkmate::assertClass(connection, "DBIConnection", add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertCharacter(cohortDatabaseSchema, len = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertCharacter(cohortTable, len = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertIntegerish(cohortId, len = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertIntegerish(startOffsetDays, len = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertIntegerish(endOffsetDays, len = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertFlag(censorOnCohortEndDate, add = errorMessages)
  checkmate::assertFlag(verbose, add = errorMessages)

  checkmate::assertIntegerish(visitConceptIds, min.len = 1, unique = TRUE, any.missing = FALSE, add = errorMessages)
  
  checkmate::reportAssertions(errorMessages)
  
  if (endOffsetDays < startOffsetDays) {
    cli::cli_abort("`endOffsetDays` ({endOffsetDays}) must be greater than or equal to `startOffsetDays` ({startOffsetDays}).")
  }
  
  # --- SQL Execution ---
  if (verbose) {
    cli::cli_inform("Calculating Length of Stay for cohort ID {.val {cohortId}}...")
  }
  
  sql <- SqlRender::readSql(system.file("sql", "LoS.sql", package = "CostUtilization")) |> 
    SqlRender::render(
      cdm_database_schema = cdmDatabaseSchema,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      cohort_id = cohortId,
      visit_table = visitTable,
      visit_id_col = ifelse(visitTable == "visit_occurrence", "visit_occurrence_id", "visit_detail_id"),
      visit_concept_ids = visitConceptIds,
      time_a = startOffsetDays,
      time_b = endOffsetDays,
      use_cohort_end_date = as.integer(anchorCol == "cohort_end_date"),
      censor_on_cohort_end_date = as.integer(censorOnCohortEndDate),
    ) |> 
    SqlRender::translate(DatabaseConnector::dbms(connection))
  
  results <- DBI::dbGetQuery(connection, sql) |> 
    dplyr::rename_all(SqlRender::snakeCaseToCamelCase) |> 
    dplyr::tibble()
  
  if (verbose) {
    cli::cli_alert_success("Calculation complete. Found {nrow(results)} summary rows.")
  }
  
  return(results)
}