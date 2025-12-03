#' Create Analysis Windows (Internal)
#'
#' Creates temporary tables with analysis windows based on the provided cohorts
#' and time window specifications.
#'
#' @param connection       A DatabaseConnector connection object.
#' @param cdmDatabaseSchema        Schema name where the CDM data resides. Requires read permissions.
#' @param cohortDatabaseSchema     Schema name where the cohort data resides. Requires read permissions.
#' @param tempEmulationSchema      Some database platforms require a schema for temporary tables.
#'                                 This parameter can be NULL for platforms that do not require this.
#' @param cohortTable              Name of the cohort table to use for analysis.
#' @param cohortIds                Integer vector of cohort IDs to include in the analysis.
#' @param window                   A list containing the time window specifications:
#'                                 - startWith: Event to use as window start (e.g., 'cohort_start_date')
#'                                 - startOffset: Days to add/subtract from startWith date
#'                                 - endWith: Event to use as window end (e.g., 'cohort_end_date')
#'                                 - endOffset: Days to add/subtract from endWith date
#'
#' @return Nothing is returned but creates the #analysis_window temp table in the database
#'
#' @keywords internal
.createAnalysisWindows <- function(
    connection,
    cdmDatabaseSchema,
    tempEmulationSchema = NULL,
    cohortTable,
    cohortIds,
    window) {
  # Render SQL to create analysis windows
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CreateAnalysisWindows.sql",
    packageName = "CostModule",
    dbms = attr(connection, "dbms"),
    cdm_database_schema = cdmDatabaseSchema,
    cohort_table = cohortTable,
    start_with = window$startWith,
    start_offset = window$startOffset,
    end_with = window$endWith,
    end_offset = window$endOffset,
    tempEmulationSchema = tempEmulationSchema,
    target_ids = paste(cohortIds, collapse = ", ")
  )
  
  DatabaseConnector::executeSql(connection, sql)
  message("Analysis windows created")
}
