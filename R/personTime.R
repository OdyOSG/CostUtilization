#' Calculate Person-Time for Normalization (Internal)
#'
#' This internal function calculates the total person-time for each person in the database,
#' which is used for cost normalization purposes. It executes a SQL query that computes
#' the time each person contributes to the analysis period.
#'
#' @param connection A DatabaseConnector connection object to the database containing the CDM data
#' @param tempEmulationSchema A schema where temporary tables can be created in platforms that do not
#'                           support native temporary tables (e.g., Oracle, BigQuery). Default: NULL
#'
#' @return Nothing is returned, but creates/updates the appropriate tables in the database
#'
#' @keywords internal
.calculatePersonTime <- function(
    connection,
    tempEmulationSchema = NULL
) {
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CalculatePersonTime.sql",
    packageName = "CostModule",
    dbms = attr(connection, "dbms"),
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::executeSql(connection, sql)
}
