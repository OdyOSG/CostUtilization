cleanupTempTables <- function(connection, schema, ...) {
  tables <- list(...)
  for (table in tables) {
    if (!is.null(table)) {
      sql <- "DROP TABLE IF EXISTS @schema.@table;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql,
        schema = schema,
        table = table
      )
    }
  }
}

logMessage <- function(message, verbose, level = "INFO") {
  if (verbose) {
    if (level == "ERROR") {
      cli::cli_alert_danger(message)
    } else if (level == "WARNING") {
      cli::cli_alert_warning(message)
    } else if (level == "INFO") {
      cli::cli_alert_info(message)
    } else {
      cli::cli_alert(message)
    }
  }
}
