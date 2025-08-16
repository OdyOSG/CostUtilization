cleanupTempTables <- function(connection, schema, ...) {
  tables <- list(...)
  for (table in tables) {
    if (!is.null(table)) {
      sql <- "DROP TABLE IF EXISTS @schema.@table;"
      DatabaseConnector::renderTranslateExecuteSql(  # Fixed typo
        connection,
        sql,
        schema = schema,
        table = table
      )
    }
  }
}