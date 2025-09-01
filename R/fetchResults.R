.fetchResults <- function(params, connection, tempEmulationSchema, verbose) {
  executeSqlPlan(
    connection          = connection,
    params              = params,
    targetDialect       = DatabaseConnector::dbms(connection),
    tempEmulationSchema = tempEmulationSchema,
    verbose             = verbose
  )
  andr <- Andromeda::andromeda(
    results = DBI::dbGetQuery(
      connection, glue::glue("select * from {params$cohortDatabaseSchema}.{params$resultsTable}")
    )
  )
  return(andr)
}