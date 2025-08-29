.fetchResults <- function(params, connection, tempEmulationSchema, verbose) {
  # step 1 - fulfill analysis
  
  executeSqlPlan(
    connection          = connection,
    params              = params,
    targetDialect       = DatabaseConnector::dbms(connection),
    tempEmulationSchema = tempEmulationSchema,
    verbose             = verbose
  )
  andromeda <- Andromeda::andromeda()
  andromeda[['results']] <- DBI::dbGetQuery(
    connection, glue::glue("select * from {params$cohortDatabaseSchema}.{params$resultsTable}")
    )
  return(andromeda)
}