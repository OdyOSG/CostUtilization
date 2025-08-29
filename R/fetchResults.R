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
  DatabaseConnector::querySqlToAndromeda(
    connection,
    glue::glue("select * from {params$cohortDatabaseSchema}.{params$resultTable}"),
    andromeda = andromeda,
    andromedaTableName = "results"
  )
  return(andromeda)
}