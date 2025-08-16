materializeVisitConcepts <- function(conn, conceptIds, rootName, schema, permanent) {
  tableName <- paste0(rootName, "_visit_concepts")
  df <- tibble::tibble(visit_concept_id = as.integer(conceptIds))
  DatabaseConnector::insertTable(
    connection = conn,
    tableName = tableName,
    databaseSchema = schema,
    data = df,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = !permanent,
    camelCaseToSnakeCase = TRUE
  )
  return(tableName)
}

materializeEventFilters <- function(conn, eventFilters, rootName, schema, permanent) {
  tableName <- paste0(rootName, "_event_concepts")
  df <- purrr::imap_dfr(eventFilters, ~ {
    tibble::tibble(
      filter_id = .y,
      filter_name = .x$name,
      concept_id = .x$concepts,
      domain_scope = .x$domainScope
    )
  }) |>
    dplyr::mutate(
      filter_id = as.integer(.data$filter_id),
      concept_id = as.integer(.data$concept_id)
    )

  DatabaseConnector::insertTable(
    connection = conn,
    tableName = tableName,
    databaseSchema = schema,
    data = df,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = !permanent,
    camelCaseToSnakeCase = TRUE
  )
  return(tableName)
}


executeSqlPlan <- function(conn, params, targetDialect, tempEmulationSchema) {
  sqlPath <- system.file("sql", "main_cost_utilization.sql", package = "CostUtilization", mustWork = TRUE)
  sql <- SqlRender::readSql(sqlPath)

  DatabaseConnector::renderTranslateExecuteSql(
    connection = conn,
    sql = sql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    tempEmulationSchema = tempEmulationSchema,
    .warnOnMissingParameters = TRUE,
    ... = params
  )
}
