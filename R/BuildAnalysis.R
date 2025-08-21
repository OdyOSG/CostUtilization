#' Execute SQL Plan for Cost Analysis
#'
#' @description
#' Executes the main SQL analysis plan for cost and utilization calculations.
#' This function reads the SQL template, renders it with parameters, and executes
#' the statements against the database.
#'
#' @param connection A DatabaseConnector connection object
#' @param params List of parameters for SQL rendering
#' @param targetDialect The SQL dialect to translate to
#' @param tempEmulationSchema Schema for temporary tables (if needed)
#' @param verbose Whether to output progress messages
#'
#' @return NULL (invisibly)
#' @noRd
executeSqlPlan <- function(connection,
                          params,
                          targetDialect,
                          tempEmulationSchema,
                          verbose = TRUE) {
  logMessage("Starting SQL plan execution", verbose, "INFO")
  
  # Read SQL template
  sql <- system.file(
    "sql", 
    "MainCostUtilization.sql", 
    package = "CostUtilization", 
    mustWork = TRUE
  ) |> 
    SqlRender::readSql()
  
  # Prepare parameters for SQL rendering
  renderParams <- prepareSqlRenderParams(params, tempEmulationSchema)
  
  # Render SQL with parameters
  logMessage(sprintf("Translating SQL to %s dialect", targetDialect), verbose, "DEBUG")
  sql <- SqlRender::render(
    sql = sql,
    cdm_database_schema = renderParams$cdm_database_schema,
    cohort_database_schema = renderParams$cohort_database_schema,
    cohort_table = renderParams$cohort_table,
    cohort_id = renderParams$cohort_id,
    anchor_col = renderParams$anchor_col,
    time_a = renderParams$time_a,
    time_b = renderParams$time_b,
    cost_concept_id = renderParams$cost_concept_id,
    currency_concept_id = renderParams$currency_concept_id,
    n_filters = renderParams$n_filters,
    has_visit_restriction = renderParams$has_visit_restriction,
    has_event_filters = renderParams$has_event_filters,
    micro_costing = renderParams$micro_costing,
    primary_filter_id = renderParams$primary_filter_id,
    results_table = renderParams$results_table,
    diag_table = renderParams$diag_table,
    restrict_visit_table = renderParams$restrict_visit_table,
    event_concepts_table = renderParams$event_concepts_table
  )
  
  # Translate to target dialect
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = targetDialect,
    tempEmulationSchema = tempEmulationSchema
  )
  
  # Split and execute SQL statements
  sqlStatements <- SqlRender::splitSql(sql)
  logMessage(sprintf("Executing %d SQL statements", length(sqlStatements)), verbose, "INFO")
  
  executeSqlStatements(
    connection = connection,
    sqlStatements = sqlStatements,
    verbose = verbose
  )
  
  logMessage("SQL plan execution completed", verbose, "INFO")
  invisible(NULL)
}

#' Prepare SQL Render Parameters
#'
#' @description
#' Converts R parameter names (camelCase) to SQL parameter names (snake_case)
#' and prepares all parameters needed for SQL rendering.
#'
#' @param params List of parameters from the main function
#' @param tempEmulationSchema Schema for temporary tables
#'
#' @return List of parameters ready for SQL rendering
#' @noRd
prepareSqlRenderParams <- function(params, tempEmulationSchema) {
  sqlParams <- list()
  
  # Direct mapping from R camelCase to SQL snake_case
  sqlParams$cdm_database_schema <- params$cdmDatabaseSchema
  sqlParams$cohort_database_schema <- params$cohortDatabaseSchema
  sqlParams$cohort_table <- params$cohortTable
  sqlParams$cohort_id <- params$cohortId
  sqlParams$anchor_col <- params$anchorCol
  sqlParams$time_a <- params$startOffsetDays
  sqlParams$time_b <- params$endOffsetDays
  sqlParams$cost_concept_id <- params$costConceptId
  sqlParams$currency_concept_id <- params$currencyConceptId
  sqlParams$n_filters <- params$nFilters
  
  # Boolean flags for conditional SQL
  sqlParams$has_visit_restriction <- params$hasVisitRestriction
  sqlParams$has_event_filters <- params$hasEventFilters
  sqlParams$micro_costing <- params$microCosting
  
  # Derived parameters
  if (isTRUE(params$microCosting) && !is.null(params$primaryEventFilterName)) {
    filterNames <- purrr::map_chr(params$eventFilters, "name")
    primaryFilterIndex <- which(filterNames == params$primaryEventFilterName)
    
    if (length(primaryFilterIndex) == 1) {
      sqlParams$primary_filter_id <- primaryFilterIndex
    } else {
      cli::cli_abort(c(
        "Could not find a unique primary event filter for micro-costing.",
        "i" = "Expected one filter named '{params$primaryEventFilterName}' but found {length(primaryFilterIndex)}."
      ))
    }
  } else {
    sqlParams$primary_filter_id <- 0
  }
  
  # Schema-qualified table names
  sqlParams$results_table <- qualifyTableName(params$resultsTable, tempEmulationSchema)
  sqlParams$diag_table <- qualifyTableName(params$diagTable, tempEmulationSchema)
  sqlParams$restrict_visit_table <- qualifyTableName(params$restrictVisitTable, tempEmulationSchema)
  sqlParams$event_concepts_table <- qualifyTableName(params$eventConceptsTable, tempEmulationSchema)
  
  return(sqlParams)
}

#' Qualify Table Name with Schema
#'
#' @description
#' Helper function to prepend schema name to table name if both are provided.
#'
#' @param tableName The table name
#' @param schema The schema name (can be NULL)
#'
#' @return Qualified table name (schema.table) or just table name if no schema
#' @noRd
qualifyTableName <- function(tableName, schema) {
  if (!is.null(schema) && !is.null(tableName)) {
    return(paste(schema, tableName, sep = "."))
  }
  return(tableName)
}