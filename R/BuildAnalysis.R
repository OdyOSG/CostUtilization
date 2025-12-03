# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of CostUtilization
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Execute Modular SQL Plan for Cost Analysis
#'
#' @description
#' Executes the modular SQL approach using individual SQL modules instead of
#' a single monolithic SQL file. Uses DatabaseConnector for all operations.
#'
#' @param connection A DatabaseConnector connection object.
#' @param params Named list of parameters for SQL rendering (can be camelCase or snake_case).
#' @param targetDialect The SQL dialect to translate to.
#' @param tempEmulationSchema Schema for temp table emulation (if needed).
#' @param verbose Whether to output progress messages.
#'
#' @return Character string containing the final query SQL
#' @noRd
executeSqlPlan <- function(
    connection,
    params,
    targetDialect,
    tempEmulationSchema,
    verbose = TRUE) {
  
  logMessage("Starting modular SQL plan execution", verbose, "INFO")
  
  # Prepare parameters for SQL rendering
  renderParams <- prepareSqlRenderParams(params, tempEmulationSchema)
  
  # Define the SQL modules in execution order
  sqlModules <- c(
    "CreateTempTables.sql",
    "BuildCohortWindow.sql",
    "ApplyVisitRestrictions.sql",
    "ApplyEventFilters.sql",
    "CalculateBaseCosts.sql",
    "ApplyCpiAdjustment.sql",
    "AggregateResults.sql",
    "FinalQuery.sql"
  )
  
  logMessage(sprintf("Executing %d SQL modules", length(sqlModules)), verbose, "INFO")
  
  finalQuerySql <- NULL
  
  # Execute each SQL module
  for (i in seq_along(sqlModules)) {
    moduleName <- sqlModules[i]
    logMessage(sprintf("Processing module %d/%d: %s", i, length(sqlModules), moduleName), verbose, "DEBUG")
    
    # Read the SQL module
    sqlPath <- system.file("sql", "modules", moduleName, 
                          package = "CostUtilization", mustWork = TRUE)
    sql <- SqlRender::readSql(sqlPath)
    
    # Render and translate SQL
    renderedSql <- do.call(SqlRender::render, c(list(sql = sql), renderParams))
    translatedSql <- SqlRender::translate(
      sql = renderedSql,
      targetDialect = targetDialect,
      tempEmulationSchema = tempEmulationSchema
    )
    
    # Split into individual statements
    sqlStatements <- SqlRender::splitSql(translatedSql)
    
    # Handle the final query differently - return it instead of executing
    if (moduleName == "FinalQuery.sql") {
      finalQuerySql <- sqlStatements[1]
      logMessage("Final query SQL prepared", verbose, "DEBUG")
    } else {
      # Execute all statements for this module
      executeModuleStatements(
        connection = connection,
        sqlStatements = sqlStatements,
        moduleName = moduleName,
        verbose = verbose
      )
    }
  }
  
  logMessage("Modular SQL plan execution completed", verbose, "INFO")
  return(finalQuerySql)
}

#' Execute SQL Statements for a Module
#'
#' @description
#' Executes all SQL statements for a given module using DatabaseConnector.
#'
#' @param connection DatabaseConnector connection object.
#' @param sqlStatements Character vector of SQL statements.
#' @param moduleName Name of the module being executed.
#' @param verbose Whether to output progress messages.
#'
#' @return NULL (invisibly)
#' @noRd
executeModuleStatements <- function(connection, sqlStatements, moduleName, verbose = TRUE) {
  if (length(sqlStatements) == 0) {
    logMessage(sprintf("No statements to execute for module: %s", moduleName), verbose, "DEBUG")
    return(invisible(NULL))
  }
  
  for (i in seq_along(sqlStatements)) {
    stmt <- sqlStatements[i]
    if (nchar(trimws(stmt)) == 0) {
      next
    }
    
    tryCatch({
      logMessage(sprintf("Executing statement %d/%d for module %s", 
                        i, length(sqlStatements), moduleName), verbose, "DEBUG")
      
      DatabaseConnector::executeSql(
        connection = connection,
        sql = stmt,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
      
    }, error = function(e) {
      logMessage(sprintf("Error in module %s, statement %d: %s", 
                        moduleName, i, e$message), verbose, "ERROR")
      stop(sprintf("SQL execution failed in module %s: %s", moduleName, e$message))
    })
  }
  
  logMessage(sprintf("Module %s completed successfully", moduleName), verbose, "DEBUG")
  invisible(NULL)
}

#' Prepare SQL Render Parameters
#'
#' @description
#' Accepts either camelCase (legacy) or snake_case (new) inputs and returns a
#' single normalized list for SqlRender::render(). All boolean flags are 0/1.
#'
#' @param params List of parameters from the main function.
#' @param tempEmulationSchema Schema for temporary tables.
#'
#' @return Named list ready for SQL rendering.
#' @noRd
prepareSqlRenderParams <- function(
    params,
    tempEmulationSchema) {
  # derive hasEventFilters if not explicitly set (keeps backward compatibility)
  has_event_filters <- params$hasEventFilters %||% (as.integer(params$nFilters %||% 0L) > 0L)

  list(
    # schema/table/id
    cdm_database_schema    = params$cdmDatabaseSchema,
    cohort_database_schema = params$cohortDatabaseSchema,
    cohort_table           = params$cohortTable,
    cohort_id              = as.integer(params$cohortId),

    # window & anchor
    anchor_on_end          = .int_flag(params$anchorOnEnd),
    time_a                 = as.integer(params$timeA %||% 0L),
    time_b                 = as.integer(params$timeB %||% 365L),
    aggregated             = params$aggregated,

    # costing & filters
    cost_concept_id        = as.integer(params$costConceptId),
    currency_concept_id    = as.integer(params$currencyConceptId),
    n_filters              = as.integer(params$nFilters %||% 0L),
    has_visit_restriction  = .int_flag(params$hasVisitRestriction),
    has_event_filters      = .int_flag(has_event_filters),
    micro_costing          = params$microCosting,
    primary_filter_id      = as.integer(params$primaryFilterId %||% 0L),
    restrict_visit_table   = params$restrictVisitTable,
    event_concepts_table   = params$eventConceptsTable,

    # CPI
    cpi_adjustment         = .int_flag(params$cpiAdjustment),
    cpi_adj_table          = params$cpiAdjTable,
    
    # temp emulation
    temp_emulation_schema  = tempEmulationSchema
  )
}

#' Validate DatabaseConnector Connection
#'
#' @description
#' Validates that the provided connection is a DatabaseConnector connection object.
#'
#' @param connection Connection object to validate.
#'
#' @return TRUE if valid, throws error otherwise.
#' @noRd
validateDatabaseConnectorConnection <- function(connection) {
  if (is.null(connection)) {
    stop("Connection cannot be NULL")
  }
  
  # Check if it's a DatabaseConnector connection
  if (!inherits(connection, "DatabaseConnectorConnection")) {
    stop("Connection must be a DatabaseConnector connection object")
  }
  
  # Additional validation could be added here
  return(TRUE)
}

#' Execute SQL with Error Handling
#'
#' @description
#' Wrapper around DatabaseConnector::executeSql with enhanced error handling.
#'
#' @param connection DatabaseConnector connection object.
#' @param sql SQL statement to execute.
#' @param verbose Whether to output progress messages.
#'
#' @return NULL (invisibly)
#' @noRd
executeSqlWithErrorHandling <- function(connection, sql, verbose = TRUE) {
  validateDatabaseConnectorConnection(connection)
  
  if (is.null(sql) || nchar(trimws(sql)) == 0) {
    logMessage("Empty SQL statement provided", verbose, "WARNING")
    return(invisible(NULL))
  }
  
  tryCatch({
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = verbose,
      reportOverallTime = verbose
    )
  }, error = function(e) {
    logMessage(sprintf("SQL execution error: %s", e$message), verbose, "ERROR")
    logMessage(sprintf("Failed SQL: %s", substr(sql, 1, 200)), verbose, "DEBUG")
    stop(sprintf("SQL execution failed: %s", e$message))
  })
  
  invisible(NULL)
}