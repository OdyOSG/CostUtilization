#' SQL Module Executor
#'
#' @description
#' Executes SQL modules in sequence using base R and DatabaseConnector.
#' This replaces the monolithic SQL approach with a modular, maintainable structure.
#'
#' @param connection A DatabaseConnector connection object.
#' @param params Named list of parameters for SQL rendering.
#' @param targetDialect The SQL dialect to translate to.
#' @param tempEmulationSchema Schema for temp table emulation (if needed).
#' @param verbose Whether to output progress messages.
#'
#' @return NULL (invisibly)
#' @noRd
executeSqlModules <- function(
    connection,
    params,
    targetDialect,
    tempEmulationSchema,
    verbose = TRUE) {
  
  logMessage("Starting modular SQL execution", verbose, "INFO")
  
  # Define SQL modules in execution order
  sqlModules <- c(
    "00_initialize_diagnostics.sql",
    "01_cohort_analysis_window.sql", 
    "02_qualifying_visits.sql",
    "03_cost_calculation.sql",
    "04_final_calculations.sql",
    "05_cleanup.sql"
  )
  
  # Prepare render parameters once
  renderParams <- prepareSqlRenderParams(params, tempEmulationSchema)
  
  # Execute each module
  for (i in seq_along(sqlModules)) {
    moduleName <- sqlModules[i]
    logMessage(sprintf("Executing module %d/%d: %s", i, length(sqlModules), moduleName), verbose, "INFO")
    
    executeModule(
      connection = connection,
      moduleName = moduleName,
      renderParams = renderParams,
      targetDialect = targetDialect,
      tempEmulationSchema = tempEmulationSchema,
      verbose = verbose
    )
  }
  
  logMessage("Modular SQL execution completed", verbose, "SUCCESS")
  invisible(NULL)
}

#' Execute Single SQL Module
#'
#' @description
#' Executes a single SQL module file with error handling and progress tracking.
#'
#' @param connection DatabaseConnector connection object.
#' @param moduleName Name of the SQL module file.
#' @param renderParams List of parameters for SQL rendering.
#' @param targetDialect SQL dialect for translation.
#' @param tempEmulationSchema Schema for temp table emulation.
#' @param verbose Whether to show progress messages.
#'
#' @return NULL (invisibly)
#' @noRd
executeModule <- function(
    connection,
    moduleName,
    renderParams,
    targetDialect,
    tempEmulationSchema,
    verbose = TRUE) {
  
  # Read SQL module
  sqlPath <- system.file("sql", "modules", moduleName, package = "CostUtilization", mustWork = TRUE)
  
  if (!file.exists(sqlPath)) {
    stop(sprintf("SQL module not found: %s", sqlPath))
  }
  
  sql <- readLines(sqlPath, warn = FALSE)
  sql <- paste(sql, collapse = "\n")
  
  # Render and translate SQL
  tryCatch({
    renderedSql <- do.call(SqlRender::render, c(list(sql = sql), renderParams))
    
    translatedSql <- SqlRender::translate(
      sql = renderedSql,
      targetDialect = targetDialect,
      tempEmulationSchema = tempEmulationSchema
    )
    
    # Split into individual statements
    sqlStatements <- SqlRender::splitSql(translatedSql)
    
    # Execute statements
    executeModuleStatements(
      connection = connection,
      sqlStatements = sqlStatements,
      moduleName = moduleName,
      verbose = verbose
    )
    
  }, error = function(e) {
    logMessage(sprintf("Error in module %s: %s", moduleName, conditionMessage(e)), verbose, "ERROR")
    stop(e)
  })
  
  invisible(NULL)
}

#' Execute SQL Statements for a Module
#'
#' @description
#' Executes SQL statements for a specific module with error handling.
#'
#' @param connection DatabaseConnector connection object.
#' @param sqlStatements Character vector of SQL statements.
#' @param moduleName Name of the module (for error reporting).
#' @param verbose Whether to show progress messages.
#'
#' @return NULL (invisibly)
#' @noRd
executeModuleStatements <- function(connection, sqlStatements, moduleName, verbose = TRUE) {
  
  nStatements <- length(sqlStatements)
  if (nStatements == 0) return(invisible(NULL))
  
  logMessage(sprintf("  Executing %d statement%s", nStatements, if (nStatements != 1) "s" else ""), verbose, "DEBUG")
  
  for (i in seq_len(nStatements)) {
    sql <- sqlStatements[i]
    
    # Skip empty statements
    if (is.null(sql) || !nzchar(trimws(sql))) {
      next
    }
    
    tryCatch({
      DatabaseConnector::executeSql(connection, sql)
    }, error = function(e) {
      # Enhanced error reporting
      previewSql <- substr(gsub("[\r\n]+", " ", sql), 1, 100)
      if (nchar(sql) > 100) previewSql <- paste0(previewSql, "...")
      
      logMessage(sprintf("Error in %s, statement %d: %s", moduleName, i, conditionMessage(e)), verbose, "ERROR")
      logMessage(sprintf("SQL preview: %s", previewSql), verbose, "DEBUG")
      stop(e)
    })
  }
  
  invisible(NULL)
}