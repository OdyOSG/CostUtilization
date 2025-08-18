#' Build and Execute Cost Analysis
#'
#' @description
#' Internal functions for building and executing the cost analysis SQL plan.
#' These functions handle SQL rendering, translation, and execution.
#'
#' @keywords internal
#' Execute SQL Plan for Cost Analysis
#'
#' @description
#' Renders, translates, and executes the main cost analysis SQL
#'
#' @param connection DatabaseConnector connection object
#' @param params List of SQL parameters
#' @param targetDialect Target SQL dialect
#' @param tempEmulationSchema Schema for temporary tables
#' @param verbose Print progress messages
#'
#' @return Invisible NULL
#' @noRd
executeSqlPlan <- function(connection, params, targetDialect, tempEmulationSchema, 
                           verbose = TRUE) {
  
  # Log start
  logMessage("Starting SQL plan execution", verbose,  "INFO")
  
  # Get SQL file path - Fixed case sensitivity
  sqlPath <- system.file("sql", "MainCostUtilization.sql", 
                         package = "CostUtilization", mustWork = TRUE)
  
  if (!file.exists(sqlPath)) {
    cli::cli_abort("SQL file not found at: {sqlPath}")
  }
  
  # Read SQL
  logMessage("Reading SQL template", verbose,  "DEBUG")
  sql <- SqlRender::readSql(sqlPath)
  
  # Prepare parameters for rendering
  renderParams <- prepareSqlRenderParams(params, tempEmulationSchema)
  
  # Render SQL
  logMessage("Rendering SQL with parameters", verbose,  "DEBUG")
  sql <- SqlRender::render(
    sql = sql,
    warnOnMissingParameters = FALSE,
    .params = renderParams
  )
  
  # Translate SQL
  logMessage(sprintf("Translating SQL to %s dialect", targetDialect), 
             verbose,  "DEBUG")
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = targetDialect,
    tempEmulationSchema = tempEmulationSchema
  )
  
  # Split SQL into statements
  sqlStatements <- SqlRender::splitSql(sql)
  logMessage(sprintf("Executing %d SQL statements", length(sqlStatements)), 
             verbose,  "INFO")
  
  # Execute statements
  executeSqlStatements(
    connection = connection,
    sqlStatements = sqlStatements,
    verbose = verbose,
    logger = logger
  )
  
  logMessage("SQL plan execution completed", verbose,  "INFO")
  invisible(NULL)
}

#' Prepare SQL Render Parameters
#'
#' @description
#' Prepares parameters for SQL rendering, handling schema qualifications
#'
#' @param params Original parameters list
#' @param tempEmulationSchema Temp emulation schema
#'
#' @return List of parameters ready for SQL rendering
#' @noRd
prepareSqlRenderParams <- function(params, tempEmulationSchema) {
  # Copy params to avoid modifying original
  renderParams <- params
  
  # Handle schema-qualified table names
  if (!is.null(tempEmulationSchema)) {
    # Qualify helper tables with temp schema
    if (!is.null(renderParams$restrictVisitTable)) {
      renderParams$restrictVisitTable <- paste(tempEmulationSchema, 
                                               renderParams$restrictVisitTable, 
                                               sep = ".")
    }
    
    if (!is.null(renderParams$eventConceptsTable)) {
      renderParams$eventConceptsTable <- paste(tempEmulationSchema, 
                                               renderParams$eventConceptsTable, 
                                               sep = ".")
    }
    
    # Qualify results tables
    renderParams$resultsTable <- paste(tempEmulationSchema, 
                                       renderParams$resultsTable, 
                                       sep = ".")
    renderParams$diagTable <- paste(tempEmulationSchema, 
                                    renderParams$diagTable, 
                                    sep = ".")
  }
  
  # Add computed parameters
  renderParams$windowStartDays <- renderParams$startOffsetDays
  renderParams$windowEndDays <- renderParams$endOffsetDays
  
  # Handle anchor column mapping
  renderParams$anchorDateColumn <- renderParams$anchorCol
  
  # Set flags for conditional SQL sections
  renderParams$useVisitRestriction <- renderParams$hasVisitRestriction
  renderParams$useEventFilters <- renderParams$hasEventFilters
  renderParams$useMicroCosting <- renderParams$microCosting
  
  # Add default values for optional parameters
  renderParams$costDomainId <- ifelse(
    renderParams$microCosting,
    "'Visit Detail'",
    "'Visit'"
  )
  
  return(renderParams)
}

#' Execute SQL Statements
#'
#' @description
#' Executes a list of SQL statements with error handling and progress tracking
#'
#' @param connection DatabaseConnector connection object
#' @param sqlStatements Vector of SQL statements
#' @param verbose Print progress messages
#' @param logger Optional logger object
#'
#' @return Invisible NULL
#' @noRd
executeSqlStatements <- function(connection, sqlStatements, verbose, logger) {
  totalStatements <- length(sqlStatements)
  
  # Progress tracking
  progressCallback <- createProgressCallback(totalStatements, verbose, logger)
  
  for (i in seq_along(sqlStatements)) {
    statement <- sqlStatements[i]
    
    # Skip empty statements
    if (nchar(trimws(statement)) == 0) {
      next
    }
    
    # Update progress
    progressCallback(i, statement)
    
    # Execute with error handling
    tryCatch(
      {
        DatabaseConnector::executeSql(
          connection = connection,
          sql = statement,
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
      },
      error = function(e) {
        logMessage(
          sprintf("Error executing statement %d/%d: %s", i, totalStatements, e$message),
          verbose,  "ERROR"
        )
        
        # Log the failing SQL for debugging
        if (!is.null(logger)) {
          logger$log("DEBUG", sprintf("Failing SQL: %s", substr(statement, 1, 500)))
        }
        
        # Re-throw the error with context
        cli::cli_abort(
          "SQL execution failed at statement {i}/{totalStatements}: {e$message}"
        )
      }
    )
  }
  
  invisible(NULL)
}

#' Create Progress Callback
#'
#' @description
#' Creates a callback function for tracking SQL execution progress
#'
#' @param totalStatements Total number of statements
#' @param verbose Print progress messages
#' @param logger Optional logger object
#'
#' @return Function that reports progress
#' @noRd
createProgressCallback <- function(totalStatements, verbose, logger) {
  startTime <- Sys.time()
  
  function(currentStatement, sql) {
    if (verbose && (currentStatement == 1 || 
                    currentStatement %% 10 == 0 || 
                    currentStatement == totalStatements)) {
      
      elapsed <- as.numeric(difftime(Sys.time(), startTime, units = "secs"))
      
      # Extract statement type
      statementType <- getStatementType(sql)
      
      message <- sprintf(
        "Executing statement %d/%d (%s) - Elapsed: %.1fs",
        currentStatement,
        totalStatements,
        statementType,
        elapsed
      )
      
      logMessage(message, verbose,  "DEBUG")
    }
  }
}

#' Get SQL Statement Type
#'
#' @description
#' Extracts the type of SQL statement for logging purposes
#'
#' @param sql SQL statement
#'
#' @return Character string describing statement type
#' @noRd
getStatementType <- function(sql) {
  sql_trimmed <- trimws(toupper(substr(sql, 1, 50)))
  
  if (grepl("^CREATE", sql_trimmed)) {
    if (grepl("TEMP", sql_trimmed)) {
      return("CREATE TEMP TABLE")
    } else {
      return("CREATE TABLE")
    }
  } else if (grepl("^DROP", sql_trimmed)) {
    return("DROP TABLE")
  } else if (grepl("^INSERT", sql_trimmed)) {
    return("INSERT")
  } else if (grepl("^UPDATE", sql_trimmed)) {
    return("UPDATE")
  } else if (grepl("^DELETE", sql_trimmed)) {
    return("DELETE")
  } else if (grepl("^SELECT", sql_trimmed)) {
    return("SELECT")
  } else if (grepl("^WITH", sql_trimmed)) {
    return("CTE")
  } else {
    return("OTHER")
  }
}

#' Build Event Filter SQL
#'
#' @description
#' Builds SQL conditions for event filters
#'
#' @param eventFilters List of event filters
#' @param tableAlias Table alias to use in SQL
#'
#' @return Character string with SQL conditions
#' @noRd
buildEventFilterSql <- function(eventFilters, tableAlias = "e") {
  if (is.null(eventFilters) || length(eventFilters) == 0) {
    return("")
  }
  
  filterConditions <- purrr::map_chr(seq_along(eventFilters), function(i) {
    filter <- eventFilters[[i]]
    
    # Build domain condition
    domainCondition <- buildDomainCondition(filter$domain, tableAlias)
    
    # Build concept condition
    conceptCondition <- sprintf(
      "%s.concept_id IN (SELECT concept_id FROM event_concepts WHERE filter_id = %d)",
      tableAlias, i
    )
    
    # Combine conditions
    if (nchar(domainCondition) > 0) {
      sprintf("(%s AND %s)", domainCondition, conceptCondition)
    } else {
      conceptCondition
    }
  })
  
  # Join all filter conditions with OR
  paste(filterConditions, collapse = " OR ")
}

#' Build Domain Condition
#'
#' @description
#' Builds SQL condition for domain filtering
#'
#' @param domain Domain name
#' @param tableAlias Table alias
#'
#' @return SQL condition string
#' @noRd
buildDomainCondition <- function(domain, tableAlias) {
  domainMap <- list(
    "Condition" = "condition_occurrence",
    "Procedure" = "procedure_occurrence",
    "Drug" = "drug_exposure",
    "Measurement" = "measurement",
    "Observation" = "observation"
  )
  
  if (domain == "All") {
    return("")
  }
  
  if (domain %in% names(domainMap)) {
    return(sprintf("%s.domain_table = '%s'", tableAlias, domainMap[[domain]]))
  }
  
  cli::cli_warn("Unknown domain: {domain}")
  return("")
}

#' Optimize SQL for Database
#'
#' @description
#' Applies database-specific optimizations to SQL
#'
#' @param sql SQL string
#' @param targetDialect Target database dialect
#'
#' @return Optimized SQL string
#' @noRd
optimizeSqlForDatabase <- function(sql, targetDialect) {
  dialect_lower <- tolower(targetDialect)
  
  if (dialect_lower == "redshift") {
    # Add distribution keys for better performance
    sql <- gsub(
      "CREATE TABLE (\\w+) \\(",
      "CREATE TABLE \\1 DISTSTYLE KEY DISTKEY(person_id) (",
      sql
    )
  } else if (dialect_lower == "bigquery") {
    # Add clustering for better performance
    sql <- gsub(
      "CREATE TABLE (\\w+) \\(",
      "CREATE TABLE \\1 CLUSTER BY person_id (",
      sql
    )
  } else if (dialect_lower == "spark") {
    # Add bucketing for better performance
    sql <- gsub(
      "CREATE TABLE (\\w+) \\(",
      "CREATE TABLE \\1 USING PARQUET CLUSTERED BY (person_id) INTO 10 BUCKETS (",
      sql
    )
  }
  
  return(sql)
}

#' Validate SQL Execution Environment
#'
#' @description
#' Validates that the SQL execution environment is properly configured
#'
#' @param connection DatabaseConnector connection object
#' @param params SQL parameters
#' @param targetDialect Target dialect
#'
#' @return Invisible TRUE if valid
#' @noRd
validateSqlEnvironment <- function(connection, params, targetDialect) {
  # Check connection
  if (!DatabaseConnector::dbIsValid(connection)) {
    cli::cli_abort("Database connection is invalid")
  }
  
  # Check required schemas exist
  schemas <- unique(c(
    params$cdmDatabaseSchema,
    params$cohortDatabaseSchema
  ))
  
  for (schema in schemas) {
    if (!checkSchemaExists(connection, schema)) {
      cli::cli_abort("Schema '{schema}' does not exist or is not accessible")
    }
  }
  
  # Check dialect support
  supportedDialects <- c(
    "oracle", "postgresql", "pdw", "redshift", "impala", 
    "netezza", "bigquery", "sql server", "spark", "snowflake"
  )
  
  if (!tolower(targetDialect) %in% supportedDialects) {
    cli::cli_warn(
      "Dialect '{targetDialect}' may not be fully supported. Supported dialects: {paste(supportedDialects, collapse = ', ')}"
    )
  }
  
  invisible(TRUE)
}

#' Create Execution Summary
#'
#' @description
#' Creates a summary of the SQL execution for logging/debugging
#'
#' @param params SQL parameters
#' @param targetDialect Target dialect
#' @param startTime Execution start time
#' @param endTime Execution end time
#'
#' @return List with execution summary
#' @noRd
createExecutionSummary <- function(params, targetDialect, startTime, endTime) {
  summary <- list(
    executionTime = difftime(endTime, startTime, units = "secs"),
    targetDialect = targetDialect,
    parameters = list(
      cohortId = params$cohortId,
      anchorCol = params$anchorCol,
      timeWindow = sprintf("%d to %d days", params$startOffsetDays, params$endOffsetDays),
      microCosting = params$microCosting,
      hasVisitRestriction = params$hasVisitRestriction,
      hasEventFilters = params$hasEventFilters,
      nFilters = params$nFilters
    ),
    tables = list(
      source = sprintf("%s.%s", params$cohortDatabaseSchema, params$cohortTable),
      results = params$resultsTable,
      diagnostics = params$diagTable
    )
  )
  
  class(summary) <- c("cost_analysis_summary", "list")
  return(summary)
}

#' Print Cost Analysis Summary
#'
#' @description
#' Print method for cost analysis summary
#'
#' @param x Cost analysis summary object
#' @param ... Additional arguments (ignored)
#'
#' @export
print.cost_analysis_summary <- function(x, ...) {
  cli::cli_h2("Cost Analysis Execution Summary")
  cli::cli_text("Execution time: {round(x$executionTime, 2)} seconds")
  cli::cli_text("Target dialect: {x$targetDialect}")
  
  cli::cli_h3("Parameters")
  cli::cli_ul()
  cli::cli_li("Cohort ID: {x$parameters$cohortId}")
  cli::cli_li("Anchor: {x$parameters$anchorCol}")
  cli::cli_li("Time window: {x$parameters$timeWindow}")
  cli::cli_li("Micro-costing: {x$parameters$microCosting}")
  if (x$parameters$hasEventFilters) {
    cli::cli_li("Event filters: {x$parameters$nFilters} filters applied")
  }
  cli::cli_end()
  
  cli::cli_h3("Tables")
  cli::cli_ul()
  cli::cli_li("Source: {x$tables$source}")
  cli::cli_li("Results: {x$tables$results}")
  cli::cli_li("Diagnostics: {x$tables$diagnostics}")
  cli::cli_end()
  
  invisible(x)
}

#' Log Message Helper
#'
#' @description
#' Logs a message using cli and optional logger
#'
#' @param message Message to log
#' @param verbose Print to console
#' @param logger Optional logger object
#' @param level Log level
#'
#' @return Invisible NULL
#' @noRd
logMessage <- function(message, verbose,  level = "INFO") {
  if (verbose) {
    if (level == "ERROR") {
      cli::cli_alert_danger(message)
    } else if (level == "WARNING") {
      cli::cli_alert_warning(message)
    } else if (level == "INFO") {
      cli::cli_alert_info(message)
    } else if (level == "SUCCESS") {
      cli::cli_alert_success(message)
    } else {
      cli::cli_alert(message)
    }
  }
  
  if (!is.null(logger) && is.function(logger$log)) {
    logger$log(level, message)
  }
  
  invisible(NULL)
}