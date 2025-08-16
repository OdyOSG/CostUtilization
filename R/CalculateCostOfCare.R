#' Calculate Cost of Care Analysis
#'
#' @description
#' Performs cost-of-care analysis for a specified cohort, calculating total costs
#' and utilization rates within a defined time window.
#'
#' @param connection DatabaseConnector connectionection object
#' @param cdmDatabaseSchema Schema name for CDM tables
#' @param cohortDatabaseSchema Schema name for cohort table
#' @param cohortTable Name of the cohort table
#' @param cohortId Cohort definition ID to analyze
#' @param anchorCol Column to use as anchor for time window ("cohort_start_date" or "cohort_end_date")
#' @param startOffsetDays Days to add to anchor date for window start (can be negative)
#' @param endOffsetDays Days to add to anchor date for window end
#' @param restrictVisitConceptIds Optional vector of visit concept IDs to restrict analysis
#' @param eventFilters Optional list of event filters, each with name, domain, and conceptIds
#' @param microCosting Logical; if TRUE, performs line-level costing at visit detail level
#' @param costConceptId Concept ID for cost type (default: 31978 for total charge)
#' @param currencyConceptId Concept ID for currency (default: 44818668 for USD)
#' @param primaryEventFilterName For micro-costing, name of primary event filter
#' @param targetDialect SQL dialect to use (auto-detected if NULL)
#' @param tempEmulationSchema Schema for temporary tables (if needed)
#' @param asPermanent Keep helper tables after execution?
#' @param returnFormat Return results as "tibble" or "list" (includes diagnostics)
#' @param verbose Print progress messages?
#' @param logger Optional logger object for detailed logging
#'
#' @return
#' If returnFormat = "tibble": A tibble with cost and utilization metrics
#' If returnFormat = "list": A list with results and diagnostics tibbles
#'
#' @export
calculateCostOfCare <- function(
    connection,
    cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable,
    cohortId,
    anchorCol = c("cohort_start_date", "cohort_end_date"),
    startOffsetDays = 0L,
    endOffsetDays = 90L,
    restrictVisitConceptIds = NULL,
    eventFilters = NULL,
    microCosting = FALSE,
    costConceptId = 31978L,
    currencyConceptId = 44818668L,
    primaryEventFilterName = NULL,
    targetDialect = NULL,
    tempEmulationSchema = NULL,
    asPermanent = FALSE,
    returnFormat = c("tibble", "list"),
    verbose = TRUE,
    logger = NULL) {
  # Start timing
  startTime <- Sys.time()

  # Validate and process arguments
  args <- processArguments(
    anchorCol = anchorCol,
    returnFormat = returnFormat,
    targetDialect = targetDialect,
    connection = connection,
    microCosting = microCosting,
    cdmDatabaseSchema = cdmDatabaseSchema,
    eventFilters = eventFilters,
    primaryEventFilterName = primaryEventFilterName
  )

  # Log start
  logMessage("Starting cost of care analysis", verbose, logger, "INFO")

  # Create execution environment
  execEnv <- createExecutionEnvironment(
    connection = connection,
    tempEmulationSchema = tempEmulationSchema,
    asPermanent = asPermanent,
    verbose = verbose,
    logger = logger
  )

  # Setup cleanup
  on.exit(
    {
      cleanupExecutionEnvironment(execEnv, connection)
      logMessage(
        sprintf(
          "Analysis completed in %.2f seconds",
          as.numeric(difftime(Sys.time(), startTime, units = "secs"))
        ),
        verbose, logger, "INFO"
      )
    },
    add = TRUE
  )

  # Materialize helper tables if needed
  helperTables <- materializeHelperTables(
    connection = connection,
    restrictVisitConceptIds = restrictVisitConceptIds,
    eventFilters = eventFilters,
    execEnv = execEnv,
    verbose = verbose,
    logger = logger
  )

  # Prepare SQL parameters
  sqlParams <- prepareSqlParameters(
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = cohortId,
    anchorCol = args$anchorCol,
    startOffsetDays = startOffsetDays,
    endOffsetDays = endOffsetDays,
    costConceptId = costConceptId,
    currencyConceptId = currencyConceptId,
    restrictVisitConceptIds = restrictVisitConceptIds,
    eventFilters = eventFilters,
    microCosting = microCosting,
    primaryFilterId = args$primaryFilterId,
    helperTables = helperTables,
    execEnv = execEnv
  )

  # Execute analysis
  executeCostAnalysis(
    connection = connection,
    sqlParams = sqlParams,
    targetDialect = args$targetDialect,
    tempEmulationSchema = tempEmulationSchema,
    verbose = verbose,
    logger = logger
  )

  # Retrieve and format results
  results <- retrieveResults(
    connection = connection,
    execEnv = execEnv,
    returnFormat = args$returnFormat,
    verbose = verbose,
    logger = logger
  )

  return(results)
}

# Helper function to process and validate arguments
processArguments <- function(anchorCol, returnFormat, targetDialect, connection,
                             microCosting, cdmDatabaseSchema, eventFilters,
                             primaryEventFilterName) {
  # Match arguments
  anchorCol <- rlang::arg_match(anchorCol)
  returnFormat <- rlang::arg_match(returnFormat)

  # Auto-detect dialect if needed
  if (is.null(targetDialect)) {
    targetDialect <- DatabaseConnector::dbms(connection)
  }

  # Validate inputs
  validateCostOfCareInputs(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    microCosting = microCosting,
    eventFilters = eventFilters,
    primaryEventFilterName = primaryEventFilterName
  )

  # Process primary filter ID for micro-costing
  primaryFilterId <- 1L
  if (microCosting && !is.null(eventFilters) && !is.null(primaryEventFilterName)) {
    primaryFilterId <- getPrimaryFilterId(eventFilters, primaryEventFilterName)
  }

  list(
    anchorCol = anchorCol,
    returnFormat = returnFormat,
    targetDialect = targetDialect,
    primaryFilterId = primaryFilterId
  )
}

# Create execution environment with table names and cleanup tracking
createExecutionEnvironment <- function(connection, tempEmulationSchema, asPermanent,
                                       verbose, logger) {
  tablePrefix <- paste0("cu_", paste(sample(letters, 6), collapse = ""))

  env <- list(
    tablePrefix = tablePrefix,
    resultsTable = paste0(tablePrefix, "_res"),
    diagTable = paste0(tablePrefix, "_diag"),
    restrictVisitTable = NULL,
    eventConceptsTable = NULL,
    tempEmulationSchema = tempEmulationSchema,
    asPermanent = asPermanent,
    tempTables = list()
  )

  # Pre-create results tables
  createResultsTables(connection, env, tempEmulationSchema)

  return(env)
}

# Cleanup execution environment
cleanupExecutionEnvironment <- function(execEnv, connection) {
  # Clean up SQL temp tables
  cleanupSqlTempTables(connection)

  # Clean up created tables if not permanent
  if (!execEnv$asPermanent) {
    tablesToClean <- c(
      execEnv$resultsTable,
      execEnv$diagTable,
      execEnv$restrictVisitTable,
      execEnv$eventConceptsTable
    )

    cleanupTempTables(
      connection,
      execEnv$tempEmulationSchema,
      tablesToClean
    )
  }
}

# Materialize helper tables efficiently
materializeHelperTables <- function(connection, restrictVisitConceptIds, eventFilters,
                                    execEnv, verbose, logger) {
  helperTables <- list()

  # Materialize visit restrictions
  if (!is.null(restrictVisitConceptIds)) {
    logMessage("Materializing visit concept restrictions", verbose, logger, "DEBUG")

    execEnv$restrictVisitTable <- paste0(execEnv$tablePrefix, "_visit")
    helperTables$restrictVisitTable <- execEnv$restrictVisitTable

    materializeConceptTable(
      connection = connection,
      conceptIds = restrictVisitConceptIds,
      tableName = execEnv$restrictVisitTable,
      columnName = "visit_concept_id",
      tempEmulationSchema = execEnv$tempEmulationSchema
    )
  }

  # Materialize event filters
  if (!is.null(eventFilters)) {
    logMessage("Materializing event filter concepts", verbose, logger, "DEBUG")

    execEnv$eventConceptsTable <- paste0(execEnv$tablePrefix, "_event")
    helperTables$eventConceptsTable <- execEnv$eventConceptsTable

    materializeEventFilterTable(
      connection = connection,
      eventFilters = eventFilters,
      tableName = execEnv$eventConceptsTable,
      tempEmulationSchema = execEnv$tempEmulationSchema
    )
  }

  return(helperTables)
}

# Prepare all SQL parameters
prepareSqlParameters <- function(cdmDatabaseSchema, cohortDatabaseSchema, cohortTable, cohortId,
                                 anchorCol, startOffsetDays, endOffsetDays,
                                 costConceptId, currencyConceptId,
                                 restrictVisitConceptIds, eventFilters,
                                 microCosting, primaryFilterId,
                                 helperTables, execEnv) {
  list(
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = cohortId,
    anchorCol = anchorCol,
    startOffsetDays = startOffsetDays,
    endOffsetDays = endOffsetDays,
    costConceptId = costConceptId,
    currencyConceptId = currencyConceptId,
    hasVisitRestriction = !is.null(restrictVisitConceptIds),
    restrictVisitTable = helperTables$restrictVisitTable,
    hasEventFilters = !is.null(eventFilters),
    eventConceptsTable = helperTables$eventConceptsTable,
    nFilters = length(eventFilters %||% list()),
    microCosting = microCosting,
    primaryFilterId = primaryFilterId,
    resultsTable = execEnv$resultsTable,
    diagTable = execEnv$diagTable
  )
}

# Execute the main cost analysis SQL
executeCostAnalysis <- function(connection, sqlParams, targetDialect,
                                tempEmulationSchema, verbose, logger) {
  logMessage("Executing cost analysis SQL", verbose, logger, "INFO")

  # Read and render SQL
  sql <- SqlRender::readSql(
    system.file("sql", "MainCostUtilization.sql", package = "CostUtilization")
  )

  sql <- SqlRender::render(sql, warnOnMissingParameters = FALSE, .params = sqlParams)
  sql <- SqlRender::translate(sql, targetDialect = targetDialect)

  # Split and execute
  sqlStatements <- SqlRender::splitSql(sql)

  withCallingHandlers(
    {
      for (i in seq_along(sqlStatements)) {
        if (nchar(trimws(sqlStatements[i])) > 0) {
          DatabaseConnector::executeSql(connection, sqlStatements[i])
        }
      }
    },
    error = function(e) {
      logMessage(paste("SQL execution error:", e$message), verbose, logger, "ERROR")
      stop(e)
    }
  )

  logMessage("SQL execution completed successfully", verbose, logger, "INFO")
}

# Retrieve and format results
retrieveResults <- function(connection, execEnv, returnFormat, verbose, logger) {
  logMessage("Retrieving results", verbose, logger, "DEBUG")

  # Retrieve main results
  resultsSql <- sprintf("SELECT * FROM %s", execEnv$resultsTable)
  results <- DatabaseConnector::querySql(connection, resultsSql) %>%
    dplyr::as_tibble() %>%
    standardizeColumnNames()

  # Retrieve diagnostics
  diagSql <- sprintf("SELECT * FROM %s", execEnv$diagTable)
  diagnostics <- DatabaseConnector::querySql(connection, diagSql) %>%
    dplyr::as_tibble() %>%
    standardizeColumnNames()

  # Log summary
  if (verbose && nrow(results) > 0) {
    logMessage(
      sprintf(
        "Analysis complete: %d persons, %.2f total cost, %.2f PPPM",
        diagnostics$n_persons[diagnostics$step_name == "00_initial_cohort"],
        results$total_cost[1],
        results$cost_pppm[1]
      ),
      verbose, logger, "INFO"
    )
  }

  # Return based on format
  if (returnFormat == "tibble") {
    return(results)
  } else {
    return(list(
      results = results,
      diagnostics = diagnostics,
      metadata = list(
        analysisTime = Sys.time(),
        parameters = as.list(match.call())
      )
    ))
  }
}

# Validate inputs
validateCostOfCareInputs <- function(connection, cdmDatabaseSchema, microCosting,
                                     eventFilters, primaryEventFilterName) {
  # Check connectionection
  if (!DatabaseConnector::dbIsValid(connection)) {
    stop("Invalid database connectionection")
  }

  # Check schema exists
  if (!schemaExists(connection, cdmDatabaseSchema)) {
    stop(sprintf("CDM schema '%s' does not exist", cdmDatabaseSchema))
  }

  # Check micro-costing prerequisites
  if (microCosting) {
    if (!tableExists(connection, cdmDatabaseSchema, "visit_detail")) {
      stop("Micro-costing requires visit_detail table in CDM")
    }

    if (!is.null(primaryEventFilterName) && is.null(eventFilters)) {
      stop("primaryEventFilterName specified but no eventFilters provided")
    }
  }

  # Validate event filters
  if (!is.null(eventFilters)) {
    validateEventFilters(eventFilters)
  }
}

# Get primary filter ID
getPrimaryFilterId <- function(eventFilters, primaryEventFilterName) {
  filterNames <- purrr::map_chr(eventFilters, "name")
  id <- which(filterNames == primaryEventFilterName)

  if (length(id) == 0) {
    stop(sprintf(
      "primaryEventFilterName '%s' not found in eventFilters",
      primaryEventFilterName
    ))
  }

  as.integer(id)
}

# Create results tables
createResultsTables <- function(connection, execEnv, tempEmulationSchema) {
  # Results table
  resultsSql <- SqlRender::render(
    "CREATE TABLE @table (
      total_person_days FLOAT,
      total_person_months FLOAT,
      total_person_years FLOAT,
      metric_type VARCHAR(50),
      total_cost FLOAT,
      n_persons_with_cost INT,
      distinct_visits INT,
      distinct_visit_dates INT,
      distinct_visit_details INT,
      cost_pppm FLOAT,
      visits_per_1000_py FLOAT,
      visit_dates_per_1000_py FLOAT,
      visit_details_per_1000_py FLOAT
    )",
    table = if (!is.null(tempEmulationSchema)) {
      paste(tempEmulationSchema, execEnv$resultsTable, sep = ".")
    } else {
      execEnv$resultsTable
    }
  )
  DatabaseConnector::executeSql(connection, resultsSql)

  # Diagnostics table
  diagSql <- SqlRender::render(
    "CREATE TABLE @table (
      step_name VARCHAR(255),
      n_persons BIGINT,
      n_events BIGINT
    )",
    table = if (!is.null(tempEmulationSchema)) {
      paste(tempEmulationSchema, execEnv$diagTable, sep = ".")
    } else {
      execEnv$diagTable
    }
  )
  DatabaseConnector::executeSql(connection, diagSql)
}

# Materialize concept table efficiently
materializeConceptTable <- function(connection, conceptIds, tableName, columnName,
                                    tempEmulationSchema) {
  # Create table
  createSql <- SqlRender::render(
    "CREATE TABLE @table (@column BIGINT)",
    table = if (!is.null(tempEmulationSchema)) {
      paste(tempEmulationSchema, tableName, sep = ".")
    } else {
      tableName
    },
    column = columnName
  )
  DatabaseConnector::executeSql(connection, createSql)

  # Insert in batches for better performance
  batchSize <- 1000
  batches <- split(conceptIds, ceiling(seq_along(conceptIds) / batchSize))

  for (batch in batches) {
    values <- paste(sprintf("(%s)", batch), collapse = ", ")
    insertSql <- SqlRender::render(
      "INSERT INTO @table (@column) VALUES @values",
      table = if (!is.null(tempEmulationSchema)) {
        paste(tempEmulationSchema, tableName, sep = ".")
      } else {
        tableName
      },
      column = columnName,
      values = values
    )
    DatabaseConnector::executeSql(connection, insertSql)
  }
}

# Materialize event filter table
materializeEventFilterTable <- function(connection, eventFilters, tableName,
                                        tempEmulationSchema) {
  # Create table
  createSql <- SqlRender::render(
    "CREATE TABLE @table (
      filter_id INT,
      filter_name VARCHAR(255),
      domain_scope VARCHAR(50),
      concept_id BIGINT
    )",
    table = if (!is.null(tempEmulationSchema)) {
      paste(tempEmulationSchema, tableName, sep = ".")
    } else {
      tableName
    }
  )
  DatabaseConnector::executeSql(connection, createSql)

  # Prepare data for batch insert
  filterData <- purrr::imap_dfr(eventFilters, function(filter, idx) {
    dplyr::tibble(
      filter_id = idx,
      filter_name = filter$name,
      domain_scope = filter$domain,
      concept_id = filter$conceptIds
    )
  })

  # Insert data
  DatabaseConnector::insertTable(
    connectionection = connection,
    tableName = tableName,
    databaseSchema = tempEmulationSchema,
    data = filterData,
    createTable = FALSE,
    tempTable = is.null(tempEmulationSchema)
  )
}

# Standardize column names from database
standardizeColumnNames <- function(df) {
  names(df) <- tolower(names(df))
  names(df) <- gsub("_", "", names(df), fixed = TRUE)
  df
}

# Logging helper
logMessage <- function(message, verbose, logger, level = "INFO") {
  if (verbose) {
    cat(sprintf("[%s] %s: %s\n", Sys.time(), level, message))
  }

  if (!is.null(logger) && is.function(logger$log)) {
    logger$log(level, message)
  }
}

# Check if schema exists
schemaExists <- function(connection, schema) {
  tryCatch(
    {
      # Try to query information schema or equivalent
      sql <- "SELECT 1"
      DatabaseConnector::renderTranslateQuerySql(
        connection,
        sql,
        schema = schema
      )
      TRUE
    },
    error = function(e) {
      FALSE
    }
  )
}

# Check if table exists
tableExists <- function(connection, schema, table) {
  tryCatch(
    {
      sql <- sprintf("SELECT 1 FROM %s.%s WHERE 1=0", schema, table)
      DatabaseConnector::querySql(connection, sql)
      TRUE
    },
    error = function(e) {
      FALSE
    }
  )
}
