#' Calculate Cost of Care Analysis
#'
#' @description
#' Performs cost-of-care analysis for a specified cohort, calculating total costs
#' and utilization rates within a defined time window.
#'
#' @param connection DatabaseConnector connection object
#' @param connectionDetails         An R object of type `connectionDetails` created by the
#'                                  `DatabaseConnector::createConnectionDetails` function.
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
#' @param tempEmulationSchema Schema for temporary tables (if needed)
#' @param verbose Print progress messages?
#'
#' @return
#' Andromeda object with results
#'
#' @examples
#' \dontrun{
#' connection <- DatabaseConnector::connect(connectionDetails)
#' 
#' results <- calculateCostOfCare(
#'   connection = connection,
#'   cdmDatabaseSchema = "cdm_schema",
#'   cohortDatabaseSchema = "results_schema",
#'   cohortTable = "cohort",
#'   cohortId = 1,
#'   anchorCol = "cohort_start_date",
#'   startOffsetDays = -365,
#'   endOffsetDays = 0
#' )
#' 
#' DatabaseConnector::disconnect(connection)
#' }
#'
#' @export
calculateCostOfCare <- function(
    connection = NULL,
    connectionDetails = NULL,
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
    tempEmulationSchema = NULL,
    verbose = TRUE
    ) {
  
  # Start timing
  startTime <- Sys.time()
  
  # Handle connection/connectionDetails
  if (is.null(connectionDetails) && is.null(connection)) {
    cli::cli_abort("Need to provide either connectionDetails or connection")
  }
  if (!is.null(connectionDetails) && !is.null(connection)) {
    cli::cli_abort("Need to provide either connectionDetails or connection, not both")
  }
  if (!is.null(connectionDetails)) {
    checkmate::assertClass(connectionDetails, "ConnectionDetails")
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  } else {
    checkmate::assertClass(connection, "DatabaseConnectorJdbcConnection")
  }
  # Validate and process arguments
  args <- processArguments(
    anchorCol = anchorCol,
    returnFormat = returnFormat,
    targetDialect = targetDialect,
    microCosting = microCosting,
    cdmDatabaseSchema = cdmDatabaseSchema,
    eventFilters = eventFilters,
    primaryEventFilterName = primaryEventFilterName
  )
  
  # Log start
  logMessage("Starting cost of care analysis", verbose,  "INFO")
  
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
        verbose,  "INFO"
      )
    },
    add = TRUE
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
    verbose = verbose
  )
  
  # Retrieve and format results
  results <- retrieveResults(
    connection = connection,
    execEnv = execEnv,
    returnFormat = args$returnFormat,
    verbose = verbose
  )
  
  return(results)
}

# Helper function to process and validate arguments
processArguments <- function(
    anchorCol, 
    returnFormat, 
    targetDialect, 
    microCosting, 
    cdmDatabaseSchema, 
    eventFilters,
   primaryEventFilterName
   ) {
  # Match arguments
  anchorCol <- rlang::arg_match(anchorCol)
  returnFormat <- rlang::arg_match(returnFormat)

  # Validate inputs
  validateCostOfCareInputs(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    microCosting = microCosting,
    eventFilters = eventFilters,
    primaryEventFilterName = primaryEventFilterName
  )
  list(
    anchorCol = anchorCol,
    primaryFilterId = primaryFilterId
  )
}

# Create execution environment with table names and cleanup tracking
createExecutionEnvironment <- function(connection, tempEmulationSchema, asPermanent,
                                       verbose) {
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
  return(env)
}

# Prepare all SQL parameters
prepareSqlParameters <- function(
    cdmDatabaseSchema, 
    cohortDatabaseSchema, 
    cohortTable, 
    cohortId,
    anchorCol, 
    startOffsetDays, 
    endOffsetDays,
    costConceptId, 
    currencyConceptId,
    restrictVisitConceptIds, 
    eventFilters,
    microCosting, 
    primaryFilterId,
    helperTables, execEnv
    ) {
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
  logMessage("Executing cost analysis SQL", verbose,  "INFO")
  
  # Read and render SQL - Fixed case sensitivity
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
      logMessage(paste("SQL execution error:", e$message), verbose,  "ERROR")
      stop(e)
    }
  )
  
  logMessage("SQL execution completed successfully", verbose,  "INFO")
}

# Retrieve and format results
retrieveResults <- function(connection, execEnv, returnFormat, verbose, logger) {
  logMessage("Retrieving results", verbose,  "DEBUG")
  
  # Retrieve main results
  resultsSql <- sprintf("SELECT * FROM %s", execEnv$resultsTable)
  results <- DatabaseConnector::querySql(connection, resultsSql) |>
    dplyr::as_tibble() |>
    standardizeColumnNames()
  
  # Retrieve diagnostics
  diagSql <- sprintf("SELECT * FROM %s", execEnv$diagTable)
  diagnostics <- DatabaseConnector::querySql(connection, diagSql) |>
    dplyr::as_tibble() |>
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
      verbose,  "INFO"
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



