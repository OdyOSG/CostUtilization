#' Calculate Cost of Care Analysis (Backward Compatible Wrapper)
#'
#' @description
#' This is a wrapper function that provides backward compatibility while transitioning
#' to the new split settings approach. It accepts both the old parameter style and
#' the new settings objects.
#'
#' @param connection DatabaseConnector connection object.
#' @param connectionDetails Alternative to connection - DatabaseConnector connection details.
#' @param cdmDatabaseSchema Schema name for CDM tables.
#' @param cohortDatabaseSchema Schema name for cohort table.
#' @param cohortTable Name of the cohort table.
#' @param cohortId Cohort definition ID to analyze.
#' @param costOfCareSettings Settings object created by `createCostOfCareSettings()`.
#' @param cpiAdjustmentSettings Settings object created by `createCpiAdjustmentSettings()`.
#' @param anchorCol Column to use as anchor for time window (legacy parameter).
#' @param startOffsetDays Days to add to anchor date for window start (legacy parameter).
#' @param endOffsetDays Days to add to anchor date for window end (legacy parameter).
#' @param restrictVisitConceptIds Optional vector of visit concept IDs (legacy parameter).
#' @param eventFilters Optional list of event filters (legacy parameter).
#' @param microCosting Logical; if TRUE, performs micro-costing (legacy parameter).
#' @param primaryEventFilterName For micro-costing, name of primary filter (legacy parameter).
#' @param costConceptId Concept ID for cost type (legacy parameter).
#' @param currencyConceptId Concept ID for currency (legacy parameter).
#' @param cpiAdjustment Logical; if TRUE, adjusts costs for inflation (legacy parameter).
#' @param cpiTargetYear Target year for CPI standardization (legacy parameter).
#' @param cpiDataPath Path to custom CPI data CSV (legacy parameter).
#' @param resultsDatabaseSchema Schema where result tables will be created.
#' @param resultsTableName Name of the main results table.
#' @param diagnosticsTableName Name of the diagnostics table.
#' @param tempEmulationSchema Schema for temporary tables (if needed).
#' @param returnFormat Return results as "tibble" or "list" (includes diagnostics).
#' @param verbose Print progress messages.
#' @param targetDialect SQL dialect to use (auto-detected if NULL).
#' @param asPermanent Keep helper tables after execution?
#' @param logger Optional logger object for detailed logging.
#'
#' @return If returnFormat = "tibble": A tibble with cost and utilization metrics.
#'         If returnFormat = "list": A list with results and diagnostics tibbles.
#'
#' @examples
#' \dontrun{
#' # New approach with settings objects
#' costSettings <- createCostOfCareSettings(
#'   anchorCol = "cohort_start_date",
#'   startOffsetDays = -365,
#'   endOffsetDays = 0
#' )
#' 
#' cpiSettings <- createCpiAdjustmentSettings(
#'   enabled = TRUE,
#'   targetYear = 2023
#' )
#' 
#' results <- calculateCostOfCare(
#'   connection = connection,
#'   cdmDatabaseSchema = "cdm",
#'   cohortDatabaseSchema = "results",
#'   cohortTable = "cohort",
#'   cohortId = 1,
#'   costOfCareSettings = costSettings,
#'   cpiAdjustmentSettings = cpiSettings,
#'   resultsDatabaseSchema = "results"
#' )
#' 
#' # Legacy approach (still supported)
#' results <- calculateCostOfCare(
#'   connection = connection,
#'   cdmDatabaseSchema = "cdm",
#'   cohortDatabaseSchema = "results",
#'   cohortTable = "cohort",
#'   cohortId = 1,
#'   anchorCol = "cohort_start_date",
#'   startOffsetDays = -365,
#'   endOffsetDays = 0,
#'   cpiAdjustment = TRUE,
#'   cpiTargetYear = 2023
#' )
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
    costOfCareSettings = NULL,
    cpiAdjustmentSettings = NULL,
    # Legacy parameters
    anchorCol = NULL,
    startOffsetDays = NULL,
    endOffsetDays = NULL,
    restrictVisitConceptIds = NULL,
    eventFilters = NULL,
    microCosting = NULL,
    primaryEventFilterName = NULL,
    costConceptId = NULL,
    currencyConceptId = NULL,
    cpiAdjustment = NULL,
    cpiTargetYear = NULL,
    cpiDataPath = NULL,
    # Common parameters
    resultsDatabaseSchema = cohortDatabaseSchema,
    resultsTableName = "cost_util_results",
    diagnosticsTableName = "cost_util_diag",
    tempEmulationSchema = NULL,
    returnFormat = c("tibble", "list"),
    verbose = TRUE,
    targetDialect = NULL,
    asPermanent = FALSE,
    logger = NULL
) {
  
  # Handle connection
  if (is.null(connection)) {
    if (is.null(connectionDetails)) {
      cli::cli_abort("Either {.arg connection} or {.arg connectionDetails} must be provided.")
    }
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  }
  
  returnFormat <- rlang::arg_match(returnFormat)
  
  # Check if using new settings approach or legacy parameters
  usingNewApproach <- !is.null(costOfCareSettings)
  
  if (!usingNewApproach) {
    # Create settings from legacy parameters
    if (verbose) {
      cli::cli_alert_info(
        "Using legacy parameter style. Consider using {.fn createCostOfCareSettings} for cleaner code."
      )
    }
    
    # Set defaults for legacy parameters
    anchorCol <- anchorCol %||% "cohort_start_date"
    startOffsetDays <- startOffsetDays %||% 0L
    endOffsetDays <- endOffsetDays %||% 365L
    microCosting <- microCosting %||% FALSE
    costConceptId <- costConceptId %||% 31978L
    currencyConceptId <- currencyConceptId %||% 44818668L
    
    # Create cost of care settings
    costOfCareSettings <- createCostOfCareSettings(
      anchorCol = anchorCol,
      startOffsetDays = startOffsetDays,
      endOffsetDays = endOffsetDays,
      restrictVisitConceptIds = restrictVisitConceptIds,
      eventFilters = eventFilters,
      microCosting = microCosting,
      primaryEventFilterName = primaryEventFilterName,
      costConceptId = costConceptId,
      currencyConceptId = currencyConceptId
    )
    
    # Create CPI adjustment settings if needed
    if (!is.null(cpiAdjustment) && cpiAdjustment) {
      cpiAdjustmentSettings <- createCpiAdjustmentSettings(
        enabled = TRUE,
        targetYear = cpiTargetYear,
        dataPath = cpiDataPath
      )
    }
  }
  
  # Validate settings
  costOfCareSettings <- validateCostOfCareSettings(costOfCareSettings)
  if (!is.null(cpiAdjustmentSettings)) {
    cpiAdjustmentSettings <- validateCpiAdjustmentSettings(cpiAdjustmentSettings)
  }
  
  # Call the internal implementation
  results <- calculateCostOfCareInternal(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = cohortId,
    costOfCareSettings = costOfCareSettings,
    cpiAdjustmentSettings = cpiAdjustmentSettings,
    resultsDatabaseSchema = resultsDatabaseSchema,
    resultsTableName = resultsTableName,
    diagnosticsTableName = diagnosticsTableName,
    tempEmulationSchema = tempEmulationSchema,
    targetDialect = targetDialect,
    asPermanent = asPermanent,
    verbose = verbose,
    logger = logger
  )
  
  # Format results based on returnFormat
  if (returnFormat == "tibble") {
    return(results$results)
  } else {
    return(results)
  }
}

#' Internal implementation of calculateCostOfCare
#'
#' @description
#' This is the actual implementation that uses the settings objects.
#'
#' @keywords internal
calculateCostOfCareInternal <- function(
    connection,
    cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable,
    cohortId,
    costOfCareSettings,
    cpiAdjustmentSettings,
    resultsDatabaseSchema,
    resultsTableName,
    diagnosticsTableName,
    tempEmulationSchema,
    targetDialect,
    asPermanent,
    verbose,
    logger
) {
  
  # Input validation
  checkmate::assertClass(connection, "DatabaseConnectorConnection")
  checkmate::assertClass(costOfCareSettings, "CostOfCareSettings")
  if (!is.null(cpiAdjustmentSettings)) {
    checkmate::assertClass(cpiAdjustmentSettings, "CpiAdjustmentSettings")
  }
  checkmate::assertString(cdmDatabaseSchema)
  checkmate::assertString(cohortDatabaseSchema)
  checkmate::assertString(cohortTable)
  checkmate::assertInt(cohortId)
  checkmate::assertString(resultsDatabaseSchema)
  checkmate::assertString(resultsTableName)
  checkmate::assertString(diagnosticsTableName)
  checkmate::assertFlag(verbose)
  checkmate::assertFlag(asPermanent)
  
  # Setup
  startTime <- Sys.time()
  targetDialect <- targetDialect %||% DatabaseConnector::dbms(connection)
  
  # Log start
  logMessage(
    glue::glue("Starting cost of care analysis for cohort {cohortId}"),
    verbose = verbose,
    logger = logger,
    level = "INFO"
  )
  
  # Create fully qualified table names
  resultsTable <- paste(resultsDatabaseSchema, resultsTableName, sep = ".")
  diagTable <- paste(resultsDatabaseSchema, diagnosticsTableName, sep = ".")
  
  # Prepare helper tables
  helperTables <- uploadHelperTables(
    connection = connection,
    settings = costOfCareSettings,
    tempEmulationSchema = tempEmulationSchema
  )
  
  if (!asPermanent) {
    on.exit(
      cleanupTempTables(connection, tempEmulationSchema, helperTables),
      add = TRUE
    )
  }
  
  # Handle CPI adjustment
  cpiTable <- NULL
  if (!is.null(cpiAdjustmentSettings) && cpiAdjustmentSettings$enabled) {
    cpiTable <- uploadCpiTable(
      connection = connection,
      cpiAdjustmentSettings = cpiAdjustmentSettings,
      tempEmulationSchema = tempEmulationSchema
    )
    helperTables$cpiTable <- cpiTable
  }
  
  # Prepare SQL parameters
  sqlParams <- c(
    list(
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortId = cohortId,
      resultsTable = resultsTable,
      diagTable = diagTable,
      cpiAdjustment = !is.null(cpiAdjustmentSettings) && cpiAdjustmentSettings$enabled,
      cpiTargetYear = if (!is.null(cpiAdjustmentSettings) && cpiAdjustmentSettings$enabled) {
        cpiAdjustmentSettings$targetYear
      } else {
        NA
      },
      cpiTable = cpiTable
    ),
    as.list(costOfCareSettings),
    helperTables
  )
  
  # Execute analysis
  executeSqlPlan(
    connection = connection,
    params = sqlParams,
    targetDialect = targetDialect,
    tempEmulationSchema = tempEmulationSchema,
    verbose = verbose,
    logger = logger
  )
  
  # Fetch results
  results <- list(
    results = fetchTable(connection, resultsTable),
    diagnostics = fetchTable(connection, diagTable)
  )
  
  # Add metadata
  results$metadata <- list(
    analysisTime = Sys.time(),
    executionDuration = difftime(Sys.time(), startTime, units = "secs"),
    cohortId = cohortId,
    costOfCareSettings = costOfCareSettings,
    cpiAdjustmentSettings = cpiAdjustmentSettings
  )
  
  # Add CPI adjustment metadata if applied
  if (!is.null(cpiAdjustmentSettings) && cpiAdjustmentSettings$enabled) {
    results$metadata$cpiAdjustmentApplied <- TRUE
    results$metadata$cpiTargetYear <- cpiAdjustmentSettings$targetYear
    results$metadata$cpiDataSource <- cpiAdjustmentSettings$dataPath %||% "package default"
  } else {
    results$metadata$cpiAdjustmentApplied <- FALSE
  }
  
  # Add summary to results
  class(results$results) <- c("cost_analysis_summary", class(results$results))
  
  logMessage(
    glue::glue(
      "Analysis complete in {round(results$metadata$executionDuration, 1)} seconds"
    ),
    verbose = verbose,
    logger = logger,
    level = "SUCCESS"
  )
  
  return(results)
}

# Utility function for NULL default
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}