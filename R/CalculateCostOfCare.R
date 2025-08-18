#' Calculate Cost of Care Analysis
#'
#' @description
#' Performs cost-of-care analysis for a specified cohort.
#'
#' @param connection A `DatabaseConnector` connection object.
#' @param cdmDatabaseSchema Schema name for CDM tables.
#' @param cohortDatabaseSchema Schema name for the cohort table.
#' @param cohortTable Name of the cohort table.
#' @param cohortId The cohort definition ID to analyze.
#' @param costOfCareSettings A settings object created by `createCostOfCareSettings()`.
#' @param resultsDatabaseSchema Schema where result tables will be created.
#' @param resultsTableName Name of the main results table.
#' @param diagnosticsTableName Name of the diagnostics table.
#' @param tempEmulationSchema Schema for temporary tables (if needed).
#' @param verbose Print progress messages.
#'
#' @return A list containing two tibbles: `results` and `diagnostics`.
#' @export
calculateCostOfCare <- function(
    connection,
    cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable,
    cohortId,
    costOfCareSettings,
    resultsDatabaseSchema,
    resultsTableName = "cost_util_results",
    diagnosticsTableName = "cost_util_diag",
    tempEmulationSchema = NULL,
    verbose = TRUE
) {
  
  # --- Input Validation ---
  checkmate::assertClass(connection, "DatabaseConnectorConnection")
  checkmate::assertClass(costOfCareSettings, "CostOfCareSettings")
  checkmate::assertString(cdmDatabaseSchema)
  checkmate::assertString(cohortDatabaseSchema)
  checkmate::assertString(cohortTable)
  checkmate::assertInt(cohortId)
  checkmate::assertString(resultsDatabaseSchema)
  checkmate::assertString(resultsTableName)
  checkmate::assertString(diagnosticsTableName)
  
  # --- Setup ---
  startTime <- Sys.time()
  targetDialect <- DatabaseConnector::dbms(connection)
  
  # Create fully qualified table names
  resultsTable <- glue::glue("{resultsDatabaseSchema}.{resultsTableName}")
  diagTable <- glue::glue("{resultsDatabaseSchema}.{diagnosticsTableName}")
  
  # --- Prepare Helper Tables ---
  # Upload concept sets for event filters and visit restrictions to the database
  helperTables <- uploadHelperTables(
    connection = connection,
    settings = costOfCareSettings,
    tempEmulationSchema = tempEmulationSchema
  )
  on.exit(
    cleanupTempTables(connection, tempEmulationSchema, helperTables),
    add = TRUE
  )
  
  # --- Prepare SQL Parameters ---
  sqlParams <- c(
    list(
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortId = cohortId,
      resultsTable = resultsTable,
      diagTable = diagTable
    ),
    costOfCareSettings,
    helperTables
  )
  
  # --- Execute Analysis ---
  # This function is now in R/BuildAnalysis.R and is the single execution pathway
  executeSqlPlan(
    connection = connection,
    params = sqlParams,
    targetDialect = targetDialect,
    tempEmulationSchema = tempEmulationSchema,
    verbose = verbose
  )
  
  # --- Fetch and Return Results ---
  results <- list(
    results = fetchTable(connection, resultsTable),
    diagnostics = fetchTable(connection, diagTable)
  )
  
  logMessage(
    glue::glue("Analysis complete in {round(difftime(Sys.time(), startTime, units = 'secs'), 1)}s."),
    verbose = verbose,
    level = "SUCCESS"
  )
  
  return(results)
}

# Helper function to fetch results (can be placed in utils.R)
fetchTable <- function(connection, tableName) {
  sql <- "SELECT * FROM @table;"
  DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    table = tableName,
    snakeCaseToCamelCase = TRUE
  ) |>
    dplyr::as_tibble()
}

# Helper to upload concept sets (can be placed in utils.R)
uploadHelperTables <- function(connection, settings, tempEmulationSchema) {
  tables <- list()
  
  if (settings$hasVisitRestriction) {
    df <- tibble::tibble(visit_concept_id = settings$restrictVisitConceptIds)
    name <- paste0("visit_restr_", sample.int(1e6, 1))
    DatabaseConnector::insertTable(connection, name, df, tempEmulationSchema, tempTable = TRUE)
    tables$restrictVisitTable <- name
  }
  
  if (settings$hasEventFilters) {
    df <- purrr::imap_dfr(settings$eventFilters, ~{
      tibble::tibble(
        filter_id = .y,
        filter_name = .x$name,
        domain_scope = .x$domain,
        concept_id = .x$conceptIds
      )
    })
    name <- paste0("event_filt_", sample.int(1e6, 1))
    DatabaseConnector::insertTable(connection, name, df, tempEmulationSchema, tempTable = TRUE)
    tables$eventConceptsTable <- name
    
    if (settings$microCosting) {
      primaryFilter <- df |> dplyr::filter(filter_name == settings$primaryEventFilterName)
      tables$primaryFilterId <- primaryFilter$filter_id[1]
    }
    
  }
  
  
  
  # Execute analysis
  executeCostAnalysis(
    connection = connection,
    sqlParams = sqlParams,
    tempEmulationSchema = tempEmulationSchema,
    verbose = verbose
  )
  
}




executeCostAnalysis <- function(
    connection, 
    sqlParams, 
    targetDialect,
    tempEmulationSchema, 
    verbose = TRUE
    ) {
  logMessage("Executing cost analysis SQL", verbose,  "INFO")
  
  sql <- SqlRender::loadRenderTranslateSql(
    "MainCostUtilization.sql", 
    "CostUtilization",
    warnOnMissingParameters = FALSE, 
    .params = sqlParams,
    targetDialect = DatabaseConnector::dbms(connection)
    
  )
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



