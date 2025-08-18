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
    connection = NULL,
    connectionDetails = NULL,
    cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable,
    cohortId,
    costOfCareSettings,
    tempEmulationSchema = NULL,
    verbose = TRUE
) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(costOfCareSettings, "CostOfCareSettings")
  if (is.null(connectionDetails) && is.null(connection)) {
    stop("Need to provide either connectionDetails or connection")
  }
  if (!is.null(connectionDetails) && !is.null(connection)) {
    stop("Need to provide either connectionDetails or connection, not both")
  }
  
  if (!is.null(connectionDetails)) {
    checkmate::assertClass(connectionDetails, "ConnectionDetails")
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  } else {
    checkmate::assertClass(connection, "DatabaseConnectorJdbcConnection")
  }
  checkmate::assertCharacter(cohortTable, max.len = 1)
  checkmate::assertNumeric(cohortId, any.missing = FALSE, min.len = 1, max.len = 1)
  # --- Setup ---
  startTime <- Sys.time()
  targetDialect <- DatabaseConnector::dbms(connection)
  
  # Generate unique names for temp tables to avoid session conflicts
  sessionPrefix <- paste0("cu_", stringi::stri_rand_strings(1, 10, pattern = "[a-z]"))
  resultsTableName <- paste0(sessionPrefix, "_results")
  diagTableName <- paste0(sessionPrefix, "_diag")
  restrictVisitTableName <- NULL
  eventConceptsTableName <- NULL
  cpiAdjTableName <- NULL
  
  # Ensure all created tables are dropped on exit, even if an error occurs
  on.exit({
    logMessage("Cleaning up temporary tables...", verbose, "INFO")
    cleanupTempTables(
      connection = connection,
      schema = tempEmulationSchema,
      resultsTableName,
      diagTableName,
      restrictVisitTableName,
      eventConceptsTableName,
      cpiAdjTableName
    )
  }, add = TRUE)
  
  # --- CPI Adjustment Setup ---
  if (isTRUE(costOfCareSettings$cpiAdjustment)) {
    logMessage("Setting up CPI adjustment...", verbose, "INFO")
    cpiAdjTableName <- paste0(sessionPrefix, "_cpi_adj")
    
    # Load CPI data
    if (is.null(costOfCareSettings$cpiDataPath)) {
      defaultCpiPath <- system.file("csv", "cpi_data.csv", package = "CostUtilization", mustWork = TRUE)
      cpiData <- utils::read.csv(defaultCpiPath)
    } else {
      cpiData <- utils::read.csv(costOfCareSettings$cpiDataPath)
    }
    
    # Validate and calculate adjustment factors
    if (!all(c("year", "cpi") %in% names(cpiData))) {
      cli::cli_abort("Custom CPI data must contain 'year' and 'cpi' columns.")
    }
    cpiData$adj_factor <- targetCpi / cpiData$cpi
    
    # Upload to a temp table
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = cpiAdjTableName,
      data = cpiData[, c("year", "adj_factor")],
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema
    )
    logMessage(glue::glue("Uploaded CPI adjustment factors to #{cpiAdjTableName}"), verbose, "DEBUG")
  }
  
  
  # --- Upload Helper Tables ---
  if (isTRUE(costOfCareSettings$hasVisitRestriction)) {
    restrictVisitTableName <- paste0(sessionPrefix, "_visit_restr")
    visitConcepts <- dplyr::tibble(
      visit_concept_id = costOfCareSettings$restrictVisitConceptIds
    )
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = restrictVisitTableName,
      data = visitConcepts,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE
    )
    logMessage(glue::glue("Uploaded {nrow(visitConcepts)} visit concepts to #{restrictVisitTableName}"), verbose, "DEBUG")
  }
  
  if (isTRUE(costOfCareSettings$hasEventFilters)) {
    eventConceptsTableName <- paste0(sessionPrefix, "_evt_concepts")
    eventConcepts <- purrr::imap_dfr(costOfCareSettings$eventFilters, ~ dplyr::tibble(
      filter_id = .y,
      filter_name = .x$name,
      domain_scope = .x$domain,
      concept_id = .x$conceptIds
    )
    )
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = eventConceptsTableName,
      data = eventConcepts,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE
    )
    logMessage(glue::glue("Uploaded {nrow(eventConcepts)} event concepts to #{eventConceptsTableName}"), verbose, "DEBUG")
  }
  
  # --- Assemble SQL Parameters ---
  params <- c(
    list(
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortId = cohortId,
      resultsTable = resultsTableName,
      diagTable = diagTableName,
      restrictVisitTable = restrictVisitTableName,
      eventConceptsTable = eventConceptsTableName,
      cpiAdjTable = cpiAdjTableName
    ),
    costOfCareSettings
  )
  
  # --- Execute Analysis ---
  executeSqlPlan(
    connection = connection,
    params = params,
    targetDialect = targetDialect,
    tempEmulationSchema = tempEmulationSchema,
    verbose = verbose
  )
  
  # --- Fetch and Return Results ---
  logMessage("Fetching results from database", verbose, "INFO")
  resultsTableFqn <- if (!is.null(tempEmulationSchema)) paste(tempEmulationSchema, resultsTableName, sep = ".") else resultsTableName
  diagTableFqn <- if (!is.null(tempEmulationSchema)) paste(tempEmulationSchema, diagTableName, sep = ".") else diagTableName
  
  resultsSql <- glue::glue("SELECT * FROM {resultsTableFqn};")
  results <- DatabaseConnector::querySql(connection, resultsSql, snakeCaseToCamelCase = TRUE) |>
    dplyr::as_tibble()
  
  diagSql <- glue::glue("SELECT * FROM {diagTableFqn} ORDER BY step_name;")
  diagnostics <- DatabaseConnector::querySql(connection, diagSql, snakeCaseToCamelCase = TRUE) |>
    dplyr::as_tibble()
  
  logMessage(
    glue::glue("Analysis complete in {round(difftime(Sys.time(), startTime, units = 'secs'), 1)}s."),
    verbose = verbose,
    level = "SUCCESS"
  )
  
  return(list(results = results, diagnostics = diagnostics))
}



