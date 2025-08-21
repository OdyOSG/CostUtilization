#' Calculate Cost of Care Analysis (CDM 5.5 Compatible)
#'
#' @description
#' Performs cost-of-care analysis for a specified cohort using OMOP CDM 5.5 cost table structure.
#'
#' @param connection A `DatabaseConnector` connection object.
#' @param cdmDatabaseSchema Schema name for CDM tables.
#' @param cohortDatabaseSchema Schema name for the cohort table.
#' @param cohortTable Name of the cohort table.
#' @param cohortId The cohort definition ID to analyze.
#' @param costOfCareSettings A settings object created by `createCostOfCareSettings()`.
#' @param tempEmulationSchema Schema for temporary tables (if needed).
#' @param verbose Print progress messages.
#'
#' @return A list containing two tibbles: `results` and `diagnostics`.
#' @export
calculateCostOfCareCDM55 <- function(
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
  
  # Ensure all created tables are dropped on exit, even if an error occurs
  on.exit({
    logMessage("Cleaning up temporary tables...", verbose, "INFO")
    cleanupTempTables(
      connection = connection,
      schema = tempEmulationSchema,
      resultsTableName,
      diagTableName,
      restrictVisitTableName,
      eventConceptsTableName
    )
  }, add = TRUE)
  
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
      eventConceptsTable = eventConceptsTableName
    ),
    costOfCareSettings
  )
  
  # --- Execute Analysis ---
  executeSqlPlanCDM55(
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

#' Execute SQL Plan for Cost Analysis (CDM 5.5)
#' @noRd
executeSqlPlanCDM55 <- function(
    connection,
    params,
    targetDialect,
    tempEmulationSchema,
    verbose = TRUE
) {
  logMessage("Starting SQL plan execution for CDM 5.5", verbose,  "INFO")
  sql <- system.file(
    "sql", 
    "MainCostUtilization_CDM55.sql", 
    package = "CostUtilization", 
    mustWork = TRUE) |> SqlRender::readSql()
  
  renderParams <- prepareSqlRenderParams(params, tempEmulationSchema)
  
  logMessage(sprintf("Translating SQL to %s dialect", targetDialect), verbose,  "DEBUG")
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
    diag_table = renderParams$diag_table
  )
  
  sqlStatements <- SqlRender::splitSql(sql)
  logMessage(sprintf("Executing %d SQL statements", length(sqlStatements)), verbose,  "INFO")
  
  executeSqlStatements(
    connection = connection,
    sqlStatements = sqlStatements,
    verbose = verbose
  )
  
  logMessage("SQL plan execution completed", verbose,  "INFO")
  invisible(NULL)
}

#' Helper functions (reuse from original)
#' @noRd
logMessage <- function(message, verbose = TRUE, level = "INFO") {
  if (!verbose) return(invisible(NULL))
  
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  
  if (level == "SUCCESS") {
    cli::cli_alert_success("{timestamp} - {message}")
  } else if (level == "WARNING") {
    cli::cli_alert_warning("{timestamp} - {message}")
  } else if (level == "ERROR") {
    cli::cli_alert_danger("{timestamp} - {message}")
  } else if (level == "DEBUG") {
    cli::cli_alert_info("{timestamp} - [DEBUG] {message}")
  } else {
    cli::cli_alert_info("{timestamp} - {message}")
  }
}

#' @noRd
cleanupTempTables <- function(connection, schema, ...) {
  tables <- list(...)
  for (table in tables) {
    if (!is.null(table)) {
      tryCatch({
        if (!is.null(schema)) {
          DatabaseConnector::executeSql(connection, glue::glue("DROP TABLE IF EXISTS {schema}.{table};"))
        } else {
          DatabaseConnector::executeSql(connection, glue::glue("DROP TABLE IF EXISTS {table};"))
        }
      }, error = function(e) {
        # Silently ignore errors during cleanup
      })
    }
  }
}

#' @noRd
executeSqlStatements <- function(connection, sqlStatements, verbose) {
  for (i in seq_along(sqlStatements)) {
    statement <- sqlStatements[i]
    
    # Skip empty statements
    if (trimws(statement) == "") next
    
    tryCatch({
      DatabaseConnector::executeSql(connection, statement)
    }, error = function(e) {
      logMessage(
        sprintf("Error executing SQL statement %d: %s", i, e$message),
        verbose,
        "ERROR"
      )
      stop(e)
    })
  }
}

#' Prepare SQL Render Parameters (reuse from original)
#' @noRd
prepareSqlRenderParams <- function(params, tempEmulationSchema) {
  sqlParams <- list()
  
  # --- Direct mapping from R camelCase to SQL snake_case ---
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
  
  # --- Boolean flags for conditional SQL ---
  sqlParams$has_visit_restriction <- params$hasVisitRestriction
  sqlParams$has_event_filters <- params$hasEventFilters
  sqlParams$micro_costing <- params$microCosting
  
  # --- Derived parameters ---
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
  
  # --- Schema-qualified table names ---
  sqlParams$results_table <- params$resultsTable
  sqlParams$diag_table <- params$diagTable
  sqlParams$restrict_visit_table <- params$restrictVisitTable
  sqlParams$event_concepts_table <- params$eventConceptsTable
  
  qualifyTableName <- function(tableName, schema) {
    if (!is.null(schema) && !is.null(tableName)) {
      return(paste(schema, tableName, sep = "."))
    }
    return(tableName)
  }
  
  sqlParams$results_table <- qualifyTableName(sqlParams$results_table, tempEmulationSchema)
  sqlParams$diag_table <- qualifyTableName(sqlParams$diag_table, tempEmulationSchema)
  sqlParams$restrict_visit_table <- qualifyTableName(sqlParams$restrict_visit_table, tempEmulationSchema)
  sqlParams$event_concepts_table <- qualifyTableName(sqlParams$event_concepts_table, tempEmulationSchema)
  
  return(sqlParams)
}