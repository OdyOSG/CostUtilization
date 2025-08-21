#' Calculate Cost of Care Analysis
#'
#' @description
#' Performs cost-of-care analysis for a specified cohort using the OMOP CDM.
#' This function calculates aggregated cost and utilization metrics within
#' specified time windows relative to cohort entry or exit dates.
#'
#' @param connection A `DatabaseConnector` connection object. Either this or
#'   `connectionDetails` must be provided.
#' @param connectionDetails A `ConnectionDetails` object as created by
#'   `DatabaseConnector::createConnectionDetails()`. Either this or `connection`
#'   must be provided.
#' @param cdmDatabaseSchema Schema name for CDM tables.
#' @param cohortDatabaseSchema Schema name for the cohort table.
#' @param cohortTable Name of the cohort table.
#' @param cohortId The cohort definition ID to analyze.
#' @param costOfCareSettings A settings object created by `createCostOfCareSettings()`.
#' @param tempEmulationSchema Schema for temporary tables (if needed).
#' @param verbose Print progress messages.
#'
#' @return A list containing two tibbles:
#'   \item{results}{Main analysis results with cost and utilization metrics}
#'   \item{diagnostics}{Step-by-step diagnostic information about the analysis}
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Create connection
#' connectionDetails <- DatabaseConnector::createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "localhost/ohdsi",
#'   user = "user",
#'   password = "password"
#' )
#'
#' # Create settings
#' settings <- createCostOfCareSettings(
#'   anchorCol = "cohort_start_date",
#'   startOffsetDays = 0,
#'   endOffsetDays = 365,
#'   restrictVisitConceptIds = c(9201, 9203)
#' )
#'
#' # Run analysis
#' results <- calculateCostOfCare(
#'   connectionDetails = connectionDetails,
#'   cdmDatabaseSchema = "cdm_schema",
#'   cohortDatabaseSchema = "results_schema",
#'   cohortTable = "cohort",
#'   cohortId = 1,
#'   costOfCareSettings = settings
#' )
#' }
calculateCostOfCare <- function(connection = NULL,
                               connectionDetails = NULL,
                               cdmDatabaseSchema,
                               cohortDatabaseSchema,
                               cohortTable,
                               cohortId,
                               costOfCareSettings,
                               tempEmulationSchema = NULL,
                               verbose = TRUE) {
  
  # Input validation
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(costOfCareSettings, "CostOfCareSettings", add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(cohortDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(cohortTable, len = 1, add = errorMessages)
  checkmate::assertNumeric(cohortId, len = 1, add = errorMessages)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertLogical(verbose, len = 1, add = errorMessages)
  checkmate::reportAssertions(errorMessages)
  
  # Handle connection/connectionDetails
  if (is.null(connectionDetails) && is.null(connection)) {
    stop("Need to provide either connectionDetails or connection")
  }
  if (!is.null(connectionDetails) && !is.null(connection)) {
    stop("Need to provide either connectionDetails or connection, not both")
  }
  
  if (!is.null(connectionDetails)) {
    checkmate::assertClass(connectionDetails, "ConnectionDetails")
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  } else {
    checkmate::assertClass(connection, "DatabaseConnectorConnection")
  }
  
  # Setup
  startTime <- Sys.time()
  targetDialect <- DatabaseConnector::dbms(connection)
  
  # Generate unique names for temp tables to avoid session conflicts
  sessionPrefix <- paste0("cu_", stringi::stri_rand_strings(1, 10, pattern = "[a-z]"))
  resultsTableName <- paste0(sessionPrefix, "_results")
  diagTableName <- paste0(sessionPrefix, "_diag")
  restrictVisitTableName <- NULL
  eventConceptsTableName <- NULL
  
  # Ensure all created tables are dropped on exit
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
  
  # Upload helper tables if needed
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
    logMessage(
      glue::glue("Uploaded {nrow(visitConcepts)} visit concepts to #{restrictVisitTableName}"),
      verbose,
      "DEBUG"
    )
  }
  
  if (isTRUE(costOfCareSettings$hasEventFilters)) {
    eventConceptsTableName <- paste0(sessionPrefix, "_evt_concepts")
    eventConcepts <- purrr::imap_dfr(
      costOfCareSettings$eventFilters,
      ~ dplyr::tibble(
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
    logMessage(
      glue::glue("Uploaded {nrow(eventConcepts)} event concepts to #{eventConceptsTableName}"),
      verbose,
      "DEBUG"
    )
  }
  
  # Assemble SQL parameters
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
  
  # Execute analysis
  executeSqlPlan(
    connection = connection,
    params = params,
    targetDialect = targetDialect,
    tempEmulationSchema = tempEmulationSchema,
    verbose = verbose
  )
  
  # Fetch and return results
  logMessage("Fetching results from database", verbose, "INFO")
  
  resultsTableFqn <- if (!is.null(tempEmulationSchema)) {
    paste(tempEmulationSchema, resultsTableName, sep = ".")
  } else {
    resultsTableName
  }
  
  diagTableFqn <- if (!is.null(tempEmulationSchema)) {
    paste(tempEmulationSchema, diagTableName, sep = ".")
  } else {
    diagTableName
  }
  
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
  
  return(list(
    results = results,
    diagnostics = diagnostics
  ))
}