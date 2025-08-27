#' Calculate Cost of Care Analysis
#'
#' @description
#' Runs the cost-of-care analysis for a specified cohort using a validated
#' `CostOfCareSettings` object. Creates temp helper tables as needed
#' (visit restrictions, event filters, CPI) and executes the SQL plan.
#'
#' @param connection A live `DatabaseConnector` connection object.
#' @param connectionDetails Optional `ConnectionDetails` to create a connection
#'   if `connection` is not supplied (provide exactly one of the two).
#' @param cdmDatabaseSchema Schema (or database) that contains CDM tables.
#' @param cohortDatabaseSchema Schema (or database) that contains the cohort table.
#' @param cohortTable Name of the cohort table.
#' @param cohortId Numeric cohort definition id to analyze.
#' @param costOfCareSettings A settings object from `createCostOfCareSettings()`.
#' @param tempEmulationSchema Optional schema for temp table emulation (Oracle/Redshift/…).
#' @param verbose Logical; print progress messages.
#'
#' @return A list with two tibbles: `results` and `diagnostics`.
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
    verbose = TRUE) {
  # --- Validation / connection management ---
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(costOfCareSettings, "CostOfCareSettings", add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(cohortDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(cohortTable, len = 1, add = errorMessages)
  checkmate::assertIntegerish(cohortId, len = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertFlag(verbose, add = errorMessages)
  checkmate::reportAssertions(errorMessages)

  if (is.null(connectionDetails) && is.null(connection)) {
    stop("Provide either `connectionDetails` or an open `connection`.")
  }
  if (!is.null(connectionDetails) && !is.null(connection)) {
    stop("Provide exactly one of `connectionDetails` or `connection`, not both.")
  }

  if (!is.null(connectionDetails)) {
    checkmate::assertClass(connectionDetails, "ConnectionDetails")
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  } else {
    checkmate::assertClass(connection, "DatabaseConnectorJdbcConnection")
  }

  # --- Setup ---
  startTime <- Sys.time()
  targetDialect <- DatabaseConnector::dbms(connection)

  # Unique prefix for this run (temp/staging tables)
  sessionPrefix <- purrr::chuck(SqlRender::translate('#', 'oracle'), 1)
  resultsTableName <- paste0(sessionPrefix, "_results")
  diagTableName <- paste0(sessionPrefix, "_diag")
  restrictVisitTableName <- NULL
  eventConceptsTableName <- NULL
  cpiAdjTableName <- NULL

  # Always attempt cleanup on exit
  on.exit(
    {
      logMessage("Cleaning up temporary tables…", verbose, "INFO")
      cleanupTempTables(
        connection          = connection,
        schema              = tempEmulationSchema,
        resultsTableName,
        diagTableName,
        restrictVisitTableName,
        eventConceptsTableName,
        cpiAdjTableName
      )
    },
    add = TRUE
  )

  # --- CPI Adjustment: prepare adjustment factors table (if enabled) ---
  if (costOfCareSettings$cpiAdjustment) {
    logMessage("Setting up CPI adjustment…", verbose, "INFO")
    cpiAdjTableName <- paste0(sessionPrefix, "_cpi_adj")

    # Load CPI data from explicit path; caller validated existence already
    cpiPath <- costOfCareSettings$cpiFilePath
    cpiData <- utils::read.csv(cpiPath, stringsAsFactors = FALSE)

    if (!all(c("year", "adj_factor") %in% names(cpiData))) {
      # Backward compatibility: allow a file with `year` and `cpi` by renaming
      if (all(c("year", "cpi") %in% names(cpiData))) {
        cpiData$adj_factor <- cpiData$cpi
      } else {
        cli::cli_abort("CPI data must contain columns 'year' and 'adj_factor' (or 'year' and 'cpi').")
      }
    }

    cpiData <- cpiData[, c("year", "adj_factor")]
    checkmate::assertIntegerish(cpiData$year, lower = 1900, any.missing = FALSE)
    checkmate::assertNumeric(cpiData$adj_factor, any.missing = FALSE)

    DatabaseConnector::insertTable(
      connection = connection,
      tableName = cpiAdjTableName,
      data = cpiData,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = FALSE
    )
    logMessage(sprintf("Uploaded %d CPI rows to #%s", nrow(cpiData), cpiAdjTableName), verbose, "DEBUG")
  }

  # --- Upload helper tables ---
  if (costOfCareSettings$hasVisitRestriction) {
    restrictVisitTableName <- paste0(sessionPrefix, "_visit_restr")
    visitConcepts <- tibble::tibble(visit_concept_id = costOfCareSettings$restrictVisitConceptIds)
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = restrictVisitTableName,
      data = visitConcepts,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE
    )
    logMessage(sprintf("Uploaded %d visit concepts to #%s", nrow(visitConcepts), restrictVisitTableName), verbose, "DEBUG")
  }

  if (costOfCareSettings$hasEventFilters) {
    eventConceptsTableName <- paste0(sessionPrefix, "_evt_concepts")
    # Build a long table: one row per concept id per filter
    eventConcepts <- 
      purrr::map_dfr(
        seq_along(costOfCareSettings$eventFilters), ~ dplyr::tibble(
          filter_id    = .x,
          filter_name  = costOfCareSettings$eventFilters[[.x]]$name,
          domain_scope = costOfCareSettings$eventFilters[[.x]]$domain,
          concept_id   = as.integer(costOfCareSettings$eventFilters[[.x]]$conceptIds)
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
    logMessage(sprintf("Uploaded %d event concept rows to #%s", nrow(eventConcepts), eventConceptsTableName), verbose, "DEBUG")
  }

  # --- Assemble SQL parameters from settings (refactored; camelCase only) ---
  params <- list(
    # core
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = as.integer(cohortId),

    # output / helper tables (unqualified here)
    resultsTable = resultsTableName,
    diagTable = diagTableName,
    restrictVisitTable = restrictVisitTableName,
    eventConceptsTable = eventConceptsTableName,
    cpiAdjTable = cpiAdjTableName,

    # window & anchor
    anchorOnEnd = isTRUE(identical(costOfCareSettings$anchorCol, "cohort_end_date")),
    timeA = as.integer(costOfCareSettings$startOffsetDays %||% 0L),
    timeB = as.integer(costOfCareSettings$endOffsetDays %||% 365L),

    # flags & knobs (logical where appropriate; numbers as integers)
    hasVisitRestriction = isTRUE(costOfCareSettings$hasVisitRestriction),
    hasEventFilters = isTRUE(costOfCareSettings$hasEventFilters),
    nFilters = as.integer(costOfCareSettings$nFilters %||% 0L),
    microCosting = costOfCareSettings$microCosting, # pass-through (string/int as your SQL expects)
    cpiAdjustment = isTRUE(costOfCareSettings$cpiAdjustment),

    # costing
    costConceptId = as.integer(costOfCareSettings$costConceptId),
    currencyConceptId = as.integer(costOfCareSettings$currencyConceptId),

    # primary filter id (index in eventFilters) if named
    primaryFilterId = {
      pefn <- costOfCareSettings$primaryEventFilterName %||% NULL
      efs <- costOfCareSettings$eventFilters %||% NULL
      if (!is.null(pefn) && !is.null(efs) && length(efs) > 0) {
        names <- vapply(efs, function(f) f$name, character(1))
        idx <- which(names == pefn)
        if (length(idx) == 1L) as.integer(idx) else 0L
      } else {
        0L
      }
    }
  )


  # Let the execution helper render/translate/execute with these params
  executeSqlPlan(
    connection          = connection,
    params              = params,
    targetDialect       = targetDialect,
    tempEmulationSchema = tempEmulationSchema,
    verbose             = verbose
  )

  # --- Fetch & return results ---
  logMessage("Fetching results from database…", verbose, "INFO")
  resultsFqn <- if (!is.null(tempEmulationSchema)) paste(tempEmulationSchema, resultsTableName, sep = ".") else resultsTableName
  diagFqn <- if (!is.null(tempEmulationSchema)) paste(tempEmulationSchema, diagTableName, sep = ".") else diagTableName

  resultsSql <- sprintf("SELECT * FROM %s;", resultsFqn)
  diagnosticsSql <- sprintf("SELECT * FROM %s ORDER BY step_name;", diagFqn)

  results <- DatabaseConnector::querySql(connection, resultsSql, snakeCaseToCamelCase = TRUE) |>
    dplyr::as_tibble()
  diagnostics <- DatabaseConnector::querySql(connection, diagnosticsSql, snakeCaseToCamelCase = TRUE) |>
    dplyr::as_tibble()

  logMessage(
    sprintf("Analysis complete in %0.1fs.", as.numeric(difftime(Sys.time(), startTime, units = "secs"))),
    verbose = verbose,
    level = "SUCCESS"
  )

  list(results = results, diagnostics = diagnostics)
}
