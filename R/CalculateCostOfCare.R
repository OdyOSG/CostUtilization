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
  sessionPrefix <- paste0("cu_", stringi::stri_rand_strings(1, 5, pattern = "[a-z]"))
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
    cpiData$adj_factor <- cpiData$cpi
    
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


formatAsFeatureExtraction <- function(results, diagnostics, costOfCareSettings, cohortId, aggregated) {
  
  # Generate base covariate IDs
  baseAnalysisId <- 5000  # Base ID for cost analyses
  
  # Create analysis reference
  analysisRef <- dplyr::tibble(
    analysisId = baseAnalysisId,
    analysisName = "Cost of Care Analysis",
    domainId = "Cost",
    startDay = costOfCareSettings$startOffsetDays,
    endDay = costOfCareSettings$endOffsetDays,
    isBinary = FALSE,
    missingMeansZero = TRUE
  )
  
  # Create covariate reference
  covariateRef <- dplyr::tibble(
    covariateId = c(
      baseAnalysisId * 1000 + 1,  # Total cost
      baseAnalysisId * 1000 + 2,  # Cost PPPM
      baseAnalysisId * 1000 + 3,  # Visits per 1000 PY
      baseAnalysisId * 1000 + 4,  # Visit dates per 1000 PY
      baseAnalysisId * 1000 + 5   # N persons with cost
    ),
    covariateName = c(
      glue::glue("Total cost [{costOfCareSettings$startOffsetDays}d to {costOfCareSettings$endOffsetDays}d]"),
      glue::glue("Cost per person per month [{costOfCareSettings$startOffsetDays}d to {costOfCareSettings$endOffsetDays}d]"),
      glue::glue("Visits per 1000 person-years [{costOfCareSettings$startOffsetDays}d to {costOfCareSettings$endOffsetDays}d]"),
      glue::glue("Visit dates per 1000 person-years [{costOfCareSettings$startOffsetDays}d to {costOfCareSettings$endOffsetDays}d]"),
      glue::glue("Number of persons with cost [{costOfCareSettings$startOffsetDays}d to {costOfCareSettings$endOffsetDays}d]")
    ),
    analysisId = baseAnalysisId,
    conceptId = 0
  )
  
  # Add event-specific covariates if applicable
  if (costOfCareSettings$hasEventFilters) {
    eventCovariates <- purrr::imap_dfr(costOfCareSettings$eventFilters, function(filter, idx) {
      dplyr::tibble(
        covariateId = baseAnalysisId * 1000 + 100 + idx,
        covariateName = glue::glue("{filter$name} cost [{costOfCareSettings$startOffsetDays}d to {costOfCareSettings$endOffsetDays}d]"),
        analysisId = baseAnalysisId,
        conceptId = 0
      )
    })
    covariateRef <- dplyr::bind_rows(covariateRef, eventCovariates)
  }
  
  # Format covariates
  if (aggregated) {
    # Aggregated format
    covariates <- dplyr::tibble(
      cohortDefinitionId = cohortId,
      covariateId = covariateRef$covariateId[1:5],
      sumValue = c(
        results$totalCost,
        results$totalCost,  # Will be averaged
        results$distinctVisits * 1000,  # Pre-scaled for per 1000 PY
        results$distinctVisitDates * 1000,
        results$nPersonsWithCost
      ),
      averageValue = c(
        results$totalCost / results$totalPersonDays * 365.25,  # Annual average
        results$costPppm,
        results$visitsPerThousandPy,
        results$visitDatesPerThousandPy,
        NA_real_
      )
    ) |>
      dplyr::filter(!is.na(.data$sumValue) | !is.na(.data$averageValue))
  } else {
    # Person-level format would require additional SQL queries
    # For now, return aggregated with a warning
    rlang::warn("Person-level FeatureExtraction format not yet implemented. Returning aggregated format.")
    covariates <- dplyr::tibble(
      cohortDefinitionId = cohortId,
      covariateId = covariateRef$covariateId[1:5],
      covariateValue = c(
        results$totalCost,
        results$costPppm,
        results$visitsPerThousandPy,
        results$visitDatesPerThousandPy,
        results$nPersonsWithCost
      )
    )
  }
  
  # Create metadata
  metaData <- list(
    databaseId = NA_character_,
    populationSize = diagnostics |> 
      dplyr::filter(.data$stepName == "00_initial_cohort") |> 
      dplyr::pull(.data$nPersons),
    minCovariateValue = min(covariates$sumValue, covariates$averageValue, na.rm = TRUE),
    maxCovariateValue = max(covariates$sumValue, covariates$averageValue, na.rm = TRUE)
  )
  
  # Return FeatureExtraction-compatible structure
  result <- list(
    covariates = covariates,
    covariateRef = covariateRef,
    analysisRef = analysisRef,
    metaData = metaData,
    diagnostics = diagnostics  # Include original diagnostics
  )
  
  class(result) <- c("CovariateData", "FeatureExtraction", class(result))
  
  return(result)
}
