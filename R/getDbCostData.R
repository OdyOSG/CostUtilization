#' Get cost data from database
#'
#' @description
#' Extracts cost covariate data from a CDM database based on the provided settings.
#' This function handles the connection to the database, SQL generation, and data retrieval.
#'
#' @param connectionDetails An object of type \code{connectionDetails} as created using the
#'   \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'   DatabaseConnector package.
#' @param cdmDatabaseSchema Schema name where your patient-level data in OMOP CDM format resides.
#' @param cohortTable Name of the table with the cohort data.
#' @param cohortDatabaseSchema Schema where the cohort table resides. Default is cdmDatabaseSchema.
#' @param cohortId The cohort definition ID of the cohort for which to extract covariates.
#' @param covariateSettings An object of type \code{costCovariateSettings} as created using
#'   the \code{costCovariateSettings} function.
#' @param aggregated Should aggregate statistics be computed? If TRUE, will return
#'   population-level statistics. If FALSE, will return person-level data.
#' @param minCharacterizationMean Minimum mean value for characterization output. 
#'   Values below this will be censored.
#' @param tempEmulationSchema Some database platforms like Oracle and Impala do not truly support
#'   temp tables. To emulate temp tables, provide a schema where temp tables can be created.
#'
#' @return
#' An object of type \code{CostCovariateData} containing:
#' \itemize{
#'   \item{covariates}{Cost covariates data}
#'   \item{covariateRef}{Reference table with covariate definitions}
#'   \item{analysisRef}{Reference table with analysis definitions}
#'   \item{metaData}{Metadata about the extraction}
#' }
#'
#' @export
getDbCostData <- function(connectionDetails,
                          cdmDatabaseSchema,
                          cohortTable = "cohort",
                          cohortDatabaseSchema = cdmDatabaseSchema,
                          cohortId,
                          covariateSettings,
                          aggregated = TRUE,
                          minCharacterizationMean = 0.001,
                          tempEmulationSchema = NULL) {
  
  # Input validation
  if (!isCostCovariateSettings(covariateSettings)) {
    stop("covariateSettings must be created using costCovariateSettings()")
  }
  
  if (missing(cohortId) || is.null(cohortId)) {
    stop("cohortId is required")
  }
  
  # Create connection
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Start timing
  startTime <- Sys.time()
  
  # Create temp tables if needed
  if (!is.null(tempEmulationSchema)) {
    options(sqlRenderTempEmulationSchema = tempEmulationSchema)
  }
  
  # Initialize results
  covariates <- NULL
  covariateRef <- NULL
  analysisRef <- NULL
  
  # Generate SQL based on settings
  sql <- generateCostCovariatesSql(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortId = cohortId,
    covariateSettings = covariateSettings,
    aggregated = aggregated
  )
  
  # Execute SQL and retrieve results
  writeLines("Extracting cost covariates...")
  
  if (aggregated) {
    # Get aggregated covariates
    covariates <- DatabaseConnector::querySql(
      connection = connection,
      sql = sql$aggregatedCovariatesSql,
      snakeCaseToCamelCase = TRUE
    )
    
    # Apply minimum mean threshold
    if (!is.null(minCharacterizationMean) && minCharacterizationMean > 0) {
      covariates <- covariates[covariates$meanValue >= minCharacterizationMean, ]
    }
    
  } else {
    # Get person-level covariates
    covariates <- DatabaseConnector::querySql(
      connection = connection,
      sql = sql$covariatesSql,
      snakeCaseToCamelCase = TRUE
    )
  }
  
  # Get reference tables
  writeLines("Getting reference tables...")
  
  covariateRef <- DatabaseConnector::querySql(
    connection = connection,
    sql = sql$covariateRefSql,
    snakeCaseToCamelCase = TRUE
  )
  
  analysisRef <- DatabaseConnector::querySql(
    connection = connection,
    sql = sql$analysisRefSql,
    snakeCaseToCamelCase = TRUE
  )
  
  # Create metadata
  metaData <- list(
    databaseId = connectionDetails$dbms,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortId = cohortId,
    covariateSettings = covariateSettings,
    aggregated = aggregated,
    extractionDateTime = Sys.time(),
    extractionDuration = difftime(Sys.time(), startTime, units = "secs"),
    cdmVersion = getCdmVersion(connection, cdmDatabaseSchema)
  )
  
  # Create result object
  result <- list(
    covariates = covariates,
    covariateRef = covariateRef,
    analysisRef = analysisRef,
    metaData = metaData
  )
  
  class(result) <- "CostCovariateData"
  
  writeLines(sprintf(
    "Cost covariate extraction completed in %.2f seconds",
    metaData$extractionDuration
  ))
  
  return(result)
}




#' Generate SQL for cost covariates extraction
#'
#' @description
#' This internal function constructs the necessary SQL queries to extract cost and
#' utilization covariates. It creates and uploads a temporary table for temporal
#' windows to avoid complex string parsing in SQL, making the queries more robust
#' and efficient.
#'
#' @param connection A `DatabaseConnector` connection object.
#' @param cdmDatabaseSchema The schema holding the OMOP CDM data.
#' @param cohortTable The table containing the cohorts.
#' @param cohortDatabaseSchema The schema where the cohort table resides.
#' @param cohortId The ID of the cohort for which to extract data.
#' @param covariateSettings An object of type `costCovariateSettings`.
#' @param aggregated A logical value indicating whether to compute aggregated statistics
#'   or extract person-level data.
#'
#' @return
#' A list containing the rendered and translated SQL queries for covariates,
#' covariate reference, and analysis reference.
#'
#' @keywords internal
generateCostCovariatesSql <- function(connection,
                                      cdmDatabaseSchema,
                                      cohortTable,
                                      cohortDatabaseSchema,
                                      cohortId,
                                      covariateSettings,
                                      aggregated) {
  
  # --- Step 1: Create and upload a structured temporary table for temporal windows ---
  # This is more robust than parsing comma-separated strings in SQL.
  temporalWindows <- data.frame(
    window_id = seq_along(covariateSettings$temporalStartDays),
    start_day = as.integer(covariateSettings$temporalStartDays),
    end_day = as.integer(covariateSettings$temporalEndDays)
  )
  
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "#temporal_windows",
    data = temporalWindows,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE,
    camelCaseToSnakeCase = TRUE # Ensure column names are snake_case for SQL
  )
  
  # --- Step 2: Prepare parameters for SqlRender ---
  # Handle NULL settings by converting them to empty strings, which the SQL templates expect.
  sqlParams <- list(
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_id = cohortId,
    aggregate_method = covariateSettings$aggregateMethod,
    cost_type_concept_ids = if (is.null(covariateSettings$costTypeConceptIds)) {
      ""
    } else {
      paste(covariateSettings$costTypeConceptIds, collapse = ",")
    },
    cost_domain_ids = if (is.null(covariateSettings$costDomainIds)) {
      ""
    } else {
      # Format as a quoted, comma-separated string for SQL 'IN' clause
      paste0("'", paste(covariateSettings$costDomainIds, collapse = "','"), "'")
    },
    currency_concept_ids = if (is.null(covariateSettings$currencyConceptIds)) {
      ""
    } else {
      paste(covariateSettings$currencyConceptIds, collapse = ",")
    }
    # Note: temporal_start_days and temporal_end_days are no longer passed as parameters
  )
  
  # --- Step 3: Select and render the appropriate main SQL script ---
  sqlFileName <- if (aggregated) {
    "GetAggregatedCostCovariates.sql"
  } else {
    "GetCostCovariates.sql"
  }
  
  writeLines(paste("Using SQL template:", sqlFileName))
  
  # Render the main covariate extraction query
  # The same rendered SQL is returned for both list items to maintain
  # compatibility with the calling getDbCostData function's structure.
  renderedCovariatesSql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = sqlFileName,
    packageName = "CostUtilization",
    dbms = connection@dbms,
    ... = sqlParams
  )
  
  # --- Step 4: Render the reference SQL scripts ---
  # The settings are passed as a JSON string for potential future use in more
  # dynamic reference table generation.
  settingsJson <- as.character(jsonlite::toJSON(covariateSettings, auto_unbox = TRUE))
  
  renderedCovariateRefSql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "GetCostCovariateRef.sql",
    packageName = "CostUtilization",
    dbms = connection@dbms,
    covariate_settings = settingsJson
  )
  
  renderedAnalysisRefSql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "GetCostAnalysisRef.sql",
    packageName = "CostUtilization",
    dbms = connection@dbms,
    covariate_settings = settingsJson
  )
  
  # --- Step 5: Return the list of rendered SQL queries ---
  return(list(
    covariatesSql = renderedCovariatesSql,
    aggregatedCovariatesSql = renderedCovariatesSql,
    covariateRefSql = renderedCovariateRefSql,
    analysisRefSql = renderedAnalysisRefSql
  ))
}


#' Get CDM version from database
#'
#' @param connection Database connection
#' @param cdmDatabaseSchema CDM database schema
#' @return CDM version string
#' @keywords internal
getCdmVersion <- function(connection, cdmDatabaseSchema) {
  sql <- "SELECT cdm_version FROM @cdm_database_schema.cdm_source LIMIT 1;"
  sql <- SqlRender::render(sql, cdm_database_schema = cdmDatabaseSchema)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
  
  result <- tryCatch({
    DatabaseConnector::querySql(connection, sql)
  }, error = function(e) {
    return(data.frame(CDM_VERSION = "Unknown"))
  })
  
  if (nrow(result) > 0) {
    return(as.character(result[1, 1]))
  } else {
    return("Unknown")
  }
}