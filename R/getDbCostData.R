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
  ParallelLogger::logInfo("Extracting cost covariates...")
  
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
  ParallelLogger::logInfo("Getting reference tables...")
  
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
  
  ParallelLogger::logInfo(sprintf(
    "Cost covariate extraction completed in %.2f seconds",
    metaData$extractionDuration
  ))
  
  return(result)
}




#' Generate SQL for cost covariates extraction
#
#' @description
#' Internal function to generate SQL queries for cost covariate extraction
#'
#' @param connection Database connection
#' @param cdmDatabaseSchema CDM database schema
#' @param cohortTable Cohort table name
#' @param cohortDatabaseSchema Cohort database schema
#' @param cohortId Cohort ID
#' @param covariateSettings Cost covariate settings
#' @param aggregated Whether to generate aggregated SQL
#'
#' @return List of SQL queries
#' @keywords internal
generateCostCovariatesSql <- function(connection,
                                      cdmDatabaseSchema,
                                      cohortTable,
                                      cohortDatabaseSchema,
                                      cohortId,
                                      covariateSettings,
                                      aggregated) {
  
  # Base SQL parameters
  sqlParams <- list(
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_id = cohortId,
    aggregate_method = covariateSettings$aggregateMethod,
    use_costs = covariateSettings$useCosts
  )
  
  # Add temporal parameters
  if (!is.null(covariateSettings$temporalStartDays)) {
    sqlParams$temporal_start_days <- paste(covariateSettings$temporalStartDays, collapse = ",")
    sqlParams$temporal_end_days <- paste(covariateSettings$temporalEndDays, collapse = ",")
  }
  
  # Add cost type filters
  if (!is.null(covariateSettings$costTypeConceptIds)) {
    sqlParams$cost_type_concept_ids <- paste(covariateSettings$costTypeConceptIds, collapse = ",")
  }
  
  # Add domain filters
  if (!is.null(covariateSettings$costDomainIds)) {
    sqlParams$cost_domain_ids <- paste0("'", paste(covariateSettings$costDomainIds, collapse = "','"), "'")
  }
  
  # Add currency filters
  if (!is.null(covariateSettings$currencyConceptIds)) {
    sqlParams$currency_concept_ids <- paste(covariateSettings$currencyConceptIds, collapse = ",")
  }
  
  # Generate main covariate SQL
  if (aggregated) {
    covariatesSql <- SqlRender::loadRenderTranslateSql(
      "GetAggregatedCostCovariates.sql",
      packageName = "CostUtilization",
      dbms = connection@dbms,
      cdm_database_schema = sqlParams$cdm_database_schema,
      cohort_database_schema = sqlParams$cohort_database_schema,
      cohort_table = sqlParams$cohort_table,
      cohort_id = sqlParams$cohort_id,
      temporal_start_days = sqlParams$temporal_start_days,
      temporal_end_days = sqlParams$temporal_end_days,
      cost_type_concept_ids = sqlParams$cost_type_concept_ids,
      cost_domain_ids = sqlParams$cost_domain_ids,
      currency_concept_ids = sqlParams$currency_concept_ids,
      aggregate_method = sqlParams$aggregate_method
    )
  } else {
    covariatesSql <- SqlRender::loadRenderTranslateSql(
      "GetCostCovariates.sql",
      packageName = "CostUtilization",
      dbms = connection@dbms,
      cdm_database_schema = sqlParams$cdm_database_schema,
      cohort_database_schema = sqlParams$cohort_database_schema,
      cohort_table = sqlParams$cohort_table,
      cohort_id = sqlParams$cohort_id,
      temporal_start_days = sqlParams$temporal_start_days,
      temporal_end_days = sqlParams$temporal_end_days,
      cost_type_concept_ids = sqlParams$cost_type_concept_ids,
      cost_domain_ids = sqlParams$cost_domain_ids,
      currency_concept_ids = sqlParams$currency_concept_ids
    )
  }
  
  # Generate reference SQL
  covariateRefSql <- SqlRender::loadRenderTranslateSql(
    "GetCostCovariateRef.sql",
    packageName = "CostUtilization",
    dbms = connection@dbms,
    covariate_settings = as.character(jsonlite::toJSON(covariateSettings))
  )
  
  analysisRefSql <- SqlRender::loadRenderTranslateSql(
    "GetCostAnalysisRef.sql",
    packageName = "CostUtilization",
    dbms = connection@dbms,
    covariate_settings = as.character(jsonlite::toJSON(covariateSettings))
  )
  
  return(list(
    covariatesSql = covariatesSql,
    aggregatedCovariatesSql = covariatesSql,
    covariateRefSql = covariateRefSql,
    analysisRefSql = analysisRefSql
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