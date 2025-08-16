# CalculateCostOfCareWithCpi.R
# Extended version of calculateCostOfCare with CPI adjustment support

#' Calculate cost of care with CPI adjustment
#'
#' @description
#' Extended version of calculateCostOfCare that supports Consumer Price Index (CPI)
#' adjustment. All CPI calculations are performed in SQL for efficiency.
#'
#' @param connection DatabaseConnector connection object
#' @param cdmDatabaseSchema Schema containing the CDM tables
#' @param cohortDatabaseSchema Schema containing the cohort table
#' @param cohortTable Name of the cohort table
#' @param cohortId Cohort definition ID
#' @param startOffsetDays Days from cohort start to begin cost calculation
#' @param endOffsetDays Days from cohort start to end cost calculation
#' @param visitConceptIds Vector of visit concept IDs to restrict to
#' @param eventFilterTable Name of table containing event filters
#' @param microCosting If TRUE, aggregate costs at visit detail level
#' @param cpiAdjustment If TRUE, adjust costs using CPI
#' @param cpiDatabaseSchema Schema containing CPI table (defaults to tempEmulationSchema)
#' @param cpiTable Name of CPI table (default: "cpi_factors")
#' @param cpiTargetYear Year to adjust all costs to
#' @param createCpiTable If TRUE and CPI table doesn't exist, create it
#' @param tempEmulationSchema Schema for temporary tables
#' @param resultsSchema Schema for results tables
#'
#' @return Data frame with cost analysis results
#' @export
calculateCostOfCareWithCpi <- function(connection,
                                      cdmDatabaseSchema,
                                      cohortDatabaseSchema,
                                      cohortTable,
                                      cohortId,
                                      startOffsetDays = 0,
                                      endOffsetDays = 365,
                                      visitConceptIds = NULL,
                                      eventFilterTable = NULL,
                                      microCosting = FALSE,
                                      cpiAdjustment = FALSE,
                                      cpiDatabaseSchema = NULL,
                                      cpiTable = "cpi_factors",
                                      cpiTargetYear = NULL,
                                      createCpiTable = TRUE,
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                      resultsSchema = NULL) {
  
  # Start timing
  startTime <- Sys.time()
  
  # Validate inputs
  checkmate::assertClass(connection, "DatabaseConnectorConnection")
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1)
  checkmate::assertCharacter(cohortDatabaseSchema, len = 1)
  checkmate::assertCharacter(cohortTable, len = 1)
  checkmate::assertIntegerish(cohortId, len = 1)
  checkmate::assertNumeric(startOffsetDays, len = 1)
  checkmate::assertNumeric(endOffsetDays, len = 1)
  checkmate::assertLogical(microCosting, len = 1)
  checkmate::assertLogical(cpiAdjustment, len = 1)
  
  # Handle CPI parameters
  if (cpiAdjustment) {
    if (is.null(cpiDatabaseSchema)) {
      cpiDatabaseSchema <- tempEmulationSchema
    }
    
    if (is.null(cpiTargetYear)) {
      cpiTargetYear <- as.integer(format(Sys.Date(), "%Y"))
      cli::cli_alert_info("No target year specified, using current year: {cpiTargetYear}")
    }
    
    # Check if CPI table exists, create if needed
    if (createCpiTable) {
      fullCpiTable <- paste(cpiDatabaseSchema, cpiTable, sep = ".")
      tableExists <- tryCatch({
        sql <- SqlRender::render("SELECT 1 FROM @table WHERE 1=0;", table = fullCpiTable)
        sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
        DatabaseConnector::executeSql(connection, sql)
        TRUE
      }, error = function(e) {
        FALSE
      })
      
      if (!tableExists) {
        cli::cli_alert_info("Creating CPI table with default data")
        createCpiTable(
          connection = connection,
          cpiDatabaseSchema = cpiDatabaseSchema,
          cpiTable = cpiTable
        )
      }
    }
    
    # Validate CPI parameters
    validateCpiParameters(
      connection = connection,
      cpiDatabaseSchema = cpiDatabaseSchema,
      cpiTable = cpiTable,
      targetYear = cpiTargetYear
    )
  }
  
  # Prepare SQL parameters
  sqlParams <- list(
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_id = cohortId,
    start_offset_days = startOffsetDays,
    end_offset_days = endOffsetDays,
    has_visit_restriction = !is.null(visitConceptIds),
    visit_concept_ids = if (!is.null(visitConceptIds)) paste(visitConceptIds, collapse = ",") else "",
    has_event_filter = !is.null(eventFilterTable),
    event_filter_table = ifelse(!is.null(eventFilterTable), eventFilterTable, ""),
    micro_costing = microCosting,
    cpi_adjustment = cpiAdjustment,
    cpi_table = cpiTable,
    cpi_target_year = cpiTargetYear,
    temp_database_schema = ifelse(cpiAdjustment, cpiDatabaseSchema, tempEmulationSchema)
  )
  
  # Choose appropriate SQL file
  sqlFile <- ifelse(
    cpiAdjustment,
    "MainCostUtilizationWithCpi.sql",
    "MainCostUtilization.sql"
  )
  
  # Read and render SQL
  sql <- SqlRender::readSql(system.file("sql", sqlFile, package = "CostUtilization"))
  sql <- SqlRender::render(sql, warnOnMissingParameters = FALSE, .dots = sqlParams)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
  
  # Execute analysis
  cli::cli_alert_info("Executing cost analysis{ifelse(cpiAdjustment, ' with CPI adjustment', '')}")
  
  results <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  
  # Add metadata
  attr(results, "analysisParameters") <- list(
    cohortId = cohortId,
    startOffsetDays = startOffsetDays,
    endOffsetDays = endOffsetDays,
    visitConceptIds = visitConceptIds,
    microCosting = microCosting,
    cpiAdjustment = cpiAdjustment,
    cpiTargetYear = if (cpiAdjustment) cpiTargetYear else NULL
  )
  
  # Report execution time
  executionTime <- difftime(Sys.time(), startTime, units = "secs")
  cli::cli_alert_success("Analysis completed in {round(executionTime, 2)} seconds")
  cli::cli_alert_info("Returned {nrow(results)} rows")
  
  return(results)
}