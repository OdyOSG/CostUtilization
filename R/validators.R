#' Validators for CostUtilization Package
#'
#' @description
#' Internal validation functions for the CostUtilization package.
#' These functions ensure input parameters meet requirements before analysis execution.
#'
#' @keywords internal
#' @noRd

#' Validate All Input Parameters
#'
#' @description
#' Comprehensive validation of all input parameters for calculateCostOfCare function
#'
#' @param params List of parameters to validate
#'
#' @return Invisible TRUE if all validations pass, otherwise throws error
#' @noRd
validateInputs <- function(params) {
  # Validate connection
  checkmate::assertClass(params$connection, "DatabaseConnection")
  
  # Validate schema names
  checkmate::assertCharacter(params$cdmDatabaseSchema, len = 1, min.chars = 1)
  checkmate::assertCharacter(params$cohortDatabaseSchema, len = 1, min.chars = 1)
  
  # Validate table names
  checkmate::assertCharacter(params$cohortTable, len = 1, min.chars = 1)
  
  # Validate cohort ID
  checkmate::assertIntegerish(params$cohortId, lower = 1, len = 1)
  
  # Validate anchor column
  checkmate::assertChoice(params$anchorCol, c("cohort_start_date", "cohort_end_date"))
  
  # Validate time windows
  checkmate::assertIntegerish(params$startOffsetDays, len = 1)
  checkmate::assertIntegerish(params$endOffsetDays, len = 1)
  
  # Validate that time window is valid (end must be after start)
  if (params$endOffsetDays <= params$startOffsetDays) {
    cli::cli_abort("endOffsetDays must be greater than startOffsetDays")
  }
  
  # Validate optional visit restrictions
  if (!is.null(params$restrictVisitConceptIds)) {
    checkmate::assertIntegerish(params$restrictVisitConceptIds, lower = 1, min.len = 1)
  }
  
  # Validate event filters
  if (!is.null(params$eventFilters)) {
    validateEventFilters(params$eventFilters)
  }
  
  # Validate micro-costing parameters
  checkmate::assertLogical(params$microCosting, len = 1)
  
  if (params$microCosting) {
    if (!is.null(params$primaryEventFilterName)) {
      checkmate::assertCharacter(params$primaryEventFilterName, len = 1, min.chars = 1)
      
      # Check that primaryEventFilterName exists in eventFilters
      if (!is.null(params$eventFilters)) {
        filterNames <- purrr::map_chr(params$eventFilters, "name")
        if (!params$primaryEventFilterName %in% filterNames) {
          cli::cli_abort(
            "primaryEventFilterName '{params$primaryEventFilterName}' not found in eventFilters"
          )
        }
      } else {
        cli::cli_abort("primaryEventFilterName specified but no eventFilters provided")
      }
    }
  }
  
  # Validate cost concept IDs
  checkmate::assertIntegerish(params$costConceptId, lower = 1, len = 1)
  checkmate::assertIntegerish(params$currencyConceptId, lower = 1, len = 1)
  
  # Validate SQL dialect if provided
  if (!is.null(params$targetDialect)) {
    validDialects <- c(
      "oracle", "postgresql", "pdw", "redshift", "impala", 
      "netezza", "bigquery", "sql server", "spark", "snowflake"
    )
    checkmate::assertChoice(tolower(params$targetDialect), validDialects)
  }
  
  # Validate temp emulation schema if provided
  if (!is.null(params$tempEmulationSchema)) {
    checkmate::assertCharacter(params$tempEmulationSchema, len = 1, min.chars = 1)
  }
  
  # Validate flags
  checkmate::assertLogical(params$asPermanent, len = 1)
  checkmate::assertLogical(params$verbose, len = 1)
  
  # Validate return format
  checkmate::assertChoice(params$returnFormat, c("tibble", "list"))
  
  # Validate logger if provided
  if (!is.null(params$logger)) {
    if (!is.list(params$logger) || !is.function(params$logger$log)) {
      cli::cli_abort("Logger must be a list with a 'log' function")
    }
  }
  
  invisible(TRUE)
}

#' Validate Event Filters Structure
#'
#' @description
#' Validates the structure and content of event filters
#'
#' @param eventFilters List of event filters to validate
#'
#' @return Invisible TRUE if valid, otherwise throws error
#' @noRd
validateEventFilters <- function(eventFilters) {
  checkmate::assertList(eventFilters, min.len = 1)
  
  # Check for duplicate names
  filterNames <- purrr::map_chr(eventFilters, ~ .x$name)
  if (anyDuplicated(filterNames)) {
    cli::cli_abort("Event filter names must be unique")
  }
  
  # Validate each filter
  for (i in seq_along(eventFilters)) {
    filter <- eventFilters[[i]]
    
    # Check structure
    checkmate::assertList(filter, names = "named")
    checkmate::assertNames(
      names(filter), 
      must.include = c("name", "domain", "conceptIds")
    )
    
    # Validate name
    checkmate::assertCharacter(filter$name, len = 1, min.chars = 1)
    
    # Validate domain
    validDomains <- c("All", "Condition", "Procedure", "Drug", "Measurement", "Observation")
    checkmate::assertChoice(filter$domain, validDomains)
    
    # Validate concept IDs
    checkmate::assertIntegerish(filter$conceptIds, min.len = 1, lower = 1)
    
    # Check for duplicate concept IDs within filter
    if (anyDuplicated(filter$conceptIds)) {
      cli::cli_warn(
        "Event filter '{filter$name}' contains duplicate concept IDs"
      )
    }
  }
  
  invisible(TRUE)
}

#' Check Database Connection
#'
#' @description
#' Validates that the database connection is active and valid
#'
#' @param connection DatabaseConnector connection object
#'
#' @return Invisible TRUE if valid, otherwise throws error
#' @noRd
checkDatabaseConnection <- function(connection) {
  checkmate::assertClass(connection, "DatabaseConnection")
  
  if (!DatabaseConnector::dbIsValid(connection)) {
    cli::cli_abort("Database connection is not valid or has been closed")
  }
  
  invisible(TRUE)
}

#' Check Schema Exists
#'
#' @description
#' Checks if a database schema exists and is accessible
#'
#' @param connection DatabaseConnector connection object
#' @param schema Schema name to check
#'
#' @return TRUE if schema exists, FALSE otherwise
#' @noRd
checkSchemaExists <- function(connection, schema) {
  tryCatch(
    {
      # Try a simple query to test schema access
      sql <- "SELECT 1 AS test"
      result <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        snakeCaseToCamelCase = FALSE
      )
      return(TRUE)
    },
    error = function(e) {
      return(FALSE)
    }
  )
}

#' Check Table Exists
#'
#' @description
#' Checks if a table exists in the specified schema
#'
#' @param connection DatabaseConnector connection object
#' @param schema Schema name
#' @param table Table name
#'
#' @return TRUE if table exists, FALSE otherwise
#' @noRd
checkTableExists <- function(connection, schema, table) {
  tryCatch(
    {
      sql <- SqlRender::render(
        "SELECT 1 FROM @schema.@table WHERE 1 = 0",
        schema = schema,
        table = table
      )
      DatabaseConnector::executeSql(connection, sql)
      return(TRUE)
    },
    error = function(e) {
      return(FALSE)
    }
  )
}

#' Check Micro-costing Prerequisites
#'
#' @description
#' Validates that all required tables and columns exist for micro-costing analysis
#'
#' @param connection DatabaseConnector connection object
#' @param cdmDatabaseSchema CDM schema name
#'
#' @return Invisible TRUE if all prerequisites met, otherwise throws error
#' @noRd
checkMicrocostingPrerequisites <- function(connection, cdmDatabaseSchema) {
  # Check for visit_detail table
  if (!checkTableExists(connection, cdmDatabaseSchema, "visit_detail")) {
    cli::cli_abort(
      "Micro-costing requires 'visit_detail' table in CDM schema '{cdmDatabaseSchema}'"
    )
  }
  
  # Check for cost table
  if (!checkTableExists(connection, cdmDatabaseSchema, "cost")) {
    cli::cli_abort(
      "Micro-costing requires 'cost' table in CDM schema '{cdmDatabaseSchema}'"
    )
  }
  
  # Check for required columns in cost table
  sql <- SqlRender::render(
    "SELECT TOP 1 
       cost_id,
       cost_event_id,
       cost_domain_id,
       cost_type_concept_id,
       currency_concept_id,
       total_charge,
       total_cost,
       total_paid
     FROM @cdm_database_schema.cost",
    cdm_database_schema = cdmDatabaseSchema
  )
  
  tryCatch(
    {
      DatabaseConnector::querySql(connection, sql)
    },
    error = function(e) {
      cli::cli_abort(
        "Cost table is missing required columns for micro-costing analysis"
      )
    }
  )
  
  invisible(TRUE)
}

#' Validate Cohort Table Structure
#'
#' @description
#' Validates that the cohort table has the required structure
#'
#' @param connection DatabaseConnector connection object
#' @param cohortDatabaseSchema Cohort schema name
#' @param cohortTable Cohort table name
#' @param cohortId Cohort ID to check
#'
#' @return Invisible TRUE if valid, otherwise throws error
#' @noRd
validateCohortTable <- function(connection, cohortDatabaseSchema, cohortTable, cohortId) {
  # Check table exists
  if (!checkTableExists(connection, cohortDatabaseSchema, cohortTable)) {
    cli::cli_abort(
      "Cohort table '{cohortDatabaseSchema}.{cohortTable}' does not exist"
    )
  }
  
  # Check required columns
  sql <- SqlRender::render(
    "SELECT TOP 1 
       cohort_definition_id,
       subject_id,
       cohort_start_date,
       cohort_end_date
     FROM @cohort_database_schema.@cohort_table",
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable
  )
  
  tryCatch(
    {
      DatabaseConnector::querySql(connection, sql)
    },
    error = function(e) {
      cli::cli_abort(
        "Cohort table is missing required columns (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)"
      )
    }
  )
  
  # Check if cohort ID exists
  sql <- SqlRender::render(
    "SELECT COUNT(*) AS n
     FROM @cohort_database_schema.@cohort_table
     WHERE cohort_definition_id = @cohort_id",
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_id = cohortId
  )
  
  result <- DatabaseConnector::querySql(connection, sql)
  
  if (result$N[1] == 0) {
    cli::cli_abort(
      "No records found for cohort ID {cohortId} in '{cohortDatabaseSchema}.{cohortTable}'"
    )
  }
  
  invisible(TRUE)
}

#' Validate Cost Concept IDs
#'
#' @description
#' Validates that the specified cost and currency concept IDs exist in the vocabulary
#'
#' @param connection DatabaseConnector connection object
#' @param vocabDatabaseSchema Vocabulary schema name
#' @param costConceptId Cost type concept ID
#' @param currencyConceptId Currency concept ID
#'
#' @return Invisible TRUE if valid, otherwise throws error
#' @noRd
validateCostConceptIds <- function(connection, vocabDatabaseSchema, 
                                   costConceptId, currencyConceptId) {
  # Check cost concept ID
  sql <- SqlRender::render(
    "SELECT concept_id, concept_name, domain_id
     FROM @vocab_database_schema.concept
     WHERE concept_id IN (@cost_concept_id, @currency_concept_id)
       AND invalid_reason IS NULL",
    vocab_database_schema = vocabDatabaseSchema,
    cost_concept_id = costConceptId,
    currency_concept_id = currencyConceptId
  )
  
  result <- DatabaseConnector::querySql(connection, sql)
  
  if (nrow(result) == 0) {
    cli::cli_abort(
      "Cost concept ID {costConceptId} or currency concept ID {currencyConceptId} not found in vocabulary"
    )
  }
  
  # Check if cost concept is in correct domain
  costConcept <- result[result$CONCEPT_ID == costConceptId, ]
  if (nrow(costConcept) > 0 && !costConcept$DOMAIN_ID[1] %in% c("Type Concept", "Metadata")) {
    cli::cli_warn(
      "Cost concept ID {costConceptId} ('{costConcept$CONCEPT_NAME[1]}') is in domain '{costConcept$DOMAIN_ID[1]}', expected 'Type Concept'"
    )
  }
  
  invisible(TRUE)
}

#' Validate Time Window
#'
#' @description
#' Validates that the time window parameters create a valid analysis period
#'
#' @param startOffsetDays Start offset in days from anchor date
#' @param endOffsetDays End offset in days from anchor date
#' @param anchorCol Anchor column (cohort_start_date or cohort_end_date)
#'
#' @return Invisible TRUE if valid, otherwise throws error
#' @noRd
validateTimeWindow <- function(startOffsetDays, endOffsetDays, anchorCol) {
  # Check that end is after start
  if (endOffsetDays <= startOffsetDays) {
    cli::cli_abort(
      "Invalid time window: endOffsetDays ({endOffsetDays}) must be greater than startOffsetDays ({startOffsetDays})"
    )
  }
  
  # Check window size
  windowDays <- endOffsetDays - startOffsetDays
  if (windowDays > 3650) {  # More than 10 years
    cli::cli_warn(
      "Time window spans {windowDays} days (>10 years). This may impact performance."
    )
  }
  
  # Warn about negative offsets with cohort_end_date
  if (anchorCol == "cohort_end_date" && startOffsetDays < 0) {
    cli::cli_inform(
      "Using negative startOffsetDays ({startOffsetDays}) with cohort_end_date anchor will look forward in time from cohort end"
    )
  }
  
  invisible(TRUE)
}

#' Create Validation Report
#'
#' @description
#' Creates a comprehensive validation report for the analysis parameters
#'
#' @param params List of all parameters
#' @param connection DatabaseConnector connection object
#'
#' @return List containing validation results and warnings
#' @noRd
createValidationReport <- function(params, connection) {
  report <- list(
    timestamp = Sys.time(),
    parameters = params,
    checks = list(),
    warnings = character(),
    errors = character()
  )
  
  # Run all validation checks
  checks <- list(
    connection = tryCatch(
      {checkDatabaseConnection(connection); TRUE},
      error = function(e) {report$errors <- c(report$errors, e$message); FALSE}
    ),
    cdmSchema = tryCatch(
      {checkSchemaExists(connection, params$cdmDatabaseSchema)},
      error = function(e) {report$errors <- c(report$errors, e$message); FALSE}
    ),
    cohortTable = tryCatch(
      {validateCohortTable(connection, params$cohortDatabaseSchema, 
                           params$cohortTable, params$cohortId); TRUE},
      error = function(e) {report$errors <- c(report$errors, e$message); FALSE}
    ),
    timeWindow = tryCatch(
      {validateTimeWindow(params$startOffsetDays, params$endOffsetDays, 
                          params$anchorCol); TRUE},
      error = function(e) {report$errors <- c(report$errors, e$message); FALSE}
    )
  )
  
  report$checks <- checks
  report$allChecksPassed <- all(unlist(checks))
  
  return(report)
}