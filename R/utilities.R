#' Utility Functions for CostUtilization Package
#' 
#' @description
#' This file contains utility functions used throughout the CostUtilization package
#' for logging, validation, cleanup, and other common operations.

#' Log Messages with Different Levels
#' @param message Character string message to log
#' @param verbose Logical; if TRUE, print the message
#' @param level Character string indicating message level ("INFO", "DEBUG", "WARNING", "ERROR", "SUCCESS")
#' @noRd
logMessage <- function(message, verbose = TRUE, level = "INFO") {
  if (!verbose) return(invisible(NULL))
  
  switch(level,
    "INFO" = cli::cli_alert_info(message),
    "DEBUG" = cli::cli_alert(message),
    "WARNING" = cli::cli_alert_warning(message),
    "ERROR" = cli::cli_alert_danger(message),
    "SUCCESS" = cli::cli_alert_success(message),
    cli::cli_alert_info(message)  # Default
  )
}

#' Execute SQL Statements with Error Handling
#' @param connection DatabaseConnector connection object
#' @param sqlStatements Character vector of SQL statements
#' @param verbose Logical; if TRUE, show progress
#' @noRd
executeSqlStatements <- function(connection, sqlStatements, verbose = TRUE) {
  for (i in seq_along(sqlStatements)) {
    if (verbose && length(sqlStatements) > 5) {
      cli::cli_progress_step("Executing SQL statement {i} of {length(sqlStatements)}")
    }
    
    tryCatch({
      DatabaseConnector::executeSql(connection, sqlStatements[i])
    }, error = function(e) {
      cli::cli_abort(c(
        "SQL execution failed at statement {i}",
        "x" = "Error: {e$message}",
        "i" = "Statement: {substr(sqlStatements[i], 1, 100)}..."
      ))
    })
  }
}

#' Clean Up Temporary Tables
#' @param connection DatabaseConnector connection object
#' @param schema Schema name (can be NULL)
#' @param ... Table names to drop
#' @noRd
cleanupTempTables <- function(connection, schema = NULL, ...) {
  tableNames <- list(...)
  tableNames <- tableNames[!sapply(tableNames, is.null)]
  
  if (length(tableNames) == 0) return(invisible(NULL))
  
  for (tableName in tableNames) {
    if (is.null(tableName)) next
    
    fullTableName <- if (!is.null(schema)) {
      paste(schema, tableName, sep = ".")
    } else {
      tableName
    }
    
    tryCatch({
      DatabaseConnector::executeSql(
        connection, 
        glue::glue("DROP TABLE IF EXISTS {fullTableName};")
      )
    }, error = function(e) {
      # Silently ignore cleanup errors
      invisible(NULL)
    })
  }
}

#' Validate Event Filters Structure
#' @param eventFilters List of event filter specifications
#' @noRd
validateEventFilters <- function(eventFilters) {
  if (!is.list(eventFilters)) {
    cli::cli_abort("{.arg eventFilters} must be a list")
  }
  
  if (length(eventFilters) == 0) {
    cli::cli_abort("{.arg eventFilters} cannot be empty")
  }
  
  # Check each filter
  for (i in seq_along(eventFilters)) {
    filter <- eventFilters[[i]]
    
    if (!is.list(filter)) {
      cli::cli_abort("Event filter {i} must be a list")
    }
    
    # Required fields
    required_fields <- c("name", "domain", "conceptIds")
    missing_fields <- setdiff(required_fields, names(filter))
    
    if (length(missing_fields) > 0) {
      cli::cli_abort(c(
        "Event filter {i} is missing required fields:",
        "x" = "Missing: {paste(missing_fields, collapse = ', ')}",
        "i" = "Required fields: {paste(required_fields, collapse = ', ')}"
      ))
    }
    
    # Validate field types
    checkmate::assertString(filter$name, min.chars = 1)
    checkmate::assertChoice(filter$domain, 
      choices = c("Drug", "Procedure", "Condition", "Measurement", "Observation", "Device", "Visit", "All"))
    checkmate::assertIntegerish(filter$conceptIds, lower = 1, min.len = 1, unique = TRUE)
  }
  
  # Check for duplicate filter names
  filterNames <- purrr::map_chr(eventFilters, "name")
  duplicateNames <- duplicated(filterNames)
  
  if (any(duplicateNames)) {
    duplicates <- unique(filterNames[duplicateNames])
    cli::cli_abort(c(
      "Event filter names must be unique",
      "x" = "Duplicate names: {paste(duplicates, collapse = ', ')}"
    ))
  }
  
  invisible(TRUE)
}

#' Create Example Event Filters for Common Conditions
#' @param condition Character string specifying the condition ("diabetes", "cardiovascular", "cancer")
#' @return List of event filters
#' @export
#' @examples
#' \dontrun{
#' diabetesFilters <- createExampleEventFilters("diabetes")
#' cvFilters <- createExampleEventFilters("cardiovascular")
#' }
createExampleEventFilters <- function(condition = c("diabetes", "cardiovascular", "cancer")) {
  condition <- match.arg(condition)
  
  switch(condition,
    "diabetes" = list(
      list(
        name = "Type 2 Diabetes",
        domain = "Condition",
        conceptIds = c(201820L, 201826L, 443238L, 442793L)
      ),
      list(
        name = "Diabetes Medications",
        domain = "Drug", 
        conceptIds = c(1503297L, 1502826L, 1502855L, 1560171L)
      ),
      list(
        name = "Diabetes Monitoring",
        domain = "Measurement",
        conceptIds = c(3004501L, 3003309L, 3034639L)
      )
    ),
    
    "cardiovascular" = list(
      list(
        name = "Myocardial Infarction",
        domain = "Condition",
        conceptIds = c(4329847L, 314666L, 4108217L)
      ),
      list(
        name = "Cardiac Procedures", 
        domain = "Procedure",
        conceptIds = c(4336464L, 4178904L, 4019824L)
      ),
      list(
        name = "Cardiac Medications",
        domain = "Drug",
        conceptIds = c(1308216L, 1310149L, 1545958L, 1549686L)
      ),
      list(
        name = "Cardiac Biomarkers",
        domain = "Measurement", 
        conceptIds = c(3016407L, 3005593L, 3027114L)
      )
    ),
    
    "cancer" = list(
      list(
        name = "Malignant Neoplasms",
        domain = "Condition",
        conceptIds = c(443392L, 4112853L, 4263367L)
      ),
      list(
        name = "Cancer Treatments",
        domain = "Procedure", 
        conceptIds = c(4273629L, 4082734L, 4028908L)
      ),
      list(
        name = "Chemotherapy",
        domain = "Drug",
        conceptIds = c(1378382L, 1386957L, 1394973L)
      ),
      list(
        name = "Tumor Markers",
        domain = "Measurement",
        conceptIds = c(3003396L, 3019550L, 3024928L)
      )
    )
  )
}

#' Validate Cost Concept IDs
#' @param costConceptId Integer cost concept ID
#' @return Logical indicating if valid
#' @noRd
validateCostConceptId <- function(costConceptId) {
  validCostConcepts <- c(
    31973L,  # Total charge
    31978L,  # Total charge (alternative)
    31980L,  # Total cost
    31981L,  # Paid by patient
    31982L,  # Paid by payer
    31974L,  # Paid patient copay
    31975L,  # Paid patient coinsurance
    31976L,  # Paid patient deductible
    31979L   # Amount allowed
  )
  
  if (!costConceptId %in% validCostConcepts) {
    cli::cli_warn(c(
      "Cost concept ID {costConceptId} is not a standard OMOP cost concept",
      "i" = "Standard concepts: {paste(validCostConcepts, collapse = ', ')}"
    ))
  }
  
  return(costConceptId %in% validCostConcepts)
}

#' Validate Currency Concept IDs
#' @param currencyConceptId Integer currency concept ID
#' @return Logical indicating if valid
#' @noRd
validateCurrencyConceptId <- function(currencyConceptId) {
  validCurrencyConcepts <- c(
    44818668L,  # USD
    44818669L,  # EUR
    44818670L,  # GBP
    44818671L,  # JPY
    44818672L   # CAD
  )
  
  if (!currencyConceptId %in% validCurrencyConcepts) {
    cli::cli_warn(c(
      "Currency concept ID {currencyConceptId} is not a standard OMOP currency concept",
      "i" = "Standard concepts: {paste(validCurrencyConcepts, collapse = ', ')}"
    ))
  }
  
  return(currencyConceptId %in% validCurrencyConcepts)
}

#' Format Cost Values for Display
#' @param cost Numeric cost value
#' @param currency Character currency symbol (default "$")
#' @return Formatted character string
#' @export
#' @examples
#' formatCost(1234.56)  # "$1,234.56"
#' formatCost(1234.56, "€")  # "€1,234.56"
formatCost <- function(cost, currency = "$") {
  if (is.na(cost) || is.null(cost)) return("N/A")
  paste0(currency, formatC(cost, format = "f", digits = 2, big.mark = ","))
}

#' Calculate Cost Per Person Per Time Period
#' @param totalCost Numeric total cost
#' @param personTime Numeric person-time in specified units
#' @param timeUnit Character time unit ("days", "months", "quarters", "years")
#' @return Numeric cost per person per time period
#' @export
#' @examples
#' calculateCostPer(10000, 100, "months")  # Cost per person per month
calculateCostPer <- function(totalCost, personTime, timeUnit = c("days", "months", "quarters", "years")) {
  timeUnit <- match.arg(timeUnit)
  
  if (personTime <= 0) return(0)
  
  totalCost / personTime
}

#' Create Summary Statistics for Cost Data
#' @param costData Data frame with cost information
#' @param costCol Character name of cost column
#' @param groupCol Character name of grouping column (optional)
#' @return Data frame with summary statistics
#' @export
summarizeCosts <- function(costData, costCol = "cost", groupCol = NULL) {
  checkmate::assertDataFrame(costData)
  checkmate::assertString(costCol)
  checkmate::assertChoice(costCol, names(costData))
  
  if (!is.null(groupCol)) {
    checkmate::assertString(groupCol)
    checkmate::assertChoice(groupCol, names(costData))
  }
  
  if (is.null(groupCol)) {
    costData |>
      dplyr::summarise(
        n_records = dplyr::n(),
        n_persons = dplyr::n_distinct(.data$person_id, na.rm = TRUE),
        total_cost = sum(.data[[costCol]], na.rm = TRUE),
        mean_cost = mean(.data[[costCol]], na.rm = TRUE),
        median_cost = stats::median(.data[[costCol]], na.rm = TRUE),
        min_cost = min(.data[[costCol]], na.rm = TRUE),
        max_cost = max(.data[[costCol]], na.rm = TRUE),
        sd_cost = stats::sd(.data[[costCol]], na.rm = TRUE),
        .groups = "drop"
      )
  } else {
    costData |>
      dplyr::group_by(.data[[groupCol]]) |>
      dplyr::summarise(
        n_records = dplyr::n(),
        n_persons = dplyr::n_distinct(.data$person_id, na.rm = TRUE),
        total_cost = sum(.data[[costCol]], na.rm = TRUE),
        mean_cost = mean(.data[[costCol]], na.rm = TRUE),
        median_cost = stats::median(.data[[costCol]], na.rm = TRUE),
        min_cost = min(.data[[costCol]], na.rm = TRUE),
        max_cost = max(.data[[costCol]], na.rm = TRUE),
        sd_cost = stats::sd(.data[[costCol]], na.rm = TRUE),
        .groups = "drop"
      )
  }
}

#' Check Database Connection and Schema Access
#' @param connection DatabaseConnector connection object
#' @param cdmDatabaseSchema Character schema name
#' @return Logical indicating if connection is valid
#' @noRd
checkDatabaseConnection <- function(connection, cdmDatabaseSchema) {
  tryCatch({
    # Test basic connection
    DatabaseConnector::querySql(connection, "SELECT 1 as test;")
    
    # Test schema access
    tables <- DatabaseConnector::getTableNames(connection, cdmDatabaseSchema)
    
    # Check for required tables
    requiredTables <- c("person", "observation_period", "visit_occurrence", "cost")
    missingTables <- setdiff(tolower(requiredTables), tolower(tables))
    
    if (length(missingTables) > 0) {
      cli::cli_warn(c(
        "Some required tables are missing from schema '{cdmDatabaseSchema}':",
        "x" = "Missing: {paste(missingTables, collapse = ', ')}"
      ))
      return(FALSE)
    }
    
    return(TRUE)
  }, error = function(e) {
    cli::cli_abort(c(
      "Database connection test failed",
      "x" = "Error: {e$message}"
    ))
  })
}

#' Print Method for CostOfCareSettings
#' @param x CostOfCareSettings object
#' @param ... Additional arguments (ignored)
#' @export
print.CostOfCareSettings <- function(x, ...) {
  cli::cli_h2("Cost of Care Analysis Settings")
  
  cli::cli_h3("Time Window")
  cli::cli_ul(c(
    "Anchor Column: {x$anchorCol}",
    "Start Offset: {x$startOffsetDays} days",
    "End Offset: {x$endOffsetDays} days",
    "Window Length: {x$endOffsetDays - x$startOffsetDays} days"
  ))
  
  cli::cli_h3("Cost Parameters")
  cli::cli_ul(c(
    "Cost Concept ID: {x$costConceptId}",
    "Currency Concept ID: {x$currencyConceptId}"
  ))
  
  if (x$hasVisitRestriction) {
    cli::cli_h3("Visit Restrictions")
    cli::cli_ul("Restricted to {length(x$restrictVisitConceptIds)} visit concept(s)")
  }
  
  if (x$hasEventFilters) {
    cli::cli_h3("Event Filters")
    cli::cli_ul("Number of filters: {x$nFilters}")
    
    for (i in seq_along(x$eventFilters)) {
      filter <- x$eventFilters[[i]]
      cli::cli_ul("{filter$name} ({filter$domain}): {length(filter$conceptIds)} concepts")
    }
  }
  
  if (x$microCosting) {
    cli::cli_h3("Micro-Costing")
    cli::cli_ul(c(
      "Enabled: Yes",
      "Primary Filter: {x$primaryEventFilterName}"
    ))
  }
  
  invisible(x)
}