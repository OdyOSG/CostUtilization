#' Clean up temporary tables
#'
#' @description
#' Removes temporary tables created during analysis execution.
#'
#' @param connection A DatabaseConnector connection object
#' @param schema Schema name where temporary tables exist (can be NULL)
#' @param ... Table names to drop
#'
#' @return NULL (invisibly)
#' @noRd
cleanupTempTables <- function(connection, schema, ...) {
  tables <- list(...)
  for (table in tables) {
    if (!is.null(table)) {
      sql <- "DROP TABLE IF EXISTS @schema.@table;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        sql,
        schema = schema,
        table = table,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
  }
  invisible(NULL)
}

#' Log messages with appropriate formatting
#'
#' @description
#' Outputs formatted log messages using cli package functions based on log level.
#'
#' @param message The message to log
#' @param verbose Whether to output the message
#' @param level Log level: "ERROR", "WARNING", "INFO", "DEBUG", or "SUCCESS"
#'
#' @return NULL (invisibly)
#' @noRd
logMessage <- function(message, verbose = TRUE, level = "INFO") {
  if (!verbose) {
    return(invisible(NULL))
  }
  
  switch(level,
    "ERROR" = cli::cli_alert_danger(message),
    "WARNING" = cli::cli_alert_warning(message),
    "INFO" = cli::cli_alert_info(message),
    "DEBUG" = cli::cli_text(message),
    "SUCCESS" = cli::cli_alert_success(message),
    cli::cli_alert(message)
  )
  
  invisible(NULL)
}

#' Execute SQL statements with error handling
#'
#' @description
#' Executes a vector of SQL statements with proper error handling and logging.
#'
#' @param connection A DatabaseConnector connection object
#' @param sqlStatements Character vector of SQL statements to execute
#' @param verbose Whether to output progress messages
#'
#' @return NULL (invisibly)
#' @noRd
executeSqlStatements <- function(connection, sqlStatements, verbose = TRUE) {
  for (i in seq_along(sqlStatements)) {
    statement <- sqlStatements[i]
    
    # Skip empty statements
    if (trimws(statement) == "") {
      next
    }
    
    logMessage(
      sprintf("Executing SQL statement %d of %d", i, length(sqlStatements)),
      verbose = verbose,
      level = "DEBUG"
    )
    
    tryCatch({
      DatabaseConnector::executeSql(
        connection = connection,
        sql = statement,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }, error = function(e) {
      logMessage(
        sprintf("Error in SQL statement %d: %s", i, e$message),
        verbose = TRUE,
        level = "ERROR"
      )
      stop(e)
    })
  }
  
  invisible(NULL)
}

#' Validate event filters structure
#'
#' @description
#' Validates that event filters have the correct structure and valid values.
#'
#' @param eventFilters A list of event filter specifications
#'
#' @return NULL (invisibly). Throws an error if validation fails.
#' @noRd
validateEventFilters <- function(eventFilters) {
  if (!is.list(eventFilters)) {
    cli::cli_abort("{.arg eventFilters} must be a list")
  }
  
  validDomains <- c("Drug", "Condition", "Procedure", "Measurement", 
                    "Observation", "Device", "Visit", "All")
  
  for (i in seq_along(eventFilters)) {
    filter <- eventFilters[[i]]
    
    # Check required fields
    if (!is.list(filter)) {
      cli::cli_abort("Event filter {i} must be a list")
    }
    
    if (!"name" %in% names(filter) || !is.character(filter$name) || length(filter$name) != 1) {
      cli::cli_abort("Event filter {i} must have a 'name' field (single character string)")
    }
    
    if (!"domain" %in% names(filter) || !is.character(filter$domain) || length(filter$domain) != 1) {
      cli::cli_abort("Event filter {i} must have a 'domain' field (single character string)")
    }
    
    if (!filter$domain %in% validDomains) {
      cli::cli_abort(c(
        "Event filter {i} has invalid domain: {filter$domain}",
        "i" = "Valid domains are: {.val {validDomains}}"
      ))
    }
    
    if (!"conceptIds" %in% names(filter) || !is.numeric(filter$conceptIds) || length(filter$conceptIds) == 0) {
      cli::cli_abort("Event filter {i} must have a 'conceptIds' field (numeric vector with at least one value)")
    }
    
    # Check for duplicate names
    allNames <- purrr::map_chr(eventFilters, "name")
    if (anyDuplicated(allNames)) {
      dupNames <- unique(allNames[duplicated(allNames)])
      cli::cli_abort(c(
        "Event filter names must be unique",
        "x" = "Duplicate names found: {.val {dupNames}}"
      ))
    }
  }
  
  invisible(NULL)
}