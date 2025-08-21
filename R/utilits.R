#' Clean up temporary tables
#' 
#' @description
#' Safely drops temporary tables from the database.
#' 
#' @param connection DatabaseConnector connection
#' @param schema Schema name (can be NULL for temp tables)
#' @param ... Table names to drop
#' 
#' @return NULL (invisibly)
#' @noRd
cleanupTempTables <- function(connection, schema = NULL, ...) {
  tables <- list(...)
  
  purrr::walk(tables, function(table) {
    if (!is.null(table) && nchar(table) > 0) {
      tryCatch({
        if (!is.null(schema) && nchar(schema) > 0) {
          sql <- "DROP TABLE IF EXISTS @schema.@table;"
          DatabaseConnector::renderTranslateExecuteSql(
            connection,
            sql,
            schema = schema,
            table = table,
            progressBar = FALSE,
            reportOverallTime = FALSE
          )
        } else {
          sql <- "DROP TABLE IF EXISTS @table;"
          DatabaseConnector::renderTranslateExecuteSql(
            connection,
            sql,
            table = table,
            progressBar = FALSE,
            reportOverallTime = FALSE
          )
        }
      }, error = function(e) {
        # Silently ignore errors when dropping tables
        # This is intentional as tables may not exist
        invisible(NULL)
      })
    }
  })
  
  invisible(NULL)
}

#' Log messages with appropriate styling
#' 
#' @description
#' Logs messages to the console with appropriate styling based on level.
#' 
#' @param message The message to log
#' @param verbose Whether to display the message
#' @param level The message level: "INFO", "WARNING", "ERROR", "DEBUG", "SUCCESS"
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
         "DEBUG" = cli::cli_text(cli::col_grey(message)),
         "SUCCESS" = cli::cli_alert_success(message),
         cli::cli_alert(message)
  )
  
  invisible(NULL)
}

#' Execute multiple SQL statements
#' 
#' @description
#' Executes a vector of SQL statements with progress reporting.
#' 
#' @param connection DatabaseConnector connection
#' @param sqlStatements Character vector of SQL statements
#' @param verbose Whether to show progress
#' 
#' @return NULL (invisibly)
#' @noRd
executeSqlStatements <- function(connection, sqlStatements, verbose = TRUE) {
  nStatements <- length(sqlStatements)
  
  if (verbose && nStatements > 1) {
    cli::cli_progress_bar(
      "Executing SQL statements",
      total = nStatements,
      clear = FALSE
    )
  }
  
  purrr::walk(seq_along(sqlStatements), function(i) {
    sql <- sqlStatements[[i]]
    
    # Skip empty statements
    if (is.null(sql) || nchar(trimws(sql)) == 0) {
      return(NULL)
    }
    
    tryCatch({
      DatabaseConnector::executeSql(
        connection = connection,
        sql = sql,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
      
      if (verbose && nStatements > 1) {
        cli::cli_progress_update()
      }
    }, error = function(e) {
      if (verbose && nStatements > 1) {
        cli::cli_progress_done()
      }
      
      cli::cli_abort(c(
        "Error executing SQL statement {i} of {nStatements}",
        "x" = conditionMessage(e),
        "i" = "Statement preview: {substr(sql, 1, 100)}..."
      ))
    })
  })
  
  if (verbose && nStatements > 1) {
    cli::cli_progress_done()
  }
  
  invisible(NULL)
}

#' Format time duration for display
#' 
#' @description
#' Formats a time duration in seconds to a human-readable string.
#' 
#' @param seconds Numeric duration in seconds
#' 
#' @return Character string with formatted duration
#' @noRd
formatDuration <- function(seconds) {
  if (seconds < 60) {
    return(sprintf("%.1f seconds", seconds))
  } else if (seconds < 3600) {
    minutes <- seconds / 60
    return(sprintf("%.1f minutes", minutes))
  } else {
    hours <- seconds / 3600
    return(sprintf("%.1f hours", hours))
  }
}

