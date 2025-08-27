cleanupTempTables <- function(connection, schema = NULL, ...) {
  # Basic validation
  if (!DBI::dbIsValid(connection)) {
    abort("`connection` is not a valid DBI connection.")
  }
  tables <- rlang::list2(...)
  if (length(tables) == 0L) return(invisible(NULL))
  
  # Local helper: build a fully qualified, safely quoted identifier
  quoteIdent <- function(conn, tbl, schema = NULL) {
    if (!is.null(schema) && nzchar(schema %||% "")) {
      id <- DBI::Id(schema = schema, table = tbl)
    } else {
      id <- DBI::Id(table = tbl)
    }
    DBI::dbQuoteIdentifier(conn, id)
  }
  
  dropWithIfExists <- function(conn, qident) {
    sql <- SQL(glue("DROP TABLE IF EXISTS {qident};"))
    DBI::dbExecute(conn, sql)
  }
  dropWithoutIfExists <- function(conn, qident) {
    sql <- SQL(glue("DROP TABLE {qident};"))
    DBI::dbExecute(conn, sql)
  }
  
  purrr::walk(tables, ~{
    tbl <- .x
    if (is.null(tbl) || !nzchar(tbl)) return(invisible(NULL))
    
    qident <- quoteIdent(connection, tbl, schema)
    
    tryCatch(
      {
        tryCatch(
          dropWithIfExists(connection, qident),
          error = function(eIf) {
            tryCatch(
              dropWithoutIfExists(connection, qident),
              error = function(eDrop) invisible(NULL)
            )
          }
        )
      },
      error = function(e) invisible(NULL)
    )
    
    invisible(NULL)
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
executeSqlStatements <- function(connection, sqlStatements, verbose = TRUE, quiet_db = TRUE) {
  nStatements <- length(sqlStatements)
  if (nStatements == 0L) return(invisible(NULL))
  
  previewStmt <- function(x, n = 120L) {
    x <- gsub("[\r\n]+", " ", x, perl = TRUE)
    if (nchar(x) > n) paste0(substr(x, 1L, n), "...") else x
  }
  
  pbId <- NULL
  if (verbose && nStatements > 1L) {
    pbId <- cli::cli_progress_bar(
      "Executing SQL statements",
      total = nStatements,
      clear = TRUE
    )
  }
  
  on.exit({
    if (!is.null(pbId)) try(cli::cli_progress_done(id = pbId), silent = TRUE)
  }, add = TRUE)
  
  t0 <- proc.time()[["elapsed"]]
  
  for (i in seq_len(nStatements)) {
    sql <- sqlStatements[[i]]
    if (is.null(sql) || !nzchar(trimws(sql))) {
      if (!is.null(pbId)) cli::cli_progress_update(id = pbId, inc = 1)
      next
    }
    
    
    
    if (quiet_db) {
      res <- try(executeOne(connection, sql), silent = TRUE)
    } else {
      # still use quietly, but we can print warnings/messages manually if wanted
      res <- try(executeOne(connection, sql), silent = TRUE)
      if (!inherits(res, "try-error")) {
        if (length(res$messages)) cli::cli_inform(res$messages)
        if (length(res$warnings)) cli::cli_warn(res$warnings)
      }
    }
    
    if (inherits(res, "try-error") || !is.null(res$error)) {
      if (!is.null(pbId)) try(cli::cli_progress_done(id = pbId), silent = TRUE)
      cli::cli_abort(
        c(
          "Error executing SQL statement {i} of {nStatements}.",
          "x" = "{conditionMessage(if (inherits(res, 'try-error')) attr(res, 'condition') else res$error)}",
          "i" = "Statement preview: {previewStmt(sql)}"
        ),
        .envir = rlang::env(i = i, nStatements = nStatements, sql = sql, previewStmt = previewStmt)
      )
    }
    
    if (!is.null(pbId)) cli::cli_progress_update(id = pbId, inc = 1)
  }
  
  total_secs <- round(proc.time()[["elapsed"]] - t0, 3)
  
  if (verbose) {
    cli::cli_inform(c(
      "v" = "Executed {nStatements} SQL statement{if (nStatements != 1L) 's' else ''} in {total_secs} secs."
    ))
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



# Helpers (simple, focused)
.int_flag <- function(x) as.integer(isTRUE(x))

.qualify <- function(table, schema) {
  if (!is.null(schema) && nzchar(schema %||% "") && !is.null(table) && nzchar(table %||% "")) {
    paste(schema, table, sep = ".")
  } else {
    table
  }
}

executeOne <- purrr::quietly(DBI::dbExecute)




#' Insert a data.frame into a DBI connection (replacement for DatabaseConnector::insertTable)
#'
#' @param connection A DBI connection.
#' @param tableName Target table name (character).
#' @param data A data.frame or tibble to insert.
#' @param tempTable Logical, create a temporary table if supported.
#' @param tempEmulationSchema Optional schema name to emulate temporary tables (e.g. for Oracle).
#' @param camelCaseToSnakeCase Logical, convert column names before insert.
#'
#' @return Invisibly TRUE on success.
insertTableDBI <- function(connection,
                           tableName,
                           data,
                           tempTable = FALSE,
                           tempEmulationSchema = NULL,
                           camelCaseToSnakeCase = FALSE) {

  # Optionally rename columns
  if (isTRUE(camelCaseToSnakeCase)) {
    names(data) <- SqlRender::camelCaseToSnakeCase(names(data))
  }
  
  # Handle schema vs. temp table
  if (!is.null(tempEmulationSchema) && nzchar(tempEmulationSchema)) {
    id <- DBI::Id(schema = tempEmulationSchema, table = tableName)
  } else {
    id <- DBI::Id(table = tableName)
  }
  
  DBI::dbWriteTable(
    conn      = connection,
    name      = id,
    value     = data,
    temporary = tempTable,
    overwrite = TRUE
  )
  
  invisible(TRUE)
}