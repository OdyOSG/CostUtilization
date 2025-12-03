cleanupTempTables <- function(connection, schema = NULL, ...) {
  # Basic validation
  if (!DatabaseConnector::isConnected(connection)) {
    rlang::abort("`connection` is not a valid DatabaseConnector connection.")
  }
  tables <- rlang::list2(...)
  if (length(tables) == 0L) {
    return(invisible(NULL))
  }

  # Local helper: build a fully qualified table name
  buildTableName <- function(tbl, schema = NULL) {
    if (!is.null(schema) && nzchar(schema %||% "")) {
      paste0(schema, ".", tbl)
    } else {
      tbl
    }
  }

  dropWithIfExists <- function(conn, tableName) {
    sql <- paste0("DROP TABLE IF EXISTS ", tableName, ";")
    DatabaseConnector::executeSql(conn, sql)
  }
  
  dropWithoutIfExists <- function(conn, tableName) {
    sql <- paste0("DROP TABLE ", tableName, ";")
    DatabaseConnector::executeSql(conn, sql)
  }

  for (i in seq_along(tables)) {
    tbl <- tables[[i]]
    if (is.null(tbl) || !nzchar(tbl)) {
      next
    }

    tableName <- buildTableName(tbl, schema)

    tryCatch(
      {
        tryCatch(
          dropWithIfExists(connection, tableName),
          error = function(eIf) {
            tryCatch(
              dropWithoutIfExists(connection, tableName),
              error = function(eDrop) invisible(NULL)
            )
          }
        )
      },
      error = function(e) invisible(NULL)
    )
  }

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
  if (nStatements == 0L) {
    return(invisible(NULL))
  }

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

  on.exit(
    {
      if (!is.null(pbId)) try(cli::cli_progress_done(id = pbId), silent = TRUE)
    },
    add = TRUE
  )

  t0 <- proc.time()[["elapsed"]]

  for (i in seq_len(nStatements)) {
    sql <- sqlStatements[[i]]
    if (is.null(sql) || !nzchar(trimws(sql))) {
      if (!is.null(pbId)) cli::cli_progress_update(id = pbId, inc = 1)
      next
    }

    res <- try({
      if (quiet_db) {
        DatabaseConnector::executeSql(connection, sql)
      } else {
        DatabaseConnector::executeSql(connection, sql, reportOverallTime = FALSE)
      }
    }, silent = TRUE)

    if (inherits(res, "try-error")) {
      if (!is.null(pbId)) try(cli::cli_progress_done(id = pbId), silent = TRUE)
      cli::cli_abort(
        c(
          "Error executing SQL statement {i} of {nStatements}.",
          "x" = "{conditionMessage(attr(res, 'condition'))}",
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

# Helpers (simple, focused)
.int_flag <- function(x) as.integer(isTRUE(x))

to_title_case_base <- function(x) {
  # force lower case
  x <- tolower(x)
  # split on spaces
  words <- strsplit(x, "\\s+")[[1]]
  # uppercase first letter, append rest
  words <- paste0(
    toupper(substring(words, 1, 1)),
    substring(words, 2)
  )
  # rejoin
  paste(words, collapse = " ")
}

#' Find the 1-based index of the primary event filter
#'
#' @description
#' Internal helper to safely find the index of the primary event filter by its name
#' within the list of event filters.
#'
#' @param settings A `CostOfCareSettings` object.
#'
#' @return An integer representing the 1-based index of the matching filter,
#'   or `0L` if not found or if inputs are invalid.
#' @noRd
.findPrimaryFilterId <- function(settings) {
  primaryFilterName <- settings$primaryEventFilterName
  eventFilters <- settings$eventFilters

  # Guard clause: If there's no name to search for or no list to search in, return 0.
  if (is.null(primaryFilterName) || rlang::is_empty(eventFilters)) {
    return(0L)
  }

  # Use base R approach instead of purrr
  for (i in seq_along(eventFilters)) {
    if (identical(eventFilters[[i]]$name, primaryFilterName)) {
      return(i)
    }
  }
  
  return(0L)
}