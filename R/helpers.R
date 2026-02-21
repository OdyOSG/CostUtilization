cleanupTempTables <- function(connection, schema = NULL, ...) {
  # Basic validation: Replaced rlang::abort with stop
  if (!DBI::dbIsValid(connection)) {
    stop("`connection` is not a valid DBI connection.", call. = FALSE)
  }
  
  # Capture dots: Replaced rlang::list2 with list
  tables <- list(...)
  if (length(tables) == 0L) {
    return(invisible(NULL))
  }
  
  # Local helper: Replaced %||% logic with is.null check
  quoteIdent <- function(conn, tbl, schema = NULL) {
    if (!is.null(schema) && nzchar(schema)) {
      id <- DBI::Id(schema = schema, table = tbl)
    } else {
      id <- DBI::Id(table = tbl)
    }
    DBI::dbQuoteIdentifier(conn, id)
  }
  
  # Helpers: Replaced glue with paste0/sprintf
  dropWithIfExists <- function(conn, qident) {
    DBI::dbExecute(conn, DBI::SQL(paste0("DROP TABLE IF EXISTS ", qident, ";")))
  }
  dropWithoutIfExists <- function(conn, qident) {
    DBI::dbExecute(conn, DBI::SQL(paste0("DROP TABLE ", qident, ";")))
  }
  
  # Replaced purrr::walk with a standard for loop
  for (tbl in tables) {
    if (is.null(tbl) || !nzchar(tbl)) {
      next
    }
    
    qident <- quoteIdent(connection, tbl, schema)
    
    # Nested error handling to attempt "IF EXISTS" then fallback to standard "DROP"
    tryCatch(
      {
        tryCatch(
          dropWithIfExists(connection, qident),
          error = function(eIf) {
            # Some DBs don't support IF EXISTS, attempt a direct drop
            tryCatch(
              dropWithoutIfExists(connection, qident),
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
  
  # Format the prefix based on the level
  prefix <- paste0("[", level, "] ")
  
  # Logic for different output behaviors
  switch(level,
         "ERROR"   = message(paste0("✖ ", prefix, message)),
         "WARNING" = warning(message, call. = FALSE),
         "INFO"    = message(paste0("ℹ ", prefix, message)),
         "DEBUG"   = cat(paste0("# ", message, "\n")),
         "SUCCESS" = message(paste0("✔ ", prefix, message)),
         message(paste0("> ", message))
  )
  
  invisible(NULL)
}

#' Execute multiple SQL statements
#'
#' @description
#' Executes a vector of SQL statements with progress reporting.
#'
#' @param connection DatabaseConnector or DBI connection
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
  
  pb <- NULL
  if (verbose && nStatements > 1L) {
    # Base R Progress Bar
    pb <- txtProgressBar(min = 0, max = nStatements, style = 3)
  }
  
  on.exit({
    if (!is.null(pb)) close(pb)
  }, add = TRUE)
  
  t0 <- proc.time()[["elapsed"]]
  
  for (i in seq_len(nStatements)) {
    sql <- sqlStatements[[i]]
    
    # Skip empty/whitespace statements
    if (is.null(sql) || !nzchar(trimws(sql))) {
      if (!is.null(pb)) setTxtProgressBar(pb, i)
      next
    }
    
    # Execution logic
    # Assumes executeOne returns a list(result, output, warnings, messages)
    res <- tryCatch({
      executeOne(connection, sql)
    }, error = function(e) e)
    
    # Handle errors
    if (inherits(res, "error")) {
      if (!is.null(pb)) close(pb) # Close PB before throwing error to keep console clean
      
      msg <- sprintf(
        "Error executing SQL statement %d of %d.\nReason: %s\nStatement preview: %s",
        i, nStatements, conditionMessage(res), previewStmt(sql)
      )
      stop(msg, call. = FALSE)
    }
    
    # Handle warnings/messages if not quiet
    if (!quiet_db) {
      if (length(res$messages) > 0) lapply(res$messages, message)
      if (length(res$warnings) > 0) lapply(res$warnings, warning, call. = FALSE)
    }
    
    if (!is.null(pb)) setTxtProgressBar(pb, i)
  }
  
  total_secs <- round(proc.time()[["elapsed"]] - t0, 3)
  
  if (verbose) {
    cat(sprintf(
      "\n✔ Executed %d SQL statement%s in %0.3f secs.\n",
      nStatements, if (nStatements != 1L) "s" else "", total_secs
    ))
  }
  
  invisible(NULL)
}

# Helpers (simple, focused)
.int_flag <- function(x) as.integer(isTRUE(x))

executeOne <- function(conn, statement, ...) {
  output <- list(result = NULL, output = "", warnings = character(), messages = character())
  
  # Capture printed output
  output$output <- capture.output({
    tryCatch({
      # Capture messages and warnings
      withCallingHandlers(
        {
          output$result <- DBI::dbExecute(conn, statement, ...)
        },
        warning = function(w) {
          output$warnings <<- c(output$warnings, w$message)
          invokeRestart("muffleWarning")
        },
        message = function(m) {
          output$messages <<- c(output$messages, m$message)
          invokeRestart("muffleMessage")
        }
      )
    }, error = function(e) {
      # purrr::quietly doesn't usually catch errors (purrr::safely does)
      # but we stop here to mimic standard R behavior or you can return it in the list
      stop(e)
    })
  })
  
  return(output)
}


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
  if (camelCaseToSnakeCase) {
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

  return(tableName)
}

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
  
  # Guard clause: Check for NULL or length 0 (replaces rlang::is_empty)
  if (is.null(primaryFilterName) || length(eventFilters) == 0) {
    return(0L)
  }
  idx <- Position(function(x) identical(x$name, primaryFilterName), eventFilters)
  if (is.na(idx)) 0L else idx
}
