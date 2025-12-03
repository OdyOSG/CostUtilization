#' Database Connection Helpers
#'
#' @description
#' Helper functions for database operations using DatabaseConnector instead of DBI.
#' These functions provide a consistent interface for database operations.

#' Insert Table Using DatabaseConnector
#'
#' @description
#' Inserts a data.frame into a database table using DatabaseConnector instead of DBI.
#' Provides better compatibility with OHDSI ecosystem.
#'
#' @param connection A DatabaseConnector connection object.
#' @param tableName Target table name (character).
#' @param data A data.frame or tibble to insert.
#' @param tempTable Logical, create a temporary table if supported.
#' @param tempEmulationSchema Optional schema name to emulate temporary tables.
#' @param camelCaseToSnakeCase Logical, convert column names before insert.
#'
#' @return Invisibly TRUE on success.
#' @noRd
insertTableDC <- function(connection,
                          tableName,
                          data,
                          tempTable = FALSE,
                          tempEmulationSchema = NULL,
                          camelCaseToSnakeCase = FALSE) {
  
  # Validate inputs
  if (!inherits(connection, "DatabaseConnectorConnection")) {
    stop("Connection must be a DatabaseConnector connection object")
  }
  
  if (!is.data.frame(data)) {
    stop("Data must be a data.frame or tibble")
  }
  
  if (nrow(data) == 0) {
    logMessage(sprintf("No data to insert into table %s", tableName), TRUE, "DEBUG")
    return(invisible(TRUE))
  }
  
  # Convert column names if requested
  if (isTRUE(camelCaseToSnakeCase)) {
    names(data) <- SqlRender::camelCaseToSnakeCase(names(data))
  }
  
  # Determine full table name
  fullTableName <- if (!is.null(tempEmulationSchema) && nzchar(tempEmulationSchema)) {
    paste(tempEmulationSchema, tableName, sep = ".")
  } else {
    tableName
  }
  
  # Use DatabaseConnector::insertTable
  tryCatch({
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = fullTableName,
      data = data,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = tempTable,
      camelCaseToSnakeCase = FALSE  # We handle this above
    )
  }, error = function(e) {
    stop(sprintf("Failed to insert data into table %s: %s", fullTableName, conditionMessage(e)))
  })
  
  invisible(TRUE)
}

#' Execute SQL Query Using DatabaseConnector
#'
#' @description
#' Executes a SQL query and returns results using DatabaseConnector.
#'
#' @param connection A DatabaseConnector connection object.
#' @param sql SQL query string.
#' @param snakeCaseToCamelCase Logical, convert column names to camelCase.
#'
#' @return A data.frame with query results.
#' @noRd
querySqlDC <- function(connection, sql, snakeCaseToCamelCase = TRUE) {
  
  if (!inherits(connection, "DatabaseConnectorConnection")) {
    stop("Connection must be a DatabaseConnector connection object")
  }
  
  tryCatch({
    result <- DatabaseConnector::querySql(connection, sql)
    
    # Convert column names if requested
    if (isTRUE(snakeCaseToCamelCase)) {
      names(result) <- SqlRender::snakeCaseToCamelCase(names(result))
    }
    
    return(result)
    
  }, error = function(e) {
    stop(sprintf("Failed to execute query: %s", conditionMessage(e)))
  })
}

#' Clean Up Temporary Tables Using DatabaseConnector
#'
#' @description
#' Drops temporary tables using DatabaseConnector with proper error handling.
#'
#' @param connection A DatabaseConnector connection object.
#' @param schema Optional schema name.
#' @param ... Table names to drop.
#'
#' @return NULL (invisibly)
#' @noRd
cleanupTempTablesDC <- function(connection, schema = NULL, ...) {
  
  if (!inherits(connection, "DatabaseConnectorConnection")) {
    return(invisible(NULL))
  }
  
  tables <- list(...)
  if (length(tables) == 0) return(invisible(NULL))
  
  # Filter out NULL or empty table names
  tables <- Filter(function(x) !is.null(x) && nzchar(x), tables)
  if (length(tables) == 0) return(invisible(NULL))
  
  for (tableName in tables) {
    tryCatch({
      # Build full table name
      fullTableName <- if (!is.null(schema) && nzchar(schema)) {
        paste(schema, tableName, sep = ".")
      } else {
        tableName
      }
      
      # Try to drop the table
      sql <- sprintf("DROP TABLE IF EXISTS %s;", fullTableName)
      DatabaseConnector::executeSql(connection, sql)
      
    }, error = function(e) {
      # Silently ignore errors during cleanup
      invisible(NULL)
    })
  }
  
  invisible(NULL)
}

#' Get Database Management System
#'
#' @description
#' Gets the DBMS type from a DatabaseConnector connection.
#'
#' @param connection A DatabaseConnector connection object.
#'
#' @return Character string with DBMS type.
#' @noRd
getDbmsDC <- function(connection) {
  
  if (!inherits(connection, "DatabaseConnectorConnection")) {
    stop("Connection must be a DatabaseConnector connection object")
  }
  
  tryCatch({
    return(DatabaseConnector::dbms(connection))
  }, error = function(e) {
    stop(sprintf("Failed to get DBMS type: %s", conditionMessage(e)))
  })
}

#' Check Connection Validity
#'
#' @description
#' Checks if a DatabaseConnector connection is valid and active.
#'
#' @param connection A DatabaseConnector connection object.
#'
#' @return Logical indicating if connection is valid.
#' @noRd
isValidConnectionDC <- function(connection) {
  
  if (!inherits(connection, "DatabaseConnectorConnection")) {
    return(FALSE)
  }
  
  tryCatch({
    # Try a simple query to test connection
    DatabaseConnector::querySql(connection, "SELECT 1 as test_connection;")
    return(TRUE)
  }, error = function(e) {
    return(FALSE)
  })
}