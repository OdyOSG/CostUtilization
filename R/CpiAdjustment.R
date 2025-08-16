# CpiAdjustment.R
# Functions for managing CPI data and creating database tables

#' Create CPI table in database
#'
#' @param connection DatabaseConnector connection object
#' @param cpiDatabaseSchema Schema where CPI table will be created
#' @param cpiTable Name of the CPI table
#' @param cpiData Data frame with year and cpi_value columns
#' @param dropExisting If TRUE, drop existing table before creating
#'
#' @return NULL (creates table in database)
#' @export
createCpiTable <- function(connection,
                          cpiDatabaseSchema,
                          cpiTable = "cpi_factors",
                          cpiData = NULL,
                          dropExisting = TRUE) {
  
  # Use default data if none provided
  if (is.null(cpiData)) {
    cpiData <- loadDefaultCpiData()
  }
  
  # Validate data
  if (!all(c("year", "cpi_value") %in% names(cpiData))) {
    cli::cli_abort("CPI data must contain 'year' and 'cpi_value' columns")
  }
  
  # Create full table name
  fullTableName <- paste(cpiDatabaseSchema, cpiTable, sep = ".")
  
  # Drop existing table if requested
  if (dropExisting) {
    sql <- SqlRender::render("DROP TABLE IF EXISTS @table;", table = fullTableName)
    sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
    DatabaseConnector::executeSql(connection, sql)
  }
  
  # Create and populate table
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = fullTableName,
    data = cpiData,
    dropTableIfExists = FALSE,
    createTable = TRUE,
    tempTable = FALSE,
    progressBar = FALSE
  )
  
  # Create index on year
  sql <- SqlRender::render(
    "CREATE INDEX idx_@table_year ON @table (year);",
    table = fullTableName
  )
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
  
  tryCatch({
    DatabaseConnector::executeSql(connection, sql)
  }, error = function(e) {
    cli::cli_warn("Could not create index on CPI table: {e$message}")
  })
  
  cli::cli_alert_success("Created CPI table: {fullTableName}")
  invisible(NULL)
}

#' Load default CPI data
#'
#' @return Data frame with year and cpi_value columns
#' @export
loadDefaultCpiData <- function() {
  cpiFile <- system.file("csv", "cpi_data.csv", package = "CostUtilization")
  
  if (!file.exists(cpiFile)) {
    cli::cli_abort("Default CPI data file not found")
  }
  
  cpiData <- readr::read_csv(cpiFile, show_col_types = FALSE)
  
  # Ensure proper column names
  names(cpiData) <- tolower(names(cpiData))
  
  # Validate data
  if (!all(c("year", "cpi_value") %in% names(cpiData))) {
    cli::cli_abort("CPI data must contain 'year' and 'cpi_value' columns")
  }
  
  # Sort by year
  cpiData <- dplyr::arrange(cpiData, year)
  
  return(cpiData)
}

#' Calculate CPI adjustment factors
#'
#' @param cpiData Data frame with year and cpi_value columns
#' @param targetYear Year to adjust all values to
#'
#' @return Data frame with additional adjustment_factor column
#' @export
calculateCpiFactors <- function(cpiData, targetYear) {
  
  # Check if target year exists
  if (!targetYear %in% cpiData$year) {
    cli::cli_abort("Target year {targetYear} not found in CPI data")
  }
  
  targetCpi <- cpiData$cpi_value[cpiData$year == targetYear]
  
  cpiData <- cpiData %>%
    dplyr::mutate(
      adjustment_factor = targetCpi / cpi_value
    )
  
  return(cpiData)
}

#' Update CPI data in database
#'
#' @param connection DatabaseConnector connection object
#' @param cpiDatabaseSchema Schema where CPI table exists
#' @param cpiTable Name of the CPI table
#' @param newCpiData Data frame with new CPI values
#' @param replaceAll If TRUE, replace all data; if FALSE, update/append
#'
#' @return NULL
#' @export
updateCpiData <- function(connection,
                         cpiDatabaseSchema,
                         cpiTable = "cpi_factors",
                         newCpiData,
                         replaceAll = FALSE) {
  
  fullTableName <- paste(cpiDatabaseSchema, cpiTable, sep = ".")
  
  if (replaceAll) {
    # Replace all data
    createCpiTable(
      connection = connection,
      cpiDatabaseSchema = cpiDatabaseSchema,
      cpiTable = cpiTable,
      cpiData = newCpiData,
      dropExisting = TRUE
    )
  } else {
    # Get existing data
    sql <- SqlRender::render("SELECT * FROM @table;", table = fullTableName)
    sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
    existingData <- DatabaseConnector::querySql(connection, sql)
    names(existingData) <- tolower(names(existingData))
    
    # Merge with new data (new data takes precedence)
    updatedData <- dplyr::bind_rows(
      dplyr::anti_join(existingData, newCpiData, by = "year"),
      newCpiData
    ) %>%
      dplyr::arrange(year)
    
    # Replace table with updated data
    createCpiTable(
      connection = connection,
      cpiDatabaseSchema = cpiDatabaseSchema,
      cpiTable = cpiTable,
      cpiData = updatedData,
      dropExisting = TRUE
    )
  }
  
  invisible(NULL)
}

#' Interpolate missing CPI values
#'
#' @param cpiData Data frame with year and cpi_value columns
#' @param startYear First year to include
#' @param endYear Last year to include
#'
#' @return Data frame with interpolated values for missing years
#' @export
interpolateCpiData <- function(cpiData, startYear = NULL, endYear = NULL) {
  
  if (is.null(startYear)) {
    startYear <- min(cpiData$year)
  }
  if (is.null(endYear)) {
    endYear <- max(cpiData$year)
  }
  
  # Create complete year sequence
  allYears <- data.frame(year = seq(startYear, endYear))
  
  # Merge with existing data
  completeData <- allYears %>%
    dplyr::left_join(cpiData, by = "year") %>%
    dplyr::arrange(year)
  
  # Interpolate missing values
  completeData$cpi_value <- zoo::na.approx(completeData$cpi_value, na.rm = FALSE)
  
  # Handle edge cases (extrapolate if needed)
  if (any(is.na(completeData$cpi_value))) {
    # Use first/last known values for extrapolation
    firstValue <- completeData$cpi_value[!is.na(completeData$cpi_value)][1]
    lastValue <- tail(completeData$cpi_value[!is.na(completeData$cpi_value)], 1)
    
    completeData$cpi_value[is.na(completeData$cpi_value) & completeData$year < min(cpiData$year)] <- firstValue
    completeData$cpi_value[is.na(completeData$cpi_value) & completeData$year > max(cpiData$year)] <- lastValue
  }
  
  return(completeData)
}

#' Validate CPI parameters
#'
#' @param connection DatabaseConnector connection object
#' @param cpiDatabaseSchema Schema where CPI table exists
#' @param cpiTable Name of the CPI table
#' @param targetYear Year to adjust values to
#'
#' @return TRUE if valid, error otherwise
#' @keywords internal
validateCpiParameters <- function(connection,
                                 cpiDatabaseSchema,
                                 cpiTable,
                                 targetYear) {
  
  # Check if table exists
  fullTableName <- paste(cpiDatabaseSchema, cpiTable, sep = ".")
  
  sql <- SqlRender::render(
    "SELECT COUNT(*) AS row_count FROM @table;",
    table = fullTableName
  )
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
  
  result <- tryCatch({
    DatabaseConnector::querySql(connection, sql)
  }, error = function(e) {
    cli::cli_abort("CPI table {fullTableName} does not exist or is not accessible")
  })
  
  if (result$ROW_COUNT[1] == 0) {
    cli::cli_abort("CPI table {fullTableName} is empty")
  }
  
  # Check if target year exists
  sql <- SqlRender::render(
    "SELECT COUNT(*) AS year_exists FROM @table WHERE year = @target_year;",
    table = fullTableName,
    target_year = targetYear
  )
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
  
  result <- DatabaseConnector::querySql(connection, sql)
  
  if (result$YEAR_EXISTS[1] == 0) {
    cli::cli_abort("Target year {targetYear} not found in CPI table")
  }
  
  return(TRUE)
}