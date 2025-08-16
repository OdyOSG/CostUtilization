#' CPI Adjustment Functions for Cost Standardization
#'
#' @description
#' Functions to adjust healthcare costs for inflation using Consumer Price Index (CPI) data.
#' Supports standardizing costs to a specific year for temporal comparisons.
#'
#' @keywords internal
#' @name cpi-adjustment

#' Load CPI Data
#'
#' @description
#' Loads CPI data from the package or a custom source
#'
#' @param cpiDataPath Path to custom CPI data CSV file (optional)
#' @param cpiType Type of CPI data: "medical" (default) or "all_items"
#'
#' @return A tibble with year and cpi columns
#' @export
#'
#' @examples
#' # Load default medical CPI data
#' cpiData <- loadCpiData()
#' 
#' # Load custom CPI data
#' \dontrun{
#' cpiData <- loadCpiData(cpiDataPath = "path/to/custom_cpi.csv")
#' }
loadCpiData <- function(cpiDataPath = NULL, cpiType = c("medical", "all_items")) {
  cpiType <- rlang::arg_match(cpiType)
  
  if (is.null(cpiDataPath)) {
    # Load default CPI data from package
    cpiDataPath <- system.file("csv", "cpi_data.csv", package = "CostUtilization")
    if (!file.exists(cpiDataPath)) {
      cli::cli_abort("Default CPI data file not found in package")
    }
  } else {
    # Validate custom path
    checkmate::assertFileExists(cpiDataPath, access = "r", extension = "csv")
  }
  
  # Read CPI data
  cpiData <- utils::read.csv(cpiDataPath, stringsAsFactors = FALSE) |>
    dplyr::as_tibble() |>
    dplyr::mutate(
      year = as.integer(.data$year),
      cpi = as.numeric(.data$cpi)
    )
  
  # Validate data structure
  validateCpiData(cpiData)
  
  return(cpiData)
}

#' Validate CPI Data
#'
#' @description
#' Validates the structure and content of CPI data
#'
#' @param cpiData A data frame with year and cpi columns
#'
#' @return Invisible TRUE if valid
#' @noRd
validateCpiData <- function(cpiData) {
  checkmate::assertDataFrame(cpiData, min.rows = 1)
  checkmate::assertNames(names(cpiData), must.include = c("year", "cpi"))
  checkmate::assertIntegerish(cpiData$year, lower = 1900, upper = 2100, any.missing = FALSE)
  checkmate::assertNumeric(cpiData$cpi, lower = 0, finite = TRUE, any.missing = FALSE)
  
  # Check for duplicate years
  if (any(duplicated(cpiData$year))) {
    cli::cli_abort("CPI data contains duplicate years")
  }
  
  # Check for reasonable CPI values
  if (any(cpiData$cpi <= 0)) {
    cli::cli_abort("CPI values must be positive")
  }
  
  invisible(TRUE)
}

#' Calculate CPI Adjustment Factor
#'
#' @description
#' Calculates the adjustment factor to convert costs from one year to another
#'
#' @param fromYear The year of the original cost
#' @param toYear The target year for standardization
#' @param cpiData A tibble with year and cpi columns
#'
#' @return Numeric adjustment factor
#' @export
#'
#' @examples
#' cpiData <- loadCpiData()
#' # Adjust from 2015 to 2023 dollars
#' factor <- calculateCpiAdjustmentFactor(2015, 2023, cpiData)
calculateCpiAdjustmentFactor <- function(fromYear, toYear, cpiData) {
  checkmate::assertIntegerish(fromYear, len = 1, lower = 1900, upper = 2100)
  checkmate::assertIntegerish(toYear, len = 1, lower = 1900, upper = 2100)
  validateCpiData(cpiData)
  
  # Get CPI values for both years
  fromCpi <- getCpiForYear(fromYear, cpiData)
  toCpi <- getCpiForYear(toYear, cpiData)
  
  # Calculate adjustment factor
  adjustmentFactor <- toCpi / fromCpi
  
  return(adjustmentFactor)
}

#' Get CPI for a Specific Year
#'
#' @description
#' Retrieves CPI value for a specific year, with interpolation if needed
#'
#' @param year The year to get CPI for
#' @param cpiData A tibble with year and cpi columns
#' @param interpolate Whether to interpolate for missing years
#'
#' @return Numeric CPI value
#' @noRd
getCpiForYear <- function(year, cpiData, interpolate = TRUE) {
  # Check if year exists in data
  if (year %in% cpiData$year) {
    return(cpiData$cpi[cpiData$year == year])
  }
  
  # Check if year is outside range
  minYear <- min(cpiData$year)
  maxYear <- max(cpiData$year)
  
  if (year < minYear || year > maxYear) {
    cli::cli_abort(
      "Year {year} is outside the range of available CPI data ({minYear}-{maxYear})"
    )
  }
  
  if (!interpolate) {
    cli::cli_abort("CPI data not available for year {year}")
  }
  
  # Linear interpolation
  lowerYear <- max(cpiData$year[cpiData$year < year])
  upperYear <- min(cpiData$year[cpiData$year > year])
  
  lowerCpi <- cpiData$cpi[cpiData$year == lowerYear]
  upperCpi <- cpiData$cpi[cpiData$year == upperYear]
  
  # Linear interpolation formula
  weight <- (year - lowerYear) / (upperYear - lowerYear)
  interpolatedCpi <- lowerCpi + weight * (upperCpi - lowerCpi)
  
  cli::cli_alert_info(
    "CPI for year {year} interpolated between {lowerYear} ({round(lowerCpi, 2)}) and {upperYear} ({round(upperCpi, 2)})"
  )
  
  return(interpolatedCpi)
}

#' Create CPI Adjustment Table
#'
#' @description
#' Creates a table of CPI adjustment factors for database operations
#'
#' @param connection DatabaseConnector connection object
#' @param cpiData A tibble with year and cpi columns
#' @param targetYear The target year for standardization
#' @param tableName Name for the CPI adjustment table
#' @param tempEmulationSchema Schema for temporary tables
#'
#' @return The name of the created table
#' @export
createCpiAdjustmentTable <- function(connection, cpiData, targetYear, 
                                     tableName = NULL, tempEmulationSchema = NULL) {
  checkmate::assertClass(connection, "DatabaseConnectorConnection")
  validateCpiData(cpiData)
  checkmate::assertIntegerish(targetYear, len = 1, lower = 1900, upper = 2100)
  
  if (is.null(tableName)) {
    tableName <- paste0("cpi_adj_", paste(sample(letters, 6), collapse = ""))
  }
  
  # Calculate adjustment factors for all years
  adjustmentData <- cpiData |>
    dplyr::mutate(
      target_year = targetYear,
      adjustment_factor = calculateCpiAdjustmentFactor(.data$year, targetYear, cpiData)
    ) |>
    dplyr::select(
      from_year = .data$year,
      target_year = .data$target_year,
      adjustment_factor = .data$adjustment_factor
    )
  
  # Create table
  fullTableName <- if (!is.null(tempEmulationSchema)) {
    paste(tempEmulationSchema, tableName, sep = ".")
  } else {
    tableName
  }
  
  # Create table structure
  createSql <- SqlRender::render(
    "CREATE TABLE @table (
      from_year INT,
      target_year INT,
      adjustment_factor FLOAT
    )",
    table = fullTableName
  )
  DatabaseConnector::executeSql(connection, createSql)
  
  # Insert data
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = tableName,
    databaseSchema = tempEmulationSchema,
    data = adjustmentData,
    createTable = FALSE,
    tempTable = is.null(tempEmulationSchema)
  )
  
  # Create index for performance
  indexSql <- SqlRender::render(
    "CREATE INDEX idx_@table_year ON @table (from_year)",
    table = fullTableName
  )
  tryCatch(
    DatabaseConnector::executeSql(connection, indexSql),
    error = function(e) {
      cli::cli_alert_warning("Could not create index on CPI adjustment table: {e$message}")
    }
  )
  
  return(tableName)
}

#' Apply CPI Adjustment in SQL
#'
#' @description
#' Generates SQL snippet to apply CPI adjustment to costs
#'
#' @param costColumn Name of the cost column
#' @param yearColumn Name of the year column
#' @param cpiTable Name of the CPI adjustment table
#' @param defaultFactor Default adjustment factor if year not found
#'
#' @return SQL expression for adjusted cost
#' @export
#'
#' @examples
#' # Generate SQL for CPI adjustment
#' sql <- applyCpiAdjustmentSql("total_cost", "YEAR(incurred_date)", "cpi_adjustment")
applyCpiAdjustmentSql <- function(costColumn, yearColumn, cpiTable, defaultFactor = 1.0) {
  checkmate::assertString(costColumn)
  checkmate::assertString(yearColumn)
  checkmate::assertString(cpiTable)
  checkmate::assertNumber(defaultFactor, lower = 0)
  
  sql <- sprintf(
    "COALESCE(%s * cpi.adjustment_factor, %s * %f)",
    costColumn, costColumn, defaultFactor
  )
  
  return(sql)
}

#' Get CPI Adjustment Settings
#'
#' @description
#' Creates a settings object for CPI adjustment
#'
#' @param enableCpiAdjustment Whether to enable CPI adjustment
#' @param targetYear Target year for standardization (NULL = current year)
#' @param cpiDataPath Path to custom CPI data (NULL = use default)
#' @param cpiType Type of CPI data to use
#'
#' @return List with CPI adjustment settings
#' @export
getCpiAdjustmentSettings <- function(enableCpiAdjustment = FALSE,
                                     targetYear = NULL,
                                     cpiDataPath = NULL,
                                     cpiType = "medical") {
  checkmate::assertFlag(enableCpiAdjustment)
  
  if (enableCpiAdjustment) {
    if (is.null(targetYear)) {
      targetYear <- as.integer(format(Sys.Date(), "%Y"))
    }
    checkmate::assertIntegerish(targetYear, len = 1, lower = 1900, upper = 2100)
  }
  
  settings <- list(
    enableCpiAdjustment = enableCpiAdjustment,
    targetYear = targetYear,
    cpiDataPath = cpiDataPath,
    cpiType = cpiType
  )
  
  class(settings) <- c("CpiAdjustmentSettings", "list")
  return(settings)
}

#' Print CPI Adjustment Settings
#'
#' @param x CPI adjustment settings object
#' @param ... Additional arguments (ignored)
#'
#' @export
print.CpiAdjustmentSettings <- function(x, ...) {
  cli::cli_h3("CPI Adjustment Settings")
  cli::cli_ul()
  cli::cli_li("Enabled: {x$enableCpiAdjustment}")
  if (x$enableCpiAdjustment) {
    cli::cli_li("Target year: {x$targetYear}")
    cli::cli_li("CPI type: {x$cpiType}")
    if (!is.null(x$cpiDataPath)) {
      cli::cli_li("Custom CPI data: {x$cpiDataPath}")
    }
  }
  cli::cli_end()
  invisible(x)
}

#' Summarize CPI Adjusted Costs
#'
#' @description
#' Provides summary statistics for CPI-adjusted costs
#'
#' @param costData A tibble with cost data
#' @param originalCostCol Name of original cost column
#' @param adjustedCostCol Name of adjusted cost column
#'
#' @return A tibble with summary statistics
#' @export
summarizeCpiAdjustedCosts <- function(costData, 
                                      originalCostCol = "total_cost",
                                      adjustedCostCol = "adjusted_cost") {
  checkmate::assertDataFrame(costData)
  checkmate::assertNames(names(costData), must.include = c(originalCostCol, adjustedCostCol))
  
  summary <- costData |>
    dplyr::summarise(
      n_records = dplyr::n(),
      original_total = sum(.data[[originalCostCol]], na.rm = TRUE),
      adjusted_total = sum(.data[[adjustedCostCol]], na.rm = TRUE),
      adjustment_ratio = .data$adjusted_total / .data$original_total,
      original_mean = mean(.data[[originalCostCol]], na.rm = TRUE),
      adjusted_mean = mean(.data[[adjustedCostCol]], na.rm = TRUE),
      original_median = stats::median(.data[[originalCostCol]], na.rm = TRUE),
      adjusted_median = stats::median(.data[[adjustedCostCol]], na.rm = TRUE)
    )
  
  return(summary)
}