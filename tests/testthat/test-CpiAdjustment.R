# test-CpiAdjustment.R
# Tests for CPI adjustment functionality

library(testthat)
library(CostUtilization)

test_that("loadDefaultCpiData works correctly", {
  cpiData <- loadDefaultCpiData()
  
  expect_s3_class(cpiData, "data.frame")
  expect_true(all(c("year", "cpi_value") %in% names(cpiData)))
  expect_true(nrow(cpiData) > 0)
  expect_true(all(!is.na(cpiData$year)))
  expect_true(all(!is.na(cpiData$cpi_value)))
  expect_true(all(cpiData$cpi_value > 0))
})

test_that("calculateCpiFactors works correctly", {
  cpiData <- data.frame(
    year = c(2020, 2021, 2022, 2023),
    cpi_value = c(100, 105, 110, 115)
  )
  
  # Calculate factors for 2023
  factors <- calculateCpiFactors(cpiData, 2023)
  
  expect_equal(nrow(factors), 4)
  expect_true("adjustment_factor" %in% names(factors))
  expect_equal(factors$adjustment_factor[factors$year == 2023], 1.0)
  expect_equal(factors$adjustment_factor[factors$year == 2020], 1.15)
  
  # Error for missing year
  expect_error(
    calculateCpiFactors(cpiData, 2024),
    "Target year 2024 not found"
  )
})

test_that("interpolateCpiData works correctly", {
  cpiData <- data.frame(
    year = c(2020, 2022, 2024),
    cpi_value = c(100, 110, 120)
  )
  
  # Interpolate missing years
  interpolated <- interpolateCpiData(cpiData, 2020, 2024)
  
  expect_equal(nrow(interpolated), 5)
  expect_equal(interpolated$year, 2020:2024)
  expect_equal(interpolated$cpi_value[interpolated$year == 2021], 105)
  expect_equal(interpolated$cpi_value[interpolated$year == 2023], 115)
  
  # Test extrapolation
  extrapolated <- interpolateCpiData(cpiData, 2019, 2025)
  expect_equal(nrow(extrapolated), 7)
  expect_equal(extrapolated$cpi_value[extrapolated$year == 2019], 100)
  expect_equal(extrapolated$cpi_value[extrapolated$year == 2025], 120)
})

test_that("createCpiTable creates table correctly", {
  skip_if_not(dbms == "sqlite")
  
  connection <- DatabaseConnector::connect(
    dbms = "sqlite",
    server = ":memory:"
  )
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Create schema
  DatabaseConnector::executeSql(connection, "CREATE SCHEMA main;")
  
  # Test with default data
  createCpiTable(
    connection = connection,
    cpiDatabaseSchema = "main",
    cpiTable = "test_cpi"
  )
  
  # Check table exists
  sql <- "SELECT COUNT(*) as row_count FROM main.test_cpi;"
  result <- DatabaseConnector::querySql(connection, sql)
  expect_true(result$ROW_COUNT[1] > 0)
  
  # Test with custom data
  customData <- data.frame(
    year = 2020:2023,
    cpi_value = c(100, 105, 110, 115)
  )
  
  createCpiTable(
    connection = connection,
    cpiDatabaseSchema = "main",
    cpiTable = "test_cpi2",
    cpiData = customData
  )
  
  sql <- "SELECT * FROM main.test_cpi2 ORDER BY year;"
  result <- DatabaseConnector::querySql(connection, sql)
  expect_equal(nrow(result), 4)
  expect_equal(result$YEAR, 2020:2023)
})

test_that("updateCpiData works correctly", {
  skip_if_not(dbms == "sqlite")
  
  connection <- DatabaseConnector::connect(
    dbms = "sqlite",
    server = ":memory:"
  )
  on.exit(DatabaseConnector::disconnect(connection))
  
  DatabaseConnector::executeSql(connection, "CREATE SCHEMA main;")
  
  # Create initial table
  initialData <- data.frame(
    year = 2020:2022,
    cpi_value = c(100, 105, 110)
  )
  
  createCpiTable(
    connection = connection,
    cpiDatabaseSchema = "main",
    cpiTable = "test_cpi",
    cpiData = initialData
  )
  
  # Update with new data
  newData <- data.frame(
    year = c(2022, 2023),
    cpi_value = c(112, 118)
  )
  
  updateCpiData(
    connection = connection,
    cpiDatabaseSchema = "main",
    cpiTable = "test_cpi",
    newCpiData = newData,
    replaceAll = FALSE
  )
  
  # Check results
  sql <- "SELECT * FROM main.test_cpi ORDER BY year;"
  result <- DatabaseConnector::querySql(connection, sql)
  
  expect_equal(nrow(result), 4)
  expect_equal(result$CPI_VALUE[result$YEAR == 2022], 112) # Updated value
  expect_equal(result$CPI_VALUE[result$YEAR == 2023], 118) # New value
})

test_that("validateCpiParameters works correctly", {
  skip_if_not(dbms == "sqlite")
  
  connection <- DatabaseConnector::connect(
    dbms = "sqlite",
    server = ":memory:"
  )
  on.exit(DatabaseConnector::disconnect(connection))
  
  DatabaseConnector::executeSql(connection, "CREATE SCHEMA main;")
  
  # Test with missing table
  expect_error(
    validateCpiParameters(
      connection = connection,
      cpiDatabaseSchema = "main",
      cpiTable = "missing_table",
      targetYear = 2023
    ),
    "does not exist"
  )
  
  # Create table
  createCpiTable(
    connection = connection,
    cpiDatabaseSchema = "main",
    cpiTable = "test_cpi"
  )
  
  # Test with valid parameters
  expect_true(
    validateCpiParameters(
      connection = connection,
      cpiDatabaseSchema = "main",
      cpiTable = "test_cpi",
      targetYear = 2023
    )
  )
  
  # Test with missing year
  expect_error(
    validateCpiParameters(
      connection = connection,
      cpiDatabaseSchema = "main",
      cpiTable = "test_cpi",
      targetYear = 2050
    ),
    "not found in CPI table"
  )
})