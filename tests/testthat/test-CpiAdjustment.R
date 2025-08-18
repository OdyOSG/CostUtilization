# Test suite for CPI adjustment functionality

library(testthat)
library(dplyr)

test_that("loadCpiData loads default CPI data correctly", {
  cpiData <- loadCpiData()
  
  expect_s3_class(cpiData, "tbl_df")
  expect_true("year" %in% names(cpiData))
  expect_true("cpi" %in% names(cpiData))
  expect_true(nrow(cpiData) > 0)
  expect_true(all(cpiData$cpi > 0))
})

test_that("loadCpiData validates custom CPI data", {
  # Create temporary CSV with invalid data
  tempFile <- tempfile(fileext = ".csv")
  write.csv(
    data.frame(year = c(2020, 2020), cpi = c(100, 110)), # Duplicate years
    tempFile,
    row.names = FALSE
  )
  
  expect_error(
    loadCpiData(cpiDataPath = tempFile),
    "duplicate years"
  )
  
  unlink(tempFile)
})

test_that("calculateCpiAdjustmentFactor calculates correctly", {
  # Create test CPI data
  cpiData <- tibble(
    year = c(2015, 2020, 2023),
    cpi = c(100, 120, 150)
  )
  
  # Test exact years
  factor2015to2023 <- calculateCpiAdjustmentFactor(2015, 2023, cpiData)
  expect_equal(factor2015to2023, 1.5) # 150/100
  
  factor2020to2023 <- calculateCpiAdjustmentFactor(2020, 2023, cpiData)
  expect_equal(factor2020to2023, 1.25) # 150/120
  
  # Test same year
  factorSame <- calculateCpiAdjustmentFactor(2020, 2020, cpiData)
  expect_equal(factorSame, 1.0)
})

test_that("getCpiForYear handles interpolation", {
  cpiData <- tibble(
    year = c(2015, 2020, 2023),
    cpi = c(100, 120, 150)
  )
  
  # Test interpolation
  cpi2018 <- getCpiForYear(2018, cpiData, interpolate = TRUE)
  expect_true(cpi2018 > 100 && cpi2018 < 120)
  expect_equal(cpi2018, 112) # Linear interpolation: 100 + (120-100) * (2018-2015)/(2020-2015)
  
  # Test exact year
  cpi2020 <- getCpiForYear(2020, cpiData, interpolate = TRUE)
  expect_equal(cpi2020, 120)
  
  # Test out of range
  expect_error(
    getCpiForYear(2010, cpiData),
    "outside the range"
  )
})

test_that("createCpiAdjustmentTable creates table in database", {
  # Create mock connection
  connection <- DatabaseConnector::connect(
    dbms = "sqlite",
    server = ":memory:"
  )
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Create test CPI data
  cpiData <- tibble(
    year = c(2015, 2020, 2023),
    cpi = c(100, 120, 150)
  )
  
  # Create CPI adjustment table
  tableName <- createCpiAdjustmentTable(
    connection = connection,
    cpiData = cpiData,
    targetYear = 2023
  )
  
  expect_type(tableName, "character")
  
  # Check table exists and has correct data
  result <- DatabaseConnector::querySql(
    connection,
    sprintf("SELECT * FROM %s ORDER BY from_year", tableName)
  )
  
  expect_equal(nrow(result), 3)
  expect_equal(result$FROM_YEAR, c(2015, 2020, 2023))
  expect_equal(result$TARGET_YEAR, c(2023, 2023, 2023))
  expect_equal(result$ADJUSTMENT_FACTOR, c(1.5, 1.25, 1.0))
})

test_that("applyCpiAdjustmentSql generates correct SQL", {
  sql <- applyCpiAdjustmentSql(
    costColumn = "total_cost",
    yearColumn = "YEAR(incurred_date)",
    cpiTable = "cpi_adjustment",
    defaultFactor = 1.0
  )
  
  expect_type(sql, "character")
  expect_true(grepl("COALESCE", sql))
  expect_true(grepl("total_cost", sql))
  expect_true(grepl("adjustment_factor", sql))
})

test_that("getCpiAdjustmentSettings creates valid settings object", {
  # Test disabled CPI adjustment
  settingsDisabled <- getCpiAdjustmentSettings(enableCpiAdjustment = FALSE)
  expect_s3_class(settingsDisabled, "CpiAdjustmentSettings")
  expect_false(settingsDisabled$enableCpiAdjustment)
  
  # Test enabled with defaults
  settingsEnabled <- getCpiAdjustmentSettings(
    enableCpiAdjustment = TRUE,
    targetYear = 2023
  )
  expect_true(settingsEnabled$enableCpiAdjustment)
  expect_equal(settingsEnabled$targetYear, 2023)
  expect_equal(settingsEnabled$cpiType, "medical")
  
  # Test with custom path
  settingsCustom <- getCpiAdjustmentSettings(
    enableCpiAdjustment = TRUE,
    targetYear = 2022,
    cpiDataPath = "custom/path.csv",
    cpiType = "all_items"
  )
  expect_equal(settingsCustom$cpiDataPath, "custom/path.csv")
  expect_equal(settingsCustom$cpiType, "all_items")
})

test_that("summarizeCpiAdjustedCosts calculates summary statistics", {
  # Create test data
  costData <- tibble(
    person_id = c(1, 1, 2, 2, 3),
    total_cost = c(100, 200, 150, 250, 300),
    adjusted_cost = c(120, 240, 180, 300, 360)
  )
  
  summary <- summarizeCpiAdjustedCosts(costData)
  
  expect_s3_class(summary, "tbl_df")
  expect_equal(summary$n_records, 5)
  expect_equal(summary$original_total, 1000)
  expect_equal(summary$adjusted_total, 1200)
  expect_equal(summary$adjustment_ratio, 1.2)
  expect_equal(summary$original_mean, 200)
  expect_equal(summary$adjusted_mean, 240)
})

test_that("CPI adjustment integrates with calculateCostOfCare", {
  skip_if_not_installed("Eunomia")
  
  # Get connection to test database
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Inject cost data
  injectCostData(connection)
  
  # Transform to v5.4 format
  transformCostToCdmV5dot4(connectionDetails)
  
  # Create a simple cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE main.test_cohort AS
    SELECT 
      1 as cohort_definition_id,
      person_id as subject_id,
      observation_period_start_date as cohort_start_date,
      observation_period_end_date as cohort_end_date
    FROM main.observation_period
    WHERE person_id IN (SELECT person_id FROM main.cost LIMIT 10)
  ")
  
  # Run analysis without CPI adjustment
  resultsNoCpi <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort",
    cohortId = 1,
    cpiAdjustment = FALSE,
    returnFormat = "list"
  )
  
  # Run analysis with CPI adjustment
  resultsWithCpi <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort",
    cohortId = 1,
    cpiAdjustment = TRUE,
    cpiTargetYear = 2023,
    returnFormat = "list"
  )
  
  # Check results structure
  expect_type(resultsNoCpi, "list")
  expect_type(resultsWithCpi, "list")
  
  # Check CPI adjustment metadata
  expect_false(resultsNoCpi$metadata$cpiAdjustment)
  expect_true(resultsWithCpi$metadata$cpiAdjustment)
  expect_equal(resultsWithCpi$metadata$cpiTargetYear, 2023)
  
  # Check that adjusted costs are different (assuming historical data)
  if (nrow(resultsWithCpi$results) > 0) {
    expect_true("adjusted_cost" %in% names(resultsWithCpi$results))
    expect_true("cpi_adjusted" %in% names(resultsWithCpi$results))
    expect_equal(resultsWithCpi$results$cpi_target_year[1], 2023)
  }
  
  # Clean up
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS main.test_cohort")
})