library(testthat)

# This setup file is sourced automatically by testthat
source("setup-eunomia.R")

test_that("getDefaultCpiTable returns a valid data frame", {
  cpi_table <- getDefaultCpiTable()
  
  expect_s3_class(cpi_table, "data.frame")
  expect_true(all(c("year", "cpi") %in% colnames(cpi_table)))
  expect_type(cpi_table$year, "integer")
  expect_type(cpi_table$cpi, "double")
  expect_gt(nrow(cpi_table), 0)
})

test_that("createCostUtilSettings creates object with default values", {
  settings <- createCostUtilSettings()
  
  expect_s3_class(settings, "costUtilSettings")
  expect_equal(settings$analysisName, "Cost and Utilization Analysis")
  expect_true(settings$calculateTotalCost)
  expect_true(settings$calculateLengthOfStay)
  expect_null(settings$costStandardizationYear)
  expect_equal(settings$currencyConceptId, 44818668)
})

test_that("createCostUtilSettings accepts custom valid values", {
  custom_windows <- list(c(-180, -1), c(0, 180))
  custom_cpi <- data.frame(year = 2020, cpi = 100)
  
  settings <- createCostUtilSettings(
    analysisName = "Custom Analysis",
    timeWindows = custom_windows,
    costDomains = "Drug",
    utilizationDomains = "Visit",
    calculateTotalCost = FALSE,
    calculateLengthOfStay = FALSE,
    costTypeConceptIds = c(123, 456),
    currencyConceptId = 999,
    costStandardizationYear = 2020,
    cpiData = custom_cpi
  )
  
  expect_equal(settings$analysisName, "Custom Analysis")
  expect_equal(settings$timeWindows, custom_windows)
  expect_equal(settings$costDomains, "Drug")
  expect_false(settings$calculateTotalCost)
  expect_false(settings$calculateLengthOfStay)
  expect_equal(settings$costTypeConceptIds, c(123, 456))
  expect_equal(settings$costStandardizationYear, 2020)
  expect_equal(settings$cpiData, custom_cpi)
})

test_that("createCostUtilSettings catches invalid inputs", {
  # Invalid analysisName (not a string)
  expect_error(createCostUtilSettings(analysisName = 123))
  
  # Invalid timeWindows (not a list of numerics)
  expect_error(createCostUtilSettings(timeWindows = "invalid"))
  expect_error(createCostUtilSettings(timeWindows = list(c("a", "b"))))
  
  # Invalid boolean flags
  expect_error(createCostUtilSettings(calculateTotalCost = "TRUE"))
  
  # Invalid integerish inputs
  expect_error(createCostUtilSettings(currencyConceptId = "usd"))
  expect_error(createCostUtilSettings(costTypeConceptIds = "abc"))
  
  # Invalid CPI data frame
  expect_error(createCostUtilSettings(cpiData = data.frame(yr = 2020, val = 100)))
})

