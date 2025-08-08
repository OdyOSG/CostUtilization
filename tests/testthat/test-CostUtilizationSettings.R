library(testthat)

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
  expect_false(settings$useInCohortWindow)
  expect_equal(settings$timeWindows, list(c(-365, -1), c(0, 365)))
  expect_null(settings$costStandardizationYear)
  expect_equal(settings$currencyConceptId, 44818668)
})

test_that("createCostUtilSettings accepts 'in cohort' window only", {
  settings <- createCostUtilSettings(
    timeWindows = NULL,
    useInCohortWindow = TRUE
  )

  expect_s3_class(settings, "costUtilSettings")
  expect_null(settings$timeWindows)
  expect_true(settings$useInCohortWindow)
})

test_that("createCostUtilSettings allows both fixed and 'in cohort' windows", {
  settings <- createCostUtilSettings(
    timeWindows = list(c(-30, -1)),
    useInCohortWindow = TRUE
  )

  expect_s3_class(settings, "costUtilSettings")
  expect_length(settings$timeWindows, 1)
  expect_true(settings$useInCohortWindow)
})

test_that("createCostUtilSettings catches invalid inputs", {
  # No windowing strategy selected
  expect_error(
    createCostUtilSettings(timeWindows = NULL, useInCohortWindow = FALSE),
    "At least one windowing strategy must be used"
  )

  # Invalid useInCohortWindow flag (not a boolean)
  expect_error(createCostUtilSettings(useInCohortWindow = "TRUE"))

  # Invalid timeWindows (not a list of numerics)
  expect_error(createCostUtilSettings(timeWindows = "invalid"))

  # Invalid boolean flags
  expect_error(createCostUtilSettings(calculateTotalCost = "TRUE"))

  # Invalid CPI data frame
  expect_error(createCostUtilSettings(cpiData = data.frame(yr = 2020, val = 100)))
})


# (Existing tests for other parameters omitted for brevity)

test_that("createCostUtilSettings validates conceptIds parameter", {
  # Fails if not a numeric vector
  expect_error(createCostUtilSettings(conceptIds = "not a vector"))
  expect_error(createCostUtilSettings(conceptIds = data.frame(id = 123)))

  # Fails if vector is not unique
  expect_error(
    createCostUtilSettings(conceptIds = c(123, 456, 123)),
    "Must have unique values"
  )

  # Succeeds with a valid numeric vector
  settings <- createCostUtilSettings(conceptIds = c(123, 456))
  expect_s3_class(settings, "costUtilSettings")
  expect_equal(settings$conceptIds, c(123, 456))

  # Succeeds with NULL
  settings_null <- createCostUtilSettings(conceptIds = NULL)
  expect_null(settings_null$conceptIds)
})
