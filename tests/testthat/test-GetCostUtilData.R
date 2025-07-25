### 4. Implement Unit Tests
library(testthat)

# This setup file is sourced automatically by testthat
source("setup-eunomia.R")

test_that("Settings object creation works", {
  settings <- createCostUtilSettings()
  expect_s3_class(settings, "costUtilSettings")
  expect_true(settings$calculateTotalCost)
})

test_that("Aggregated analysis runs without error", {
  settings <- createCostUtilSettings(
    timeWindows = list(c(-365, 0)),
    costDomains = c("Drug", "Visit")
  )

  costDataAgg <- getCostUtilData(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortIds = 1,
    costUtilSettings = settings,
    aggregated = TRUE
  )

  expect_s3_class(costDataAgg, "CovariateData")

  tableNames <- Andromeda::listAndromedaTables(costDataAgg)
  expect_true("covariatesContinuous" %in% tableNames)
  expect_false("covariates" %in% tableNames)

  # Check if reference tables are present
  expect_true("analysisRef" %in% tableNames)
  expect_true("covariateRef" %in% tableNames)

  # Check content
  agg_data <- dplyr::collect(costDataAgg$covariatesContinuous)
  expect_gt(nrow(agg_data), 0)

  # Check metadata
  meta <- attr(costDataAgg, "metaData")
  expect_true(meta$aggregated)
  expect_equal(meta$cohortIds, 1)
})

test_that("Person-level analysis runs without error", {
  settings <- createCostUtilSettings(
    timeWindows = list(c(-365, 0)),
    costDomains = c("Procedure")
  )

  costDataPerson <- getCostUtilData(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortIds = 1,
    costUtilSettings = settings,
    aggregated = FALSE
  )

  expect_s3_class(costDataPerson, "CovariateData")

  tableNames <- Andromeda::listAndromedaTables(costDataPerson)
  expect_true("covariates" %in% tableNames)
  expect_false("covariatesContinuous" %in% tableNames)

  # Check content
  person_data <- dplyr::collect(costDataPerson$covariates)
  expect_gt(nrow(person_data), 0)

  # Check metadata
  meta <- attr(costDataPerson, "metaData")
  expect_false(meta$aggregated)
})

test_that("Cost standardization runs", {
  settings <- createCostUtilSettings(
    timeWindows = list(c(-365, 0)),
    calculateTotalCost = TRUE,
    costStandardizationYear = 2010
  )

  costDataStd <- getCostUtilData(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortIds = 1,
    costUtilSettings = settings,
    aggregated = TRUE
  )
  expect_s3_class(costDataStd, "CovariateData")
  # A simple check to ensure it ran. More specific value checks would require known inputs.
  expect_gt(nrow(dplyr::collect(costDataStd$covariatesContinuous)), 0)
})

# Remember to disconnect after all tests
withr::defer(
  {
    DatabaseConnector::disconnect(connection)
  },
  testthat::teardown_env()
)
