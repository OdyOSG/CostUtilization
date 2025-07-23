# Test core functionality of getDbCostData

# The setup-eunomia.R file is automatically run by testthat
# and creates connection, connectionDetails, and cohort table

test_that("getDbCostData runs in aggregated mode", {
  settings <- getDefaultCostSettings("simple",
                                     temporalStartDays = -365,
                                     temporalEndDays = -1)
  
  costData <- getDbCostData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    covariateSettings = settings,
    aggregated = TRUE
  )
  
  expect_s3_class(costData, "CostCovariateData")
  expect_true(is.data.frame(costData$covariates))
  expect_true("meanValue" %in% names(costData$covariates))
  expect_gt(nrow(costData$covariates), 0)
})

test_that("getDbCostData runs in person-level mode", {
  settings <- getDefaultCostSettings("simple",
                                     temporalStartDays = -365,
                                     temporalEndDays = -1)
  
  costData <- getDbCostData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    covariateSettings = settings,
    aggregated = FALSE
  )
  
  expect_s3_class(costData, "CostCovariateData")
  expect_true(is.data.frame(costData$covariates))
  expect_true("covariateValue" %in% names(costData$covariates))
  expect_gt(nrow(costData$covariates), 0)
})