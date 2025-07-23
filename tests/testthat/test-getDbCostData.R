# The setup-eunomia.R file is automatically run by testthat
# and creates connection, connectionDetails, cohort table, and cost table.

test_that("getDbCostData runs in aggregated mode", {
  settings <- createCostCovariateSettings(
    temporalStartDays = -365,
    temporalEndDays = 0
  )
  
  aggData <- getDbCostData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    costCovariateSettings = settings,
    aggregated = TRUE
  )
  
  expect_true(inherits(aggData, "CostCovariateData"))
  expect_true("aggregatedCovariates" %in% Andromeda::listAndromedaTables(aggData))
  
  agg_results <- aggData$aggregatedCovariates %>% dplyr::collect()
  expect_gt(nrow(agg_results), 0)
  expect_true("meanValue" %in% names(agg_results))
})

test_that("getDbCostData runs in person-level mode", {
  settings <- createCostCovariateSettings(
    temporalStartDays = -365,
    temporalEndDays = 0
  )
  
  personData <- getDbCostData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    costCovariateSettings = settings,
    aggregated = FALSE
  )
  
  expect_true(inherits(personData, "CostCovariateData"))
  expect_true("covariates" %in% Andromeda::listAndromedaTables(personData))
  
  person_results <- personData$covariates %>% dplyr::collect()
  expect_gt(nrow(person_results), 0)
  expect_true("covariateValue" %in% names(person_results))
})

test_that("getDbCostData produces correct aggregated values", {
  # In the setup script, we create a cohort of T2DM patients with index 2010-01-01
  # and add costs. We can test one specific calculation.
  
  # Simple setting: total cost in the 365 days prior to index (year 2009)
  settings <- createCostCovariateSettings(
    temporalStartDays = -365,
    temporalEndDays = -1,
    costByDomain = FALSE, # Simplify for testing
    utilization = FALSE   # Simplify for testing
  )
  
  aggData <- getDbCostData(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    costCovariateSettings = settings,
    aggregated = TRUE
  )
  
  # Manually calculate the expected average cost for this window
  # from the source tables created in setup-eunomia.R
  connection <- DatabaseConnector::connect(connectionDetails)
  sql <- "
    WITH person_costs AS (
      SELECT
        co.subject_id,
        SUM(c.cost) as total_cost
      FROM main.cohort co
      JOIN main.cost c ON co.subject_id = c.person_id
      WHERE co.cohort_definition_id = 1
        AND c.incurred_date >= DATEADD(day, -365, co.cohort_start_date)
        AND c.incurred_date <= DATEADD(day, -1, co.cohort_start_date)
      GROUP BY co.subject_id
    )
    SELECT AVG(total_cost) as expected_mean FROM person_costs;
  "
  expected <- DatabaseConnector::querySql(connection, sql)
  DatabaseConnector::disconnect(connection)
  
  # The covariateId for total cost in window 1 is 1001
  result <- aggData$aggregatedCovariates %>% 
    filter(covariateId == 1001) %>% 
    dplyr::pull(meanValue)
  
  expect_equal(result, expected$EXPECTED_MEAN, tolerance = 1e-4)
})

# Note: The original testthat.R and setup-eunomia.R files would also be included,
# with the package name corrected in testthat.R.