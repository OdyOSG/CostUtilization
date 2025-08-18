# tests/testthat/test-edge-cases.R

test_that("calculateCostOfCare handles cohorts with no observation period overlap", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create cohort outside observation periods
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE future_cohort AS
    SELECT 
      1 AS cohort_definition_id,
      1 AS subject_id,
      '2050-01-01' AS cohort_start_date,
      '2050-12-31' AS cohort_end_date
  ")
  
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "future_cohort",
    cohortId = 1,
    costOfCareSettings = createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365
    ),
    verbose = FALSE
  )
  
  # Should return results but with zero person-time
  expect_type(results, "list")
  expect_true(results$results$total_person_days == 0 || is.na(results$results$total_person_days))
})

test_that("calculateCostOfCare handles very large time windows", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE large_window_cohort AS
    SELECT 
      1 AS cohort_definition_id,
      person_id AS subject_id,
      '2010-01-01' AS cohort_start_date,
      '2010-01-01' AS cohort_end_date
    FROM main.person
    LIMIT 10
  ")
  
  # Test with 10-year window
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "large_window_cohort",
    cohortId = 1,
    costOfCareSettings = createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = -3650,  # 10 years before
      endOffsetDays = 3650      # 10 years after
    ),
    verbose = FALSE
  )
  
  expect_type(results, "list")
  expect_true(nrow(results$results) > 0)
})

test_that("calculateCostOfCare handles cohorts with single-day windows", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE single_day_cohort AS
    SELECT 
      1 AS cohort_definition_id,
      person_id AS subject_id,
      visit_start_date AS cohort_start_date,
      visit_start_date AS cohort_end_date
    FROM main.visit_occurrence
    LIMIT 20
  ")
  
  # Test single day window
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "single_day_cohort",
    cohortId = 1,
    costOfCareSettings = createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 0  # Same day only
    ),
    verbose = FALSE
  )
  
  expect_type(results, "list")
  # Person-days should equal number of people (1 day each)
  if (results$results$n_persons_with_cost > 0) {
    expect_equal(results$results$total_person_days, 
                 results$diagnostics$n_persons[results$diagnostics$step_name == "02_valid_window"])
  }
})

test_that("calculateCostOfCare handles event filters with no matching events", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE no_match_cohort AS
    SELECT DISTINCT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      MIN(visit_start_date) AS cohort_start_date,
      MAX(visit_end_date) AS cohort_end_date
    FROM main.visit_occurrence
    GROUP BY person_id
    LIMIT 30
  ")
  
  # Define filters that won't match any events
  noMatchFilters <- list(
    list(
      name = "Non-existent Drugs",
      domain = "Drug",
      conceptIds = c(999999999, 888888888)  # Non-existent concept IDs
    )
  )
  
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "no_match_cohort",
    cohortId = 1,
    costOfCareSettings = createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = noMatchFilters
    ),
    verbose = FALSE
  )
  
  expect_type(results, "list")
  # Should have zero costs since no events match
  expect_equal(results$results$total_cost, 0)
  expect_equal(results$results$n_persons_with_cost, 0)
})

test_that("calculateCostOfCare handles duplicate person entries in cohort", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create cohort with duplicate persons (different dates)
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE duplicate_person_cohort AS
    SELECT 1 AS cohort_definition_id, 1 AS subject_id, '2010-01-01' AS cohort_start_date, '2010-06-30' AS cohort_end_date
    UNION ALL
    SELECT 1 AS cohort_definition_id, 1 AS subject_id, '2010-07-01' AS cohort_start_date, '2010-12-31' AS cohort_end_date
  ")
  
  # This should handle duplicates appropriately
  expect_no_error({
    results <- calculateCostOfCare(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "duplicate_person_cohort",
      cohortId = 1,
      costOfCareSettings = createCostOfCareSettings(
        anchorCol = "cohort_start_date",
        startOffsetDays = 0,
        endOffsetDays = 180
      ),
      verbose = FALSE
    )
  })
})

test_that("calculateCostOfCare handles NULL values in cohort dates", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create cohort with NULL end dates
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE null_date_cohort AS
    SELECT 
      1 AS cohort_definition_id,
      1 AS subject_id,
      '2010-01-01' AS cohort_start_date,
      NULL AS cohort_end_date
  ")
  
  # Should handle NULL dates appropriately
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "null_date_cohort",
    cohortId = 1,
    costOfCareSettings = createCostOfCareSettings(
      anchorCol = "cohort_start_date",  # Use start date, not end
      startOffsetDays = 0,
      endOffsetDays = 90
    ),
    verbose = FALSE
  )
  
  expect_type(results, "list")
})

test_that("createCostOfCareSettings handles extreme offset values", {
  # Test very large offsets
  expect_no_error({
    settings <- createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = -10000,
      endOffsetDays = 10000
    )
  })
  
  # Test zero-width window
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 100,
      endOffsetDays = 100
    ),
    "endOffsetDays.*must be greater than.*startOffsetDays"
  )
})

test_that("createCostOfCareSettings handles empty event filters list", {
  # Empty list should be treated as no filters
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    eventFilters = list()
  )
  
  expect_false(settings$hasEventFilters)
  expect_equal(settings$nFilters, 0)
})

test_that("transformCostToCdmV5dot5 handles empty cost table", {
  skip_if_not_installed("Eunomia")
  
  # Create connection with empty database
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = ":memory:"
  )
  
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create minimal schema
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE main.observation_period (
      person_id INTEGER,
      observation_period_start_date DATE,
      observation_period_end_date DATE
    )
  ")
  
  # This should handle empty data gracefully
  expect_warning(
    transformCostToCdmV5dot5(connectionDetails),
    "No observation periods found"
  )
})

test_that("calculateCostOfCare handles special characters in table names", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create cohort with special name (numbers and underscores)
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE cohort_2024_test AS
    SELECT 
      1 AS cohort_definition_id,
      1 AS subject_id,
      '2010-01-01' AS cohort_start_date,
      '2010-12-31' AS cohort_end_date
  ")
  
  expect_no_error({
    results <- calculateCostOfCare(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort_2024_test",
      cohortId = 1,
      costOfCareSettings = createCostOfCareSettings(
        anchorCol = "cohort_start_date",
        startOffsetDays = 0,
        endOffsetDays = 365
      ),
      verbose = FALSE
    )
  })
})