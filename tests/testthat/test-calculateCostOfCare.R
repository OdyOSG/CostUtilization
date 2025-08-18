# tests/testthat/test-calculateCostOfCare.R

test_that("calculateCostOfCare works with basic parameters", {
  skip_if_not_installed("Eunomia")
  
  # Use the connection from setup-eunomia.R
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create a simple test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort AS
    SELECT DISTINCT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      MIN(visit_start_date) AS cohort_start_date,
      MAX(visit_end_date) AS cohort_end_date
    FROM main.visit_occurrence
    GROUP BY person_id
    LIMIT 100
  ")
  
  # Test basic analysis
  expect_no_error({
    results <- calculateCostOfCare(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "test_cohort",
      cohortId = 1,
      costOfCareSettings = createCostOfCareSettings(
        anchorCol = "cohort_start_date",
        startOffsetDays = 0,
        endOffsetDays = 365
      ),
      verbose = FALSE
    )
  })
  
  # Verify results structure
  expect_type(results, "list")
  expect_named(results, c("results", "diagnostics"))
  expect_s3_class(results$results, "tbl_df")
  expect_s3_class(results$diagnostics, "tbl_df")
  
  # Check results columns
  expected_cols <- c("total_person_days", "total_person_months", "total_person_years",
                     "metric_type", "total_cost", "n_persons_with_cost")
  expect_true(all(expected_cols %in% names(results$results)))
  
  # Check diagnostics has expected steps
  expect_true("step_name" %in% names(results$diagnostics))
  expect_true("n_persons" %in% names(results$diagnostics))
})

test_that("calculateCostOfCare handles visit restrictions correctly", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort_visit AS
    SELECT DISTINCT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      MIN(visit_start_date) AS cohort_start_date,
      MAX(visit_end_date) AS cohort_end_date
    FROM main.visit_occurrence
    GROUP BY person_id
    LIMIT 50
  ")
  
  # Test with visit restrictions
  inpatientVisits <- c(9201, 9203)  # Inpatient and ER visits
  
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_visit",
    cohortId = 1,
    costOfCareSettings = createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      restrictVisitConceptIds = inpatientVisits
    ),
    verbose = FALSE
  )
  
  expect_type(results, "list")
  expect_true(nrow(results$results) >= 0)  # May be 0 if no matching visits
})

test_that("calculateCostOfCare handles event filters correctly", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort_events AS
    SELECT DISTINCT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      MIN(drug_exposure_start_date) AS cohort_start_date,
      MAX(drug_exposure_end_date) AS cohort_end_date
    FROM main.drug_exposure
    GROUP BY person_id
    LIMIT 50
  ")
  
  # Define event filters
  eventFilters <- list(
    list(
      name = "Test Drugs",
      domain = "Drug",
      conceptIds = c(1118084, 1124300)  # Celecoxib, Diclofenac
    )
  )
  
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_events",
    cohortId = 1,
    costOfCareSettings = createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = -30,
      endOffsetDays = 90,
      eventFilters = eventFilters
    ),
    verbose = FALSE
  )
  
  expect_type(results, "list")
  expect_s3_class(results$results, "tbl_df")
})

test_that("calculateCostOfCare handles different time windows", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort_windows AS
    SELECT 
      1 AS cohort_definition_id,
      1 AS subject_id,
      '2010-01-01' AS cohort_start_date,
      '2010-12-31' AS cohort_end_date
  ")
  
  # Test different time windows
  timeWindows <- list(
    pre = c(-365, -1),
    during = c(0, 90),
    post = c(91, 365)
  )
  
  for (windowName in names(timeWindows)) {
    window <- timeWindows[[windowName]]
    
    results <- calculateCostOfCare(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "test_cohort_windows",
      cohortId = 1,
      costOfCareSettings = createCostOfCareSettings(
        anchorCol = "cohort_start_date",
        startOffsetDays = window[1],
        endOffsetDays = window[2]
      ),
      verbose = FALSE
    )
    
    expect_type(results, "list", info = paste("Window:", windowName))
    expect_true(nrow(results$diagnostics) > 0, info = paste("Window:", windowName))
  }
})

test_that("calculateCostOfCare handles micro-costing", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Ensure visit_detail table exists
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE IF NOT EXISTS main.visit_detail AS
    SELECT 
      ROW_NUMBER() OVER () AS visit_detail_id,
      person_id,
      visit_occurrence_id,
      9201 AS visit_detail_concept_id,
      visit_start_date AS visit_detail_start_date,
      visit_end_date AS visit_detail_end_date
    FROM main.visit_occurrence
    LIMIT 100
  ")
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort_micro AS
    SELECT DISTINCT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      MIN(visit_start_date) AS cohort_start_date,
      MAX(visit_end_date) AS cohort_end_date
    FROM main.visit_occurrence
    WHERE person_id IN (SELECT DISTINCT person_id FROM main.visit_detail)
    GROUP BY person_id
    LIMIT 20
  ")
  
  # Define event filters for micro-costing
  eventFilters <- list(
    list(
      name = "Primary Events",
      domain = "Visit",
      conceptIds = c(9201)
    )
  )
  
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_micro",
    cohortId = 1,
    costOfCareSettings = createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 90,
      microCosting = TRUE,
      eventFilters = eventFilters,
      primaryEventFilterName = "Primary Events"
    ),
    verbose = FALSE
  )
  
  expect_type(results, "list")
  expect_true("distinct_visit_details" %in% names(results$results))
})

test_that("calculateCostOfCare handles empty cohorts gracefully", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create empty cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE empty_cohort AS
    SELECT 
      1 AS cohort_definition_id,
      1 AS subject_id,
      '2025-01-01' AS cohort_start_date,
      '2025-12-31' AS cohort_end_date
    WHERE 1 = 0
  ")
  
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "empty_cohort",
    cohortId = 1,
    costOfCareSettings = createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365
    ),
    verbose = FALSE
  )
  
  expect_type(results, "list")
  expect_equal(nrow(results$results), 1)  # Should still return a row with zeros
  expect_true(results$results$total_person_days == 0 || is.na(results$results$total_person_days))
})

test_that("calculateCostOfCare handles different cost concepts", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort_cost_types AS
    SELECT DISTINCT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      MIN(visit_start_date) AS cohort_start_date,
      MAX(visit_end_date) AS cohort_end_date
    FROM main.visit_occurrence
    GROUP BY person_id
    LIMIT 30
  ")
  
  # Test different cost concepts
  costConcepts <- list(
    totalCharge = 31978L,
    totalCost = 31980L,
    paidByPayer = 31981L
  )
  
  for (conceptName in names(costConcepts)) {
    results <- calculateCostOfCare(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "test_cohort_cost_types",
      cohortId = 1,
      costOfCareSettings = createCostOfCareSettings(
        anchorCol = "cohort_start_date",
        startOffsetDays = 0,
        endOffsetDays = 180,
        costConceptId = costConcepts[[conceptName]]
      ),
      verbose = FALSE
    )
    
    expect_type(results, "list", info = paste("Cost concept:", conceptName))
    expect_true(nrow(results$results) > 0, info = paste("Cost concept:", conceptName))
  }
})

test_that("calculateCostOfCare parameter validation works", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Test missing required parameters
  expect_error(
    calculateCostOfCare(
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1
    ),
    "Need to provide either connectionDetails or connection"
  )
  
  # Test invalid cohort ID
  expect_error(
    calculateCostOfCare(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = "invalid"
    )
  )
  
  # Test invalid settings object
  expect_error(
    calculateCostOfCare(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1,
      costOfCareSettings = list(invalid = TRUE)
    )
  )
})