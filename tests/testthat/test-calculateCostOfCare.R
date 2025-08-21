# Test calculateCostOfCare function

test_that("calculateCostOfCare works with basic parameters", {
  # Use the connection from setup-eunomia.R
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Create a simple test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort AS
    SELECT 
      1 AS cohort_definition_id,
      person_id AS subject_id,
      observation_period_start_date AS cohort_start_date,
      observation_period_end_date AS cohort_end_date
    FROM main.observation_period
    LIMIT 10
  ")
  
  # Create basic settings
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365
  )
  
  # Run analysis
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE
  )
  
  # Check results structure
  expect_type(results, "list")
  expect_named(results, c("results", "diagnostics"))
  expect_s3_class(results$results, "tbl_df")
  expect_s3_class(results$diagnostics, "tbl_df")
  
  # Check results columns
  expectedCols <- c("totalPersonDays", "totalPersonMonths", "totalPersonYears",
                    "metricType", "totalCost", "nPersonsWithCost")
  expect_true(all(expectedCols %in% names(results$results)))
  
  # Check diagnostics columns
  expect_true(all(c("stepName", "nPersons") %in% names(results$diagnostics)))
})

test_that("calculateCostOfCare handles connection vs connectionDetails correctly", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  
  # Create test cohort
  connection <- DatabaseConnector::connect(connectionDetails)
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort2 AS
    SELECT 
      1 AS cohort_definition_id,
      person_id AS subject_id,
      observation_period_start_date AS cohort_start_date,
      observation_period_end_date AS cohort_end_date
    FROM main.observation_period
    LIMIT 5
  ")
  DatabaseConnector::disconnect(connection)
  
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 30
  )
  
  # Test with connectionDetails
  results1 <- calculateCostOfCare(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort2",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE
  )
  
  expect_type(results1, "list")
  
  # Test with connection
  connection <- DatabaseConnector::connect(connectionDetails)
  results2 <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort2",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE
  )
  DatabaseConnector::disconnect(connection)
  
  expect_type(results2, "list")
})

test_that("calculateCostOfCare errors when both connection and connectionDetails provided", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 30
  )
  
  expect_error(
    calculateCostOfCare(
      connection = connection,
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "test_cohort",
      cohortId = 1,
      costOfCareSettings = settings
    ),
    "Need to provide either connectionDetails or connection, not both"
  )
})

test_that("calculateCostOfCare works with visit restrictions", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort_visit AS
    SELECT 
      1 AS cohort_definition_id,
      person_id AS subject_id,
      observation_period_start_date AS cohort_start_date,
      observation_period_end_date AS cohort_end_date
    FROM main.observation_period
    LIMIT 10
  ")
  
  # Create settings with visit restriction
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    restrictVisitConceptIds = c(9201, 9203)  # Inpatient and ER visits
  )
  
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_visit",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE
  )
  
  expect_type(results, "list")
  expect_true(settings$hasVisitRestriction)
})

test_that("calculateCostOfCare works with event filters", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort_events AS
    SELECT 
      1 AS cohort_definition_id,
      person_id AS subject_id,
      observation_period_start_date AS cohort_start_date,
      observation_period_end_date AS cohort_end_date
    FROM main.observation_period
    LIMIT 10
  ")
  
  # Define event filters
  eventFilters <- list(
    list(
      name = "Test Conditions",
      domain = "Condition",
      conceptIds = c(201820, 201826)
    ),
    list(
      name = "Test Drugs",
      domain = "Drug",
      conceptIds = c(1503297, 1502826)
    )
  )
  
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = -30,
    endOffsetDays = 365,
    eventFilters = eventFilters
  )
  
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_events",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE
  )
  
  expect_type(results, "list")
  expect_true(settings$hasEventFilters)
  expect_equal(settings$nFilters, 2)
})

test_that("calculateCostOfCare handles micro-costing", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
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
    SELECT 
      1 AS cohort_definition_id,
      person_id AS subject_id,
      observation_period_start_date AS cohort_start_date,
      observation_period_end_date AS cohort_end_date
    FROM main.observation_period
    LIMIT 10
  ")
  
  eventFilters <- list(
    list(
      name = "Primary Filter",
      domain = "Drug",
      conceptIds = c(1503297)
    )
  )
  
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 90,
    eventFilters = eventFilters,
    microCosting = TRUE,
    primaryEventFilterName = "Primary Filter"
  )
  
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_micro",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE
  )
  
  expect_type(results, "list")
  expect_true(settings$microCosting)
  
  # Clean up
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS main.visit_detail")
})

test_that("calculateCostOfCare handles different cost and currency concepts", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort_concepts AS
    SELECT 
      1 AS cohort_definition_id,
      person_id AS subject_id,
      observation_period_start_date AS cohort_start_date,
      observation_period_end_date AS cohort_end_date
    FROM main.observation_period
    LIMIT 5
  ")
  
  # Test with different cost concept
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    costConceptId = 31980L,  # Total cost instead of total charge
    currencyConceptId = 44818669L  # EUR instead of USD
  )
  
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_concepts",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE
  )
  
  expect_type(results, "list")
  expect_equal(settings$costConceptId, 31980L)
  expect_equal(settings$currencyConceptId, 44818669L)
})