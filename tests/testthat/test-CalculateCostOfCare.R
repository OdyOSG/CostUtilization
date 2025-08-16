# Test suite for calculateCostOfCare function

library(testthat)
library(DatabaseConnector)
library(dplyr)
library(tibble)

# Helper function to create a mock database connection
createMockConnection <- function() {
  # Create an in-memory SQLite database for testing
  connection <- DatabaseConnector::connect(
    dbms = "sqlite",
    server = ":memory:"
  )
  return(connection)
}

# Helper function to create test CDM tables
createTestCdmTables <- function(connection) {
  # Create minimal CDM schema for testing

  # Person table
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE person (
      person_id INTEGER PRIMARY KEY,
      gender_concept_id INTEGER,
      year_of_birth INTEGER,
      month_of_birth INTEGER,
      day_of_birth INTEGER
    )
  ")

  # Observation period table
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE observation_period (
      observation_period_id INTEGER PRIMARY KEY,
      person_id INTEGER,
      observation_period_start_date DATE,
      observation_period_end_date DATE
    )
  ")

  # Visit occurrence table
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE visit_occurrence (
      visit_occurrence_id INTEGER PRIMARY KEY,
      person_id INTEGER,
      visit_concept_id INTEGER,
      visit_start_date DATE,
      visit_end_date DATE
    )
  ")

  # Visit detail table
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE visit_detail (
      visit_detail_id INTEGER PRIMARY KEY,
      person_id INTEGER,
      visit_occurrence_id INTEGER,
      visit_detail_concept_id INTEGER,
      visit_detail_start_date DATE,
      visit_detail_end_date DATE
    )
  ")

  # Cost table
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE cost (
      cost_id INTEGER PRIMARY KEY,
      cost_event_id INTEGER,
      cost_domain_id VARCHAR(20),
      cost_type_concept_id INTEGER,
      currency_concept_id INTEGER,
      total_charge NUMERIC,
      total_cost NUMERIC,
      total_paid NUMERIC,
      paid_by_payer NUMERIC,
      paid_by_patient NUMERIC,
      paid_patient_copay NUMERIC,
      paid_patient_coinsurance NUMERIC,
      paid_patient_deductible NUMERIC,
      paid_by_primary NUMERIC,
      paid_ingredient_cost NUMERIC,
      paid_dispensing_fee NUMERIC,
      payer_plan_period_id INTEGER,
      amount_allowed NUMERIC,
      revenue_code_concept_id INTEGER,
      revenue_code_source_value VARCHAR(50),
      drg_concept_id INTEGER,
      drg_source_value VARCHAR(3),
      person_id INTEGER,
      cost_concept_id INTEGER
    )
  ")

  # Drug exposure table (for event filters)
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE drug_exposure (
      drug_exposure_id INTEGER PRIMARY KEY,
      person_id INTEGER,
      drug_concept_id INTEGER,
      drug_exposure_start_date DATE,
      drug_exposure_end_date DATE,
      visit_occurrence_id INTEGER,
      visit_detail_id INTEGER
    )
  ")

  # Procedure occurrence table (for event filters)
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE procedure_occurrence (
      procedure_occurrence_id INTEGER PRIMARY KEY,
      person_id INTEGER,
      procedure_concept_id INTEGER,
      procedure_date DATE,
      visit_occurrence_id INTEGER,
      visit_detail_id INTEGER
    )
  ")

  # Condition occurrence table (for event filters)
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE condition_occurrence (
      condition_occurrence_id INTEGER PRIMARY KEY,
      person_id INTEGER,
      condition_concept_id INTEGER,
      condition_start_date DATE,
      condition_end_date DATE,
      visit_occurrence_id INTEGER
    )
  ")
}

# Helper function to create cohort table
createCohortTable <- function(connection, cohortSchema, cohortTable) {
  sql <- glue::glue("
    CREATE TABLE {cohortSchema}.{cohortTable} (
      cohort_definition_id INTEGER,
      subject_id INTEGER,
      cohort_start_date DATE,
      cohort_end_date DATE
    )
  ")
  DatabaseConnector::executeSql(connection, sql)
}

# Helper function to insert test data
insertTestData <- function(connection, cohortSchema, cohortTable) {
  # Insert persons
  DatabaseConnector::executeSql(connection, "
    INSERT INTO person (person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth)
    VALUES
      (1, 8507, 1980, 1, 1),
      (2, 8532, 1975, 6, 15),
      (3, 8507, 1990, 12, 25)
  ")

  # Insert observation periods
  DatabaseConnector::executeSql(connection, "
    INSERT INTO observation_period (observation_period_id, person_id, observation_period_start_date, observation_period_end_date)
    VALUES
      (1, 1, '2020-01-01', '2023-12-31'),
      (2, 2, '2020-01-01', '2023-12-31'),
      (3, 3, '2020-01-01', '2023-12-31')
  ")

  # Insert cohort members
  sql <- glue::glue("
    INSERT INTO {cohortSchema}.{cohortTable} (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
    VALUES
      (1, 1, '2021-01-01', '2021-12-31'),
      (1, 2, '2021-02-01', '2021-12-31'),
      (1, 3, '2021-03-01', '2021-12-31')
  ")
  DatabaseConnector::executeSql(connection, sql)

  # Insert visits
  DatabaseConnector::executeSql(connection, "
    INSERT INTO visit_occurrence (visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_end_date)
    VALUES
      (1, 1, 9201, '2021-01-15', '2021-01-15'),
      (2, 1, 9201, '2021-02-20', '2021-02-20'),
      (3, 2, 9202, '2021-02-15', '2021-02-17'),
      (4, 2, 9201, '2021-03-10', '2021-03-10'),
      (5, 3, 9201, '2021-03-20', '2021-03-20')
  ")

  # Insert costs
  DatabaseConnector::executeSql(connection, "
    INSERT INTO cost (cost_id, cost_event_id, person_id, cost_concept_id, currency_concept_id, total_charge)
    VALUES
      (1, 1, 1, 31978, 44818668, 150.00),
      (2, 2, 1, 31978, 44818668, 200.00),
      (3, 3, 2, 31978, 44818668, 1500.00),
      (4, 4, 2, 31978, 44818668, 100.00),
      (5, 5, 3, 31978, 44818668, 250.00)
  ")
}

# Test cases
test_that("calculateCostOfCare works with basic parameters", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  insertTestData(connection, "main", "cohort")

  # Run the function
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 90,
    returnFormat = "tibble"
  )

  # Basic checks
  expect_s3_class(result, "tbl_df")
  expect_true(nrow(result) > 0)
  expect_true("total_cost" %in% names(result))
  expect_true("cost_pppm" %in% names(result))
  expect_true("visits_per_1000_py" %in% names(result))
})

test_that("calculateCostOfCare returns list format when requested", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  insertTestData(connection, "main", "cohort")

  # Run the function with list format
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    returnFormat = "list"
  )

  # Check list structure
  expect_type(result, "list")
  expect_true("results" %in% names(result))
  expect_true("diagnostics" %in% names(result))
  expect_s3_class(result$results, "tbl_df")
  expect_s3_class(result$diagnostics, "tbl_df")
})

test_that("calculateCostOfCare handles visit restrictions", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  insertTestData(connection, "main", "cohort")

  # Run with visit restriction (only inpatient visits - 9201)
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    restrictVisitConceptIds = c(9201),
    returnFormat = "tibble"
  )

  expect_s3_class(result, "tbl_df")
  expect_true(nrow(result) > 0)
  # Should have lower total cost since we're filtering visits
  expect_true(result$total_cost[1] < 2100) # Total would be 2100 without filter
})

test_that("calculateCostOfCare handles event filters", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  insertTestData(connection, "main", "cohort")

  # Add some drug exposures for testing
  DatabaseConnector::executeSql(connection, "
    INSERT INTO drug_exposure (drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, visit_occurrence_id)
    VALUES
      (1, 1, 1234, '2021-01-15', 1),
      (2, 2, 1234, '2021-02-15', 3)
  ")

  # Create event filter
  eventFilters <- list(
    list(
      name = "TestDrug",
      domain = "Drug",
      conceptIds = c(1234)
    )
  )

  # Run with event filter
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    eventFilters = eventFilters,
    returnFormat = "list"
  )

  expect_type(result, "list")
  expect_s3_class(result$results, "tbl_df")
  # Should only include visits with the drug exposure
  expect_true(result$results$distinct_visits[1] <= 2)
})

test_that("calculateCostOfCare handles different anchor columns", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  insertTestData(connection, "main", "cohort")

  # Test with cohort_end_date as anchor
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    anchorCol = "cohort_end_date",
    startOffsetDays = -90,
    endOffsetDays = 0,
    returnFormat = "tibble"
  )

  expect_s3_class(result, "tbl_df")
  expect_true(nrow(result) > 0)
})

test_that("calculateCostOfCare handles micro-costing scenario", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  insertTestData(connection, "main", "cohort")

  # Add visit details and associated costs
  DatabaseConnector::executeSql(connection, "
    INSERT INTO visit_detail (visit_detail_id, person_id, visit_occurrence_id, visit_detail_start_date)
    VALUES
      (1, 1, 1, '2021-01-15'),
      (2, 1, 1, '2021-01-15')
  ")

  # Add procedure for event filter
  DatabaseConnector::executeSql(connection, "
    INSERT INTO procedure_occurrence (procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, visit_occurrence_id, visit_detail_id)
    VALUES
      (1, 1, 5678, '2021-01-15', 1, 1)
  ")

  # Add cost for visit detail
  DatabaseConnector::executeSql(connection, "
    INSERT INTO cost (cost_id, cost_event_id, person_id, cost_concept_id, currency_concept_id, total_charge)
    VALUES
      (10, 1, 1, 31978, 44818668, 75.00)
  ")

  # Create event filter for micro-costing
  eventFilters <- list(
    list(
      name = "TestProcedure",
      domain = "Procedure",
      conceptIds = c(5678)
    )
  )

  # Run with micro-costing
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    eventFilters = eventFilters,
    microCosting = TRUE,
    primaryEventFilterName = "TestProcedure",
    returnFormat = "list"
  )

  expect_type(result, "list")
  expect_s3_class(result$results, "tbl_df")
  expect_true("distinct_visit_details" %in% names(result$results))
})

test_that("calculateCostOfCare handles empty cohort gracefully", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  # Don't insert any cohort data

  # Run the function
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 999, # Non-existent cohort
    returnFormat = "tibble"
  )

  # Should return empty result
  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 0)
})

test_that("calculateCostOfCare validates required parameters", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Test missing required parameters
  expect_error(
    calculateCostOfCare(
      conn = connection,
      cdmSchema = "main"
      # Missing other required parameters
    ),
    "argument .* is missing"
  )
})

test_that("calculateCostOfCare handles different currency concepts", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  insertTestData(connection, "main", "cohort")

  # Add costs with different currency
  DatabaseConnector::executeSql(connection, "
    INSERT INTO cost (cost_id, cost_event_id, person_id, cost_concept_id, currency_concept_id, total_charge)
    VALUES
      (20, 1, 1, 31978, 44818669, 100.00)
  ")

  # Run with different currency concept
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    currencyConceptId = 44818669,
    returnFormat = "tibble"
  )

  expect_s3_class(result, "tbl_df")
  # Should only get the cost with matching currency
  expect_true(result$total_cost[1] == 100.00)
})

test_that("calculateCostOfCare cleans up temporary tables", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  insertTestData(connection, "main", "cohort")

  # Run the function
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    asPermanent = FALSE,
    returnFormat = "tibble"
  )

  # Check that temp tables are cleaned up
  # This is database-specific, but we can check that the function completes without error
  expect_s3_class(result, "tbl_df")
})

test_that("calculateCostOfCare handles permanent tables option", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  insertTestData(connection, "main", "cohort")

  # Run with permanent tables
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    asPermanent = TRUE,
    returnFormat = "tibble"
  )

  expect_s3_class(result, "tbl_df")
  # Note: In a real test, you might want to check that tables still exist
})

test_that("calculateCostOfCare handles verbose and logger options", {
  connection <- createMockConnection()
  on.exit(DatabaseConnector::disconnect(connection))

  # Setup test environment
  createTestCdmTables(connection)
  createCohortTable(connection, "main", "cohort")
  insertTestData(connection, "main", "cohort")

  # Create a mock logger
  mockLogger <- list(
    log = function(level, message) {
      # Mock logging function
    }
  )

  # Run with verbose and logger
  result <- calculateCostOfCare(
    conn = connection,
    cdmSchema = "main",
    cohortSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    verbose = TRUE,
    logger = mockLogger,
    returnFormat = "tibble"
  )

  expect_s3_class(result, "tbl_df")
})
