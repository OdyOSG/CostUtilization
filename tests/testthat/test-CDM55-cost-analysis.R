# Test script for CDM 5.5 cost analysis
# This demonstrates how to use the updated functions with the new cost table structure

library(testthat)
library(CostUtilization)
library(DatabaseConnector)
library(dplyr)

test_that("CDM 5.5 cost analysis works with new table structure", {
  # Skip if Eunomia is not available
  skip_if_not_installed("Eunomia")
  
  # Setup connection
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Create a mock CDM 5.5 cost table for testing
  # Note: In production, this would be your actual CDM 5.5 cost table
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE IF NOT EXISTS main.cost_cdm55 (
      cost_id BIGINT PRIMARY KEY,
      person_id BIGINT NOT NULL,
      visit_occurrence_id BIGINT,
      visit_detail_id BIGINT,
      event_cost_id BIGINT,  -- This is the new field name in CDM 5.5
      effective_date DATE,
      cost_event_field_concept_id INT,
      cost_type_concept_id INT,
      cost_concept_id INT,
      cost_source_value VARCHAR(50),
      currency_concept_id INT,
      cost_source_concept_id INT,
      cost FLOAT,
      payer_plan_period_id BIGINT,
      incurred_date DATE,
      billed_date DATE,
      paid_date DATE
    )
  ")
  
  # Insert sample data aligned with CDM 5.5 structure
  sampleCostData <- tibble::tibble(
    cost_id = 1:100,
    person_id = sample(1:10, 100, replace = TRUE),
    visit_occurrence_id = sample(1:50, 100, replace = TRUE),
    visit_detail_id = sample(c(NA, 1:20), 100, replace = TRUE),
    event_cost_id = 1:100,  # New field
    effective_date = as.Date("2020-01-01") + sample(0:365, 100, replace = TRUE),
    cost_event_field_concept_id = 1147332,
    cost_type_concept_id = 31968,
    cost_concept_id = sample(c(31978, 31980, 31981), 100, replace = TRUE),
    cost_source_value = "test",
    currency_concept_id = 44818668,  # USD
    cost_source_concept_id = 0,
    cost = round(runif(100, 10, 5000), 2),
    payer_plan_period_id = sample(1:20, 100, replace = TRUE),
    incurred_date = as.Date("2020-01-01") + sample(0:365, 100, replace = TRUE),
    billed_date = as.Date("2020-01-01") + sample(0:365, 100, replace = TRUE),
    paid_date = as.Date("2020-01-01") + sample(0:365, 100, replace = TRUE)
  )
  
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "cost_cdm55",
    data = sampleCostData,
    databaseSchema = "main",
    camelCaseToSnakeCase = TRUE
  )
  
  # Create a test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE IF NOT EXISTS main.test_cohort_cdm55 AS
    SELECT DISTINCT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      DATE('2020-01-01') AS cohort_start_date,
      DATE('2020-12-31') AS cohort_end_date
    FROM main.person
    WHERE person_id <= 10
  ")
  
  # Create cost settings
  costSettings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    costConceptId = 31980,  # Total cost
    currencyConceptId = 44818668  # USD
  )
  
  # Run the CDM 5.5 compatible analysis
  results <- calculateCostOfCareCDM55(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_cdm55",
    cohortId = 1,
    costOfCareSettings = costSettings,
    verbose = FALSE
  )
  
  # Verify results structure
  expect_type(results, "list")
  expect_named(results, c("results", "diagnostics"))
  
  # Check results content
  expect_s3_class(results$results, "tbl_df")
  expect_s3_class(results$diagnostics, "tbl_df")
  
  # Verify key columns exist
  expect_true("totalCost" %in% names(results$results))
  expect_true("costPppm" %in% names(results$results))
  expect_true("nPersonsWithCost" %in% names(results$results))
  
  # Check diagnostics
  expect_true(nrow(results$diagnostics) > 0)
  expect_true("stepName" %in% names(results$diagnostics))
  expect_true("nPersons" %in% names(results$diagnostics))
  
  # Clean up
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS main.cost_cdm55")
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS main.test_cohort_cdm55")
})

test_that("CDM 5.5 analysis handles event filters correctly", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE IF NOT EXISTS main.test_cohort_filters AS
    SELECT 
      1 AS cohort_definition_id,
      person_id AS subject_id,
      DATE('2020-01-01') AS cohort_start_date,
      DATE('2020-12-31') AS cohort_end_date
    FROM main.person
    WHERE person_id <= 5
  ")
  
  # Define event filters
  eventFilters <- list(
    list(
      name = "Test Procedures",
      domain = "Procedure",
      conceptIds = c(4301351, 4142875)
    ),
    list(
      name = "Test Drugs",
      domain = "Drug",
      conceptIds = c(1308216, 1310149)
    )
  )
  
  # Create settings with event filters
  costSettings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = -30,
    endOffsetDays = 90,
    costConceptId = 31980,
    currencyConceptId = 44818668,
    eventFilters = eventFilters
  )
  
  # This should run without error even if no matching events exist
  expect_no_error({
    results <- calculateCostOfCareCDM55(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "test_cohort_filters",
      cohortId = 1,
      costOfCareSettings = costSettings,
      verbose = FALSE
    )
  })
  
  # Clean up
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS main.test_cohort_filters")
})

# Helper function to create cost settings
createCostOfCareSettings <- function(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    costConceptId = 0,
    currencyConceptId = 0,
    restrictVisitConceptIds = NULL,
    eventFilters = NULL,
    microCosting = FALSE,
    primaryEventFilterName = NULL
) {
  settings <- list(
    anchorCol = anchorCol,
    startOffsetDays = startOffsetDays,
    endOffsetDays = endOffsetDays,
    costConceptId = costConceptId,
    currencyConceptId = currencyConceptId,
    hasVisitRestriction = !is.null(restrictVisitConceptIds),
    restrictVisitConceptIds = restrictVisitConceptIds,
    hasEventFilters = !is.null(eventFilters),
    eventFilters = eventFilters,
    nFilters = if (!is.null(eventFilters)) 1 else 0,
    microCosting = microCosting,
    primaryEventFilterName = primaryEventFilterName
  )
  
  class(settings) <- "CostOfCareSettings"
  return(settings)
}