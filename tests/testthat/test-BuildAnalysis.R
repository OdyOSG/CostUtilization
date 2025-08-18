# tests/testthat/test-BuildAnalysis.R

test_that("executeSqlPlan runs without errors", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create minimal test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort_sql AS
    SELECT 
      1 AS cohort_definition_id,
      1 AS subject_id,
      '2010-01-01' AS cohort_start_date,
      '2010-12-31' AS cohort_end_date
  ")
  
  # Prepare parameters for SQL execution
  params <- list(
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_sql",
    cohortId = 1,
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    costConceptId = 31978,
    currencyConceptId = 44818668,
    hasVisitRestriction = FALSE,
    hasEventFilters = FALSE,
    microCosting = FALSE,
    nFilters = 0,
    resultsTable = "test_results",
    diagTable = "test_diagnostics"
  )
  
  # Test SQL plan execution
  expect_no_error({
    executeSqlPlan(
      connection = connection,
      params = params,
      targetDialect = "sqlite",
      tempEmulationSchema = NULL,
      verbose = FALSE
    )
  })
  
  # Check that results tables were created
  tables <- DatabaseConnector::getTableNames(connection)
  expect_true("test_results" %in% tolower(tables) || "TEST_RESULTS" %in% tables)
  expect_true("test_diagnostics" %in% tolower(tables) || "TEST_DIAGNOSTICS" %in% tables)
})

test_that("prepareSqlRenderParams handles all parameter types", {
  # Test basic parameters
  params <- list(
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "results",
    cohortTable = "cohort",
    cohortId = 123,
    anchorCol = "cohort_start_date",
    startOffsetDays = -30,
    endOffsetDays = 90,
    costConceptId = 31978,
    currencyConceptId = 44818668,
    nFilters = 2,
    hasVisitRestriction = TRUE,
    hasEventFilters = TRUE,
    microCosting = FALSE,
    resultsTable = "my_results",
    diagTable = "my_diagnostics",
    restrictVisitTable = "visit_restrict",
    eventConceptsTable = "event_concepts"
  )
  
  sqlParams <- prepareSqlRenderParams(params, NULL)
  
  # Check parameter mapping
  expect_equal(sqlParams$cdm_database_schema, "cdm")
  expect_equal(sqlParams$cohort_database_schema, "results")
  expect_equal(sqlParams$cohort_table, "cohort")
  expect_equal(sqlParams$cohort_id, 123)
  expect_equal(sqlParams$anchor_col, "cohort_start_date")
  expect_equal(sqlParams$time_a, -30)
  expect_equal(sqlParams$time_b, 90)
  expect_equal(sqlParams$cost_concept_id, 31978)
  expect_equal(sqlParams$currency_concept_id, 44818668)
  expect_equal(sqlParams$n_filters, 2)
  expect_true(sqlParams$has_visit_restriction)
  expect_true(sqlParams$has_event_filters)
  expect_false(sqlParams$micro_costing)
})

test_that("prepareSqlRenderParams handles micro-costing parameters", {
  # Test micro-costing with primary filter
  params <- list(
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "results",
    cohortTable = "cohort",
    cohortId = 1,
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 90,
    costConceptId = 31978,
    currencyConceptId = 44818668,
    nFilters = 2,
    hasVisitRestriction = FALSE,
    hasEventFilters = TRUE,
    microCosting = TRUE,
    primaryEventFilterName = "Primary Procedures",
    eventFilters = list(
      list(name = "Primary Procedures", domain = "Procedure", conceptIds = c(1234)),
      list(name = "Secondary Drugs", domain = "Drug", conceptIds = c(5678))
    ),
    resultsTable = "results",
    diagTable = "diagnostics"
  )
  
  sqlParams <- prepareSqlRenderParams(params, NULL)
  
  expect_true(sqlParams$micro_costing)
  expect_equal(sqlParams$primary_filter_id, 1)  # First filter in list
})

test_that("prepareSqlRenderParams handles schema qualification", {
  params <- list(
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "results",
    cohortTable = "cohort",
    cohortId = 1,
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    costConceptId = 31978,
    currencyConceptId = 44818668,
    nFilters = 0,
    hasVisitRestriction = FALSE,
    hasEventFilters = FALSE,
    microCosting = FALSE,
    resultsTable = "my_results",
    diagTable = "my_diagnostics",
    restrictVisitTable = "visit_table",
    eventConceptsTable = "event_table"
  )
  
  # Test with temp emulation schema
  sqlParams <- prepareSqlRenderParams(params, "temp_schema")
  
  expect_equal(sqlParams$results_table, "temp_schema.my_results")
  expect_equal(sqlParams$diag_table, "temp_schema.my_diagnostics")
  expect_equal(sqlParams$restrict_visit_table, "temp_schema.visit_table")
  expect_equal(sqlParams$event_concepts_table, "temp_schema.event_table")
  
  # Test without temp emulation schema
  sqlParams2 <- prepareSqlRenderParams(params, NULL)
  
  expect_equal(sqlParams2$results_table, "my_results")
  expect_equal(sqlParams2$diag_table, "my_diagnostics")
})

test_that("prepareSqlRenderParams handles missing primary filter gracefully", {
  params <- list(
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "results",
    cohortTable = "cohort",
    cohortId = 1,
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 90,
    costConceptId = 31978,
    currencyConceptId = 44818668,
    nFilters = 1,
    hasVisitRestriction = FALSE,
    hasEventFilters = TRUE,
    microCosting = TRUE,
    primaryEventFilterName = "Non-existent Filter",
    eventFilters = list(
      list(name = "Some Filter", domain = "Drug", conceptIds = c(1234))
    ),
    resultsTable = "results",
    diagTable = "diagnostics"
  )
  
  expect_error(
    prepareSqlRenderParams(params, NULL),
    "Could not find a unique primary event filter"
  )
})

test_that("executeSqlStatements handles SQL errors gracefully", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Test with invalid SQL
  invalidStatements <- c(
    "SELECT * FROM non_existent_table;",
    "INVALID SQL STATEMENT;"
  )
  
  expect_error(
    executeSqlStatements(
      connection = connection,
      sqlStatements = invalidStatements,
      verbose = FALSE
    )
  )
})

test_that("cleanupTempTables removes tables correctly", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Create test tables
  DatabaseConnector::executeSql(connection, "CREATE TEMPORARY TABLE temp_test1 (id INT)")
  DatabaseConnector::executeSql(connection, "CREATE TEMPORARY TABLE temp_test2 (id INT)")
  
  # Clean up tables
  cleanupTempTables(
    connection = connection,
    schema = NULL,
    "temp_test1",
    "temp_test2",
    NULL  # Test NULL handling
  )
  
  # Verify tables are removed
  tables <- tolower(DatabaseConnector::getTableNames(connection))
  expect_false("temp_test1" %in% tables)
  expect_false("temp_test2" %in% tables)
})

test_that("logMessage handles different log levels", {
  # Test different log levels
  expect_no_error(logMessage("Test info", TRUE, "INFO"))
  expect_no_error(logMessage("Test warning", TRUE, "WARNING"))
  expect_no_error(logMessage("Test error", TRUE, "ERROR"))
  expect_no_error(logMessage("Test debug", TRUE, "DEBUG"))
  expect_no_error(logMessage("Test success", TRUE, "SUCCESS"))
  
  # Test with verbose = FALSE (should not output)
  expect_no_error(logMessage("Silent message", FALSE, "INFO"))
})