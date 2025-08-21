# Test BuildAnalysis functions

test_that("prepareSqlRenderParams correctly maps parameters", {
  # Create test parameters
  params <- list(
    cdmDatabaseSchema = "cdm_schema",
    cohortDatabaseSchema = "cohort_schema",
    cohortTable = "my_cohort",
    cohortId = 123,
    anchorCol = "cohort_start_date",
    startOffsetDays = -30,
    endOffsetDays = 365,
    costConceptId = 31978,
    currencyConceptId = 44818668,
    nFilters = 2,
    hasVisitRestriction = TRUE,
    hasEventFilters = TRUE,
    microCosting = FALSE,
    resultsTable = "results",
    diagTable = "diagnostics",
    restrictVisitTable = "visit_restrict",
    eventConceptsTable = "event_concepts",
    eventFilters = list(
      list(name = "Filter1"),
      list(name = "Filter2")
    ),
    primaryEventFilterName = NULL
  )
  
  # Test without temp schema
  sqlParams <- prepareSqlRenderParams(params, NULL)
  
  # Check direct mappings
  expect_equal(sqlParams$cdm_database_schema, "cdm_schema")
  expect_equal(sqlParams$cohort_database_schema, "cohort_schema")
  expect_equal(sqlParams$cohort_table, "my_cohort")
  expect_equal(sqlParams$cohort_id, 123)
  expect_equal(sqlParams$anchor_col, "cohort_start_date")
  expect_equal(sqlParams$time_a, -30)
  expect_equal(sqlParams$time_b, 365)
  expect_equal(sqlParams$cost_concept_id, 31978)
  expect_equal(sqlParams$currency_concept_id, 44818668)
  expect_equal(sqlParams$n_filters, 2)
  expect_true(sqlParams$has_visit_restriction)
  expect_true(sqlParams$has_event_filters)
  expect_false(sqlParams$micro_costing)
  expect_equal(sqlParams$primary_filter_id, 0)
  
  # Check unqualified table names
  expect_equal(sqlParams$results_table, "results")
  expect_equal(sqlParams$diag_table, "diagnostics")
  expect_equal(sqlParams$restrict_visit_table, "visit_restrict")
  expect_equal(sqlParams$event_concepts_table, "event_concepts")
  
  # Test with temp schema
  sqlParams2 <- prepareSqlRenderParams(params, "temp_schema")
  
  # Check qualified table names
  expect_equal(sqlParams2$results_table, "temp_schema.results")
  expect_equal(sqlParams2$diag_table, "temp_schema.diagnostics")
  expect_equal(sqlParams2$restrict_visit_table, "temp_schema.visit_restrict")
  expect_equal(sqlParams2$event_concepts_table, "temp_schema.event_concepts")
})

test_that("prepareSqlRenderParams handles micro-costing parameters", {
  # Test with micro-costing enabled
  params <- list(
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "cohort",
    cohortTable = "cohort",
    cohortId = 1,
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 90,
    costConceptId = 31978,
    currencyConceptId = 44818668,
    nFilters = 3,
    hasVisitRestriction = FALSE,
    hasEventFilters = TRUE,
    microCosting = TRUE,
    resultsTable = "results",
    diagTable = "diag",
    restrictVisitTable = NULL,
    eventConceptsTable = "events",
    eventFilters = list(
      list(name = "Filter1"),
      list(name = "Primary"),
      list(name = "Filter3")
    ),
    primaryEventFilterName = "Primary"
  )
  
  sqlParams <- prepareSqlRenderParams(params, NULL)
  
  # Should find the primary filter at index 2
  expect_equal(sqlParams$primary_filter_id, 2)
  
  # Test with non-existent primary filter
  params$primaryEventFilterName <- "NonExistent"
  
  expect_error(
    prepareSqlRenderParams(params, NULL),
    "Could not find a unique primary event filter"
  )
  
  # Test with duplicate filter names
  params$eventFilters <- list(
    list(name = "Primary"),
    list(name = "Primary"),
    list(name = "Filter3")
  )
  params$primaryEventFilterName <- "Primary"
  
  expect_error(
    prepareSqlRenderParams(params, NULL),
    "Could not find a unique primary event filter"
  )
})

test_that("executeSqlPlan integrates all components", {
  # This is an integration test that requires a real database connection
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Create a minimal test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE test_cohort_sql AS
    SELECT 
      1 AS cohort_definition_id,
      person_id AS subject_id,
      observation_period_start_date AS cohort_start_date,
      observation_period_end_date AS cohort_end_date
    FROM main.observation_period
    LIMIT 5
  ")
  
  # Prepare parameters
  params <- list(
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_sql",
    cohortId = 1,
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 30,
    costConceptId = 31978,
    currencyConceptId = 44818668,
    nFilters = 0,
    hasVisitRestriction = FALSE,
    hasEventFilters = FALSE,
    microCosting = FALSE,
    resultsTable = "test_results_sql",
    diagTable = "test_diag_sql",
    restrictVisitTable = NULL,
    eventConceptsTable = NULL,
    eventFilters = NULL,
    primaryEventFilterName = NULL
  )
  
  # Execute SQL plan
  expect_silent(
    executeSqlPlan(
      connection = connection,
      params = params,
      targetDialect = "sqlite",
      tempEmulationSchema = NULL,
      verbose = FALSE
    )
  )
  
  # Verify results table was created
  tables <- DatabaseConnector::getTableNames(connection)
  expect_true("test_results_sql" %in% tolower(tables))
  expect_true("test_diag_sql" %in% tolower(tables))
  
  # Check diagnostics table has data
  diag <- DatabaseConnector::querySql(connection, "SELECT * FROM test_diag_sql")
  expect_true(nrow(diag) > 0)
  
  # Clean up
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS test_results_sql")
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS test_diag_sql")
})

test_that("executeSqlPlan handles errors gracefully", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Parameters with non-existent cohort table
  params <- list(
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "non_existent_cohort",
    cohortId = 1,
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 30,
    costConceptId = 31978,
    currencyConceptId = 44818668,
    nFilters = 0,
    hasVisitRestriction = FALSE,
    hasEventFilters = FALSE,
    microCosting = FALSE,
    resultsTable = "test_results_error",
    diagTable = "test_diag_error",
    restrictVisitTable = NULL,
    eventConceptsTable = NULL,
    eventFilters = NULL,
    primaryEventFilterName = NULL
  )
  
  # Should error due to missing cohort table
  expect_error(
    executeSqlPlan(
      connection = connection,
      params = params,
      targetDialect = "sqlite",
      tempEmulationSchema = NULL,
      verbose = FALSE
    ),
    "no such table"
  )
})