# tests/testthat/test-cdm-v55-compatibility.R

testthat::test_that("transformCostToCdmV5dot5 creates proper CDM v5.5 long format", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  # Run the full transformation process
  transformCostToCdmV5dot5(connectionDetails)
  
  tables <- tolower(DatabaseConnector::getTableNames(connection, "main"))
  testthat::expect_true("cost_v5_3_backup" %in% tables)
  
  # Check CDM v5.5 structure
  costStructure <- DatabaseConnector::querySql(connection, "PRAGMA table_info(cost)")
  actualColumns <- tolower(costStructure$name)
  
  # CDM v5.5 required columns
  expectedColumns <- c(
    "cost_id", "person_id", "cost_event_id", "visit_occurrence_id", 
    "visit_detail_id", "cost_domain_id", "effective_date",
    "cost_event_field_concept_id", "cost_concept_id", "cost_type_concept_id",
    "cost_source_concept_id", "cost_source_value", "currency_concept_id",
    "cost", "incurred_date", "billed_date", "paid_date"
  )
  
  testthat::expect_true(all(expectedColumns %in% actualColumns))
})

testthat::test_that("CDM v5.5 cost table has correct data types and constraints", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  transformCostToCdmV5dot5(connectionDetails)
  
  # Check that cost_id is primary key (should be unique and not null)
  costIdCheck <- DatabaseConnector::querySql(connection, "
    SELECT 
      COUNT(*) AS total_records,
      COUNT(DISTINCT cost_id) AS unique_cost_ids,
      COUNT(cost_id) AS non_null_cost_ids
    FROM main.cost
  ")
  
  testthat::expect_equal(costIdCheck$total_records, costIdCheck$unique_cost_ids)
  testthat::expect_equal(costIdCheck$total_records, costIdCheck$non_null_cost_ids)
  
  # Check that required fields are not null
  requiredFieldsCheck <- DatabaseConnector::querySql(connection, "
    SELECT 
      COUNT(*) AS total_records,
      COUNT(person_id) AS person_id_count,
      COUNT(effective_date) AS effective_date_count,
      COUNT(cost_concept_id) AS cost_concept_id_count
    FROM main.cost
  ")
  
  testthat::expect_equal(requiredFieldsCheck$total_records, requiredFieldsCheck$person_id_count)
  testthat::expect_equal(requiredFieldsCheck$total_records, requiredFieldsCheck$effective_date_count)
  testthat::expect_equal(requiredFieldsCheck$total_records, requiredFieldsCheck$cost_concept_id_count)
})

testthat::test_that("CDM v5.5 transformation preserves cost relationships", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  transformCostToCdmV5dot5(connectionDetails)
  
  # Check that we have the expected cost concepts
  costConcepts <- DatabaseConnector::querySql(connection, "
    SELECT DISTINCT cost_concept_id, cost_source_value
    FROM main.cost
    ORDER BY cost_concept_id
  ")
  
  expectedConcepts <- c(31973, 31974, 31975, 31976, 31979, 31980, 31981, 31985)
  actualConcepts <- sort(costConcepts$cost_concept_id)
  
  # Should have at least some of the expected concepts
  testthat::expect_true(length(intersect(expectedConcepts, actualConcepts)) > 0)
  
  # Check that cost values are reasonable
  costStats <- DatabaseConnector::querySql(connection, "
    SELECT 
      MIN(cost) AS min_cost,
      MAX(cost) AS max_cost,
      AVG(cost) AS avg_cost,
      COUNT(*) AS n_records
    FROM main.cost
    WHERE cost IS NOT NULL
  ")
  
  testthat::expect_true(costStats$min_cost >= 0)
  testthat::expect_true(costStats$max_cost > costStats$min_cost)
  testthat::expect_true(costStats$n_records > 0)
})

testthat::test_that("createCostOfCareSettings works with CDM v5.5", {
  # Test basic settings creation
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0L,
    endOffsetDays = 365L,
    costConceptId = 31973L,  # Total charge
    currencyConceptId = 44818668L  # USD
  )
  
  testthat::expect_s3_class(settings, "CostOfCareSettings")
  testthat::expect_equal(settings$costConceptId, 31973L)
  testthat::expect_equal(settings$currencyConceptId, 44818668L)
  
  # Test with event filters
  eventFilters <- list(
    list(
      name = "Test Events",
      domain = "Condition",
      conceptIds = c(201820L, 201826L)
    )
  )
  
  settingsWithFilters <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = -30L,
    endOffsetDays = 90L,
    eventFilters = eventFilters,
    costConceptId = 31985L  # Total cost
  )
  
  testthat::expect_true(settingsWithFilters$hasEventFilters)
  testthat::expect_equal(settingsWithFilters$nFilters, 1)
  testthat::expect_equal(settingsWithFilters$costConceptId, 31985L)
})

testthat::test_that("calculateCostOfCare works with CDM v5.5 data", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  # Setup CDM v5.5 data
  transformCostToCdmV5dot5(connectionDetails)
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE main.test_cohort_v55 AS
    SELECT DISTINCT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      MIN(visit_start_date) AS cohort_start_date,
      MAX(visit_end_date) AS cohort_end_date
    FROM main.visit_occurrence
    GROUP BY person_id
    LIMIT 10
  ")
  
  # Create settings
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0L,
    endOffsetDays = 90L,
    costConceptId = 31973L
  )
  
  # Run analysis
  results <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "test_cohort_v55",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE
  )
  
  testthat::expect_type(results, "list")
  testthat::expect_true("results" %in% names(results))
  testthat::expect_true("diagnostics" %in% names(results))
  
  # Check results structure
  testthat::expect_true(nrow(results$results) >= 0)
  testthat::expect_true(nrow(results$diagnostics) > 0)
  
  # Clean up
  DatabaseConnector::executeSql(connection, "DROP TABLE main.test_cohort_v55")
})

testthat::test_that("CDM v5.5 supports multiple cost concept analysis", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  transformCostToCdmV5dot5(connectionDetails)
  
  # Create test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE main.multi_concept_cohort AS
    SELECT DISTINCT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      MIN(visit_start_date) AS cohort_start_date,
      MAX(visit_end_date) AS cohort_end_date
    FROM main.visit_occurrence
    GROUP BY person_id
    LIMIT 5
  ")
  
  # Test different cost concepts
  costConcepts <- c(31973L, 31985L, 31980L, 31981L)  # charge, cost, payer, patient
  
  results <- purrr::map_dfr(costConcepts, function(conceptId) {
    settings <- createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0L,
      endOffsetDays = 30L,
      costConceptId = conceptId
    )
    
    result <- calculateCostOfCare(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "multi_concept_cohort",
      cohortId = 1,
      costOfCareSettings = settings,
      verbose = FALSE
    )
    
    result$results |>
      dplyr::mutate(costConceptId = conceptId)
  })
  
  # Should have results for multiple concepts
  testthat::expect_true(length(unique(results$costConceptId)) > 1)
  
  # All cost values should be non-negative
  testthat::expect_true(all(results$total_cost >= 0, na.rm = TRUE))
  
  # Clean up
  DatabaseConnector::executeSql(connection, "DROP TABLE main.multi_concept_cohort")
})

testthat::test_that("CDM v5.5 temporal fields are properly populated", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  transformCostToCdmV5dot5(connectionDetails)
  
  # Check temporal field population
  temporalCheck <- DatabaseConnector::querySql(connection, "
    SELECT 
      COUNT(*) AS total_records,
      COUNT(effective_date) AS has_effective_date,
      COUNT(incurred_date) AS has_incurred_date,
      COUNT(billed_date) AS has_billed_date,
      COUNT(paid_date) AS has_paid_date,
      MIN(effective_date) AS min_effective_date,
      MAX(effective_date) AS max_effective_date
    FROM main.cost
  ")
  
  # effective_date should be populated for all records
  testthat::expect_equal(temporalCheck$total_records, temporalCheck$has_effective_date)
  
  # Should have reasonable date range
  testthat::expect_true(!is.na(temporalCheck$min_effective_date))
  testthat::expect_true(!is.na(temporalCheck$max_effective_date))
  testthat::expect_true(temporalCheck$max_effective_date >= temporalCheck$min_effective_date)
})

testthat::test_that("CDM v5.5 indexes are created properly", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  # Transform with index creation
  transformCostToCdmV5dot5(connectionDetails, createIndexes = TRUE)
  
  # Check that indexes exist (SQLite specific)
  indexes <- DatabaseConnector::querySql(connection, "
    SELECT name FROM sqlite_master 
    WHERE type = 'index' AND tbl_name = 'cost'
    AND name LIKE 'idx_cost_%'
  ")
  
  # Should have created multiple indexes
  testthat::expect_true(nrow(indexes) > 0)
  
  # Check for key indexes
  indexNames <- tolower(indexes$name)
  expectedIndexes <- c("idx_cost_person_id", "idx_cost_effective_date", "idx_cost_cost_concept_id")
  
  # Should have at least some expected indexes
  testthat::expect_true(any(expectedIndexes %in% indexNames))
})