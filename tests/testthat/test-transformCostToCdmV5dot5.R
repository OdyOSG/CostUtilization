# tests/testthat/test-transformCostToCdmV5dot5.R

test_that("transformCostToCdmV5dot5 creates proper long format", {
  skip_if_not_installed("Eunomia")
  
  # Create a fresh connection for this test
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  
  # Run the transformation (it includes injectCostData)
  connection <- transformCostToCdmV5dot5(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Check that backup table was created
  tables <- tolower(DatabaseConnector::getTableNames(connection, "main"))
  expect_true("cost_v5_3_backup" %in% tables)
  expect_true("cost" %in% tables)
  
  # Check the structure of the new cost table
  costStructure <- DatabaseConnector::querySql(connection, "PRAGMA table_info(cost)")
  actualColumns <- tolower(costStructure$name)
  
  # Expected columns in CDM v5.5 format
  expectedColumns <- c(
    "cost_id", "person_id", "cost_event_id", "visit_occurrence_id",
    "visit_detail_id", "cost_domain_id", "effective_date", 
    "cost_event_field_concept_id", "cost_concept_id", "cost_type_concept_id",
    "cost_source_concept_id", "cost_source_value", "currency_concept_id",
    "cost", "incurred_date", "billed_date", "paid_date",
    "payer_plan_period_id", "amount_allowed"
  )
  
  # Check that key columns exist
  keyColumns <- c("cost_id", "person_id", "cost_concept_id", "cost", "incurred_date")
  for (col in keyColumns) {
    expect_true(col %in% actualColumns, info = paste("Missing column:", col))
  }
})

test_that("transformCostToCdmV5dot5 preserves cost data integrity", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- transformCostToCdmV5dot5(connectionDetails, createIndexes = FALSE)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Compare total costs before and after transformation
  # Note: The wide format has multiple cost columns, long format has rows
  wideCostTotal <- DatabaseConnector::querySql(connection, "
    SELECT 
      SUM(COALESCE(total_charge, 0)) + 
      SUM(COALESCE(total_cost, 0)) + 
      SUM(COALESCE(paid_by_payer, 0)) + 
      SUM(COALESCE(paid_by_patient, 0)) AS total_wide_cost
    FROM main.cost_v5_3_backup
  ")
  
  longCostTotal <- DatabaseConnector::querySql(connection, "
    SELECT SUM(COALESCE(cost, 0)) AS total_long_cost
    FROM main.cost
  ")
  
  # The totals should be equal (allowing for floating point differences)
  expect_true(
    abs(wideCostTotal$TOTAL_WIDE_COST - longCostTotal$TOTAL_LONG_COST) < 1,
    info = sprintf("Wide: %f, Long: %f", 
                   wideCostTotal$TOTAL_WIDE_COST, 
                   longCostTotal$TOTAL_LONG_COST)
  )
})

test_that("transformCostToCdmV5dot5 creates correct cost concept mappings", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- transformCostToCdmV5dot5(connectionDetails, createIndexes = FALSE)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Check that correct cost concepts are used
  costConcepts <- DatabaseConnector::querySql(connection, "
    SELECT DISTINCT cost_concept_id, cost_source_value
    FROM main.cost
    ORDER BY cost_concept_id
  ")
  
  # Expected mappings
  expectedMappings <- data.frame(
    cost_concept_id = c(31973, 31974, 31975, 31976, 31979, 31980, 31981, 31985),
    cost_source_value = c("total_charge", "paid_patient_copay", "paid_patient_coinsurance",
                          "paid_patient_deductible", "amount_allowed", "paid_by_payer",
                          "paid_by_patient", "total_cost")
  )
  
  # Check that we have the expected cost concepts
  for (i in 1:nrow(expectedMappings)) {
    concept_id <- expectedMappings$cost_concept_id[i]
    source_value <- expectedMappings$cost_source_value[i]
    
    matching_rows <- costConcepts[costConcepts$COST_CONCEPT_ID == concept_id, ]
    if (nrow(matching_rows) > 0) {
      expect_equal(
        tolower(matching_rows$COST_SOURCE_VALUE[1]), 
        source_value,
        info = paste("Concept ID:", concept_id)
      )
    }
  }
})

test_that("transformCostToCdmV5dot5 handles visit linkage correctly", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- transformCostToCdmV5dot5(connectionDetails, createIndexes = FALSE)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Check that visit_occurrence_id is populated
  visitLinkage <- DatabaseConnector::querySql(connection, "
    SELECT 
      COUNT(*) AS total_records,
      COUNT(visit_occurrence_id) AS records_with_visit,
      COUNT(DISTINCT visit_occurrence_id) AS unique_visits
    FROM main.cost
    WHERE cost_domain_id IN ('Procedure', 'Drug', 'Measurement')
  ")
  
  # Most records should have visit linkage
  expect_true(visitLinkage$RECORDS_WITH_VISIT > 0)
  expect_true(visitLinkage$UNIQUE_VISITS > 0)
})

test_that("transformCostToCdmV5dot5 creates indexes when requested", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- transformCostToCdmV5dot5(connectionDetails, createIndexes = TRUE)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Check for indexes (SQLite specific)
  indexes <- DatabaseConnector::querySql(connection, "
    SELECT name 
    FROM sqlite_master 
    WHERE type = 'index' 
    AND tbl_name = 'cost'
    AND name LIKE 'idx_cost%'
  ")
  
  # Should have multiple indexes
  expect_true(nrow(indexes) > 5, 
              info = paste("Found", nrow(indexes), "indexes"))
  
  # Check for key indexes
  indexNames <- tolower(indexes$NAME)
  expectedIndexes <- c("idx_cost_person_id", "idx_cost_visit_occurrence_id", 
                       "idx_cost_cost_concept_id")
  
  for (idx in expectedIndexes) {
    expect_true(idx %in% indexNames, info = paste("Missing index:", idx))
  }
})

test_that("transformCostToCdmV5dot5 handles missing wide cost table", {
  skip_if_not_installed("Eunomia")
  
  # Create a connection without cost data
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = ":memory:"
  )
  
  # This should not error - it should inject cost data first
  expect_no_error({
    connection <- transformCostToCdmV5dot5(connectionDetails)
    DatabaseConnector::disconnect(connection)
  })
})

test_that("transformCostToCdmV5dot5 preserves payer plan period linkage", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- transformCostToCdmV5dot5(connectionDetails, createIndexes = FALSE)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Check payer plan period linkage
  payerLinkage <- DatabaseConnector::querySql(connection, "
    SELECT 
      COUNT(*) AS total_records,
      COUNT(payer_plan_period_id) AS records_with_payer,
      COUNT(DISTINCT payer_plan_period_id) AS unique_payers
    FROM main.cost
  ")
  
  # All records should have payer plan period
  expect_equal(payerLinkage$TOTAL_RECORDS, payerLinkage$RECORDS_WITH_PAYER)
  expect_true(payerLinkage$UNIQUE_PAYERS > 0)
})

test_that("transformCostToCdmV5dot5 sets correct default values", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- transformCostToCdmV5dot5(connectionDetails, createIndexes = FALSE)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Check default values
  defaults <- DatabaseConnector::querySql(connection, "
    SELECT 
      COUNT(*) AS total_records,
      SUM(CASE WHEN cost_type_concept_id = 31968 THEN 1 ELSE 0 END) AS default_cost_type,
      SUM(CASE WHEN currency_concept_id = 44818668 THEN 1 ELSE 0 END) AS default_currency,
      SUM(CASE WHEN cost_event_field_concept_id = 1147332 THEN 1 ELSE 0 END) AS default_event_field
    FROM main.cost
  ")
  
  # Most records should have default values
  expect_true(defaults$DEFAULT_COST_TYPE > 0)
  expect_true(defaults$DEFAULT_CURRENCY > 0)
  expect_true(defaults$DEFAULT_EVENT_FIELD > 0)
})

test_that("transformCostToCdmV5dot5 handles NULL values correctly", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- transformCostToCdmV5dot5(connectionDetails, createIndexes = FALSE)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Check that only non-NULL costs are included
  nullCheck <- DatabaseConnector::querySql(connection, "
    SELECT 
      COUNT(*) AS total_records,
      COUNT(cost) AS non_null_cost,
      COUNT(CASE WHEN cost = 0 THEN 1 END) AS zero_cost,
      COUNT(CASE WHEN cost > 0 THEN 1 END) AS positive_cost
    FROM main.cost
  ")
  
  # All records should have non-NULL cost
  expect_equal(nullCheck$TOTAL_RECORDS, nullCheck$NON_NULL_COST)
  
  # Most costs should be positive
  expect_true(nullCheck$POSITIVE_COST > nullCheck$ZERO_COST)
})

test_that("transformCostToCdmV5dot5 handles dates correctly", {
  skip_if_not_installed("Eunomia")
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- transformCostToCdmV5dot5(connectionDetails, createIndexes = FALSE)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Check date fields
  dates <- DatabaseConnector::querySql(connection, "
    SELECT 
      COUNT(*) AS total_records,
      COUNT(effective_date) AS has_effective_date,
      COUNT(incurred_date) AS has_incurred_date,
      MIN(effective_date) AS min_date,
      MAX(effective_date) AS max_date
    FROM main.cost
  ")
  
  # All records should have dates
  expect_equal(dates$TOTAL_RECORDS, dates$HAS_EFFECTIVE_DATE)
  expect_equal(dates$TOTAL_RECORDS, dates$HAS_INCURRED_DATE)
  
  # Dates should be reasonable
  expect_true(!is.na(dates$MIN_DATE))
  expect_true(!is.na(dates$MAX_DATE))
})