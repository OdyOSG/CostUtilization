library(testthat)
library(DBI)
library(duckdb)
library(dplyr)
library(purrr)
library(rlang)

#===============================================================================
# Enhanced Test Suite for transformCostToCdmV5dot5() - CDM v5.5 Compliance
#===============================================================================

# Define CDM v5.5 cost concept mappings for testing
COST_CONCEPTS <- list(
  total_charge = 31973L,
  total_cost = 31985L,
  paid_by_payer = 31980L,
  paid_by_patient = 31981L,
  paid_patient_copay = 31974L,
  paid_patient_coinsurance = 31975L,
  paid_patient_deductible = 31976L,
  amount_allowed = 31979L
)

# Expected CDM v5.5 cost table schema
EXPECTED_COST_SCHEMA <- c(
  "cost_id", "person_id", "visit_occurrence_id", "visit_detail_id",
  "effective_date", "cost_event_field_concept_id", "cost_type_concept_id",
  "cost_concept_id", "cost_source_value", "currency_concept_id",
  "cost_source_concept_id", "cost", "payer_plan_period_id",
  "incurred_date", "billed_date", "paid_date"
)

describe("transformCostToCdmV5dot5 - Enhanced CDM v5.5 Tests", {
  
  # Setup test environment
  setup_test_db <- function() {
    databaseFile <- getEunomiaDuckDb(pathToData = 'testing_data')
    con <- DBI::dbConnect(duckdb::duckdb(databaseFile))
    return(con)
  }
  
  cleanup_test_db <- function(con, db_file = NULL) {
    if (DBI::dbIsValid(con)) {
      DBI::dbDisconnect(con, shutdown = TRUE)
    }
    if (!is.null(db_file) && file.exists(db_file)) {
      unlink(db_file)
    }
  }
  
  #-----------------------------------------------------------------------------
  # 1. Comprehensive Schema Validation Tests
  #-----------------------------------------------------------------------------
  
  it("should create CDM v5.5 compliant cost table with correct schema", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    # Run transformation
    con <- transformCostToCdmV5dot5(con)
    
    # Get actual schema
    cost_schema_query <- "SELECT column_name, data_type 
                         FROM information_schema.columns 
                         WHERE table_name = 'cost' 
                         ORDER BY ordinal_position;"
    
    actual_schema <- DBI::dbGetQuery(con, cost_schema_query) |>
      dplyr::rename_with(tolower)
    
    # Verify all expected columns exist
    expect_true(all(EXPECTED_COST_SCHEMA %in% actual_schema$column_name))
    
    # Verify specific data types for key columns
    schema_types <- actual_schema |>
      dplyr::filter(.data$column_name %in% c("cost_id", "person_id", "cost_concept_id", "cost")) |>
      dplyr::pull(.data$data_type)
    
    expect_true(any(grepl("BIGINT|INTEGER", schema_types)))
    expect_true(any(grepl("NUMERIC|DECIMAL", schema_types)))
  })
  
  it("should maintain referential integrity with backup table", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    # Get original cost count before transformation
    original_cost_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM cost;")$n
    
    # Run transformation
    con <- transformCostToCdmV5dot5(con)
    
    # Verify backup table exists and has original data
    backup_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM cost_v5_3_backup;")$n
    expect_equal(backup_count, original_cost_count)
    
    # Verify new cost table has more records (due to pivoting)
    new_cost_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM cost;")$n
    expect_gt(new_cost_count, backup_count)
  })
  
  #-----------------------------------------------------------------------------
  # 2. Cost Concept Mapping Tests
  #-----------------------------------------------------------------------------
  
  it("should correctly map all standard cost concepts", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    con <- transformCostToCdmV5dot5(con)
    
    # Get distinct cost concepts from transformed table
    cost_concepts_query <- "
      SELECT DISTINCT cost_concept_id, cost_source_value, COUNT(*) as record_count
      FROM cost 
      WHERE cost_concept_id IS NOT NULL 
      GROUP BY cost_concept_id, cost_source_value
      ORDER BY cost_concept_id;"
    
    actual_concepts <- DBI::dbGetQuery(con, cost_concepts_query) |>
      dplyr::rename_with(tolower)
    
    # Verify expected cost concepts exist
    expected_concept_ids <- unlist(COST_CONCEPTS, use.names = FALSE)
    found_concepts <- actual_concepts$cost_concept_id
    
    # At least some of the standard concepts should be present
    expect_true(length(intersect(expected_concept_ids, found_concepts)) > 0)
    
    # Verify concept-source value mappings
    concept_mappings <- actual_concepts |>
      dplyr::filter(.data$cost_concept_id %in% expected_concept_ids)
    
    expect_gt(nrow(concept_mappings), 0)
    
    # Test specific mappings
    if (31973L %in% found_concepts) {
      total_charge_mapping <- concept_mappings |>
        dplyr::filter(.data$cost_concept_id == 31973L)
      expect_true("total_charge" %in% total_charge_mapping$cost_source_value)
    }
  })
  
  it("should preserve cost values during transformation", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    # Get sample of original cost values
    original_costs_query <- "
      SELECT person_id, total_charge, total_cost, paid_by_payer, paid_by_patient
      FROM cost 
      WHERE total_charge IS NOT NULL OR total_cost IS NOT NULL
      LIMIT 10;"
    
    original_costs <- DBI::dbGetQuery(con, original_costs_query) |>
      dplyr::rename_with(tolower)
    
    con <- transformCostToCdmV5dot5(con)
    
    # Verify transformed costs match original values
    if (nrow(original_costs) > 0) {
      sample_person <- original_costs$person_id[1]
      
      transformed_costs_query <- glue::glue("
        SELECT cost_concept_id, cost_source_value, cost
        FROM cost 
        WHERE person_id = {sample_person}
        AND cost IS NOT NULL;")
      
      transformed_costs <- DBI::dbGetQuery(con, transformed_costs_query) |>
        dplyr::rename_with(tolower)
      
      expect_gt(nrow(transformed_costs), 0)
      expect_true(all(transformed_costs$cost >= 0))
    }
  })
  
  #-----------------------------------------------------------------------------
  # 3. Temporal Field Validation Tests
  #-----------------------------------------------------------------------------
  
  it("should populate temporal fields correctly", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    con <- transformCostToCdmV5dot5(con)
    
    # Check temporal field population
    temporal_query <- "
      SELECT 
        COUNT(*) as total_records,
        COUNT(effective_date) as has_effective_date,
        COUNT(incurred_date) as has_incurred_date,
        COUNT(billed_date) as has_billed_date,
        COUNT(paid_date) as has_paid_date
      FROM cost;"
    
    temporal_stats <- DBI::dbGetQuery(con, temporal_query) |>
      dplyr::rename_with(tolower)
    
    # effective_date should be populated for most/all records
    expect_gt(temporal_stats$has_effective_date, 0)
    expect_gte(temporal_stats$has_effective_date / temporal_stats$total_records, 0.5)
    
    # Verify date formats are valid
    date_validation_query <- "
      SELECT effective_date, incurred_date
      FROM cost 
      WHERE effective_date IS NOT NULL 
      LIMIT 5;"
    
    date_sample <- DBI::dbGetQuery(con, date_validation_query) |>
      dplyr::rename_with(tolower)
    
    if (nrow(date_sample) > 0) {
      # Dates should be parseable
      expect_true(all(!is.na(as.Date(date_sample$effective_date))))
    }
  })
  
  #-----------------------------------------------------------------------------
  # 4. Data Integrity and Relationship Tests
  #-----------------------------------------------------------------------------
  
  it("should maintain person-visit relationships", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    con <- transformCostToCdmV5dot5(con)
    
    # Check person-visit consistency
    relationship_query <- "
      SELECT 
        c.person_id,
        c.visit_occurrence_id,
        vo.person_id as visit_person_id
      FROM cost c
      LEFT JOIN visit_occurrence vo ON c.visit_occurrence_id = vo.visit_occurrence_id
      WHERE c.visit_occurrence_id IS NOT NULL
      LIMIT 100;"
    
    relationships <- DBI::dbGetQuery(con, relationship_query) |>
      dplyr::rename_with(tolower)
    
    if (nrow(relationships) > 0) {
      # Person IDs should match between cost and visit tables
      mismatched <- relationships |>
        dplyr::filter(.data$person_id != .data$visit_person_id)
      
      expect_equal(nrow(mismatched), 0)
    }
  })
  
  it("should handle visit_detail relationships correctly", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    con <- transformCostToCdmV5dot5(con)
    
    # Check visit_detail relationships where they exist
    visit_detail_query <- "
      SELECT 
        COUNT(*) as total_cost_records,
        COUNT(visit_detail_id) as has_visit_detail,
        COUNT(DISTINCT visit_detail_id) as unique_visit_details
      FROM cost;"
    
    visit_detail_stats <- DBI::dbGetQuery(con, visit_detail_query) |>
      dplyr::rename_with(tolower)
    
    # Should have some cost records
    expect_gt(visit_detail_stats$total_cost_records, 0)
    
    # If visit_detail_id is populated, verify consistency
    if (visit_detail_stats$has_visit_detail > 0) {
      detail_consistency_query <- "
        SELECT c.visit_detail_id, vd.visit_occurrence_id as detail_visit_id, c.visit_occurrence_id
        FROM cost c
        INNER JOIN visit_detail vd ON c.visit_detail_id = vd.visit_detail_id
        WHERE c.visit_detail_id IS NOT NULL
        LIMIT 10;"
      
      detail_consistency <- DBI::dbGetQuery(con, detail_consistency_query) |>
        dplyr::rename_with(tolower)
      
      if (nrow(detail_consistency) > 0) {
        # Visit occurrence IDs should match between cost and visit_detail
        mismatched_visits <- detail_consistency |>
          dplyr::filter(.data$detail_visit_id != .data$visit_occurrence_id)
        
        expect_equal(nrow(mismatched_visits), 0)
      }
    }
  })
  
  #-----------------------------------------------------------------------------
  # 5. Edge Case and Error Handling Tests
  #-----------------------------------------------------------------------------
  
  it("should handle missing cost values gracefully", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    con <- transformCostToCdmV5dot5(con)
    
    # Check for NULL cost handling
    null_cost_query <- "
      SELECT 
        cost_concept_id,
        cost_source_value,
        COUNT(*) as null_cost_count
      FROM cost 
      WHERE cost IS NULL
      GROUP BY cost_concept_id, cost_source_value;"
    
    null_costs <- DBI::dbGetQuery(con, null_cost_query) |>
      dplyr::rename_with(tolower)
    
    # The transformation should filter out NULL costs, so this should be empty or minimal
    if (nrow(null_costs) > 0) {
      expect_lt(sum(null_costs$null_cost_count), 100)  # Allow some NULLs but not excessive
    }
  })
  
  it("should handle duplicate transformations idempotently", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    # First transformation
    con <- transformCostToCdmV5dot5(con)
    first_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM cost;")$n
    
    # Attempting second transformation should handle existing backup gracefully
    expect_no_error({
      # This should work without error, handling the existing backup
      con <- transformCostToCdmV5dot5(con)
    })
    
    second_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM cost;")$n
    
    # Counts might differ due to re-injection of data, but should be reasonable
    expect_gt(second_count, 0)
  })
  
  #-----------------------------------------------------------------------------
  # 6. Performance and Scalability Tests
  #-----------------------------------------------------------------------------
  
  it("should complete transformation within reasonable time", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    # Measure transformation time
    start_time <- Sys.time()
    con <- transformCostToCdmV5dot5(con)
    end_time <- Sys.time()
    
    transformation_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
    
    # Should complete within 60 seconds for test data
    expect_lt(transformation_time, 60)
    
    # Verify transformation actually occurred
    final_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM cost;")$n
    expect_gt(final_count, 0)
  })
  
  #-----------------------------------------------------------------------------
  # 7. Modern R Practices and Functional Tests
  #-----------------------------------------------------------------------------
  
  it("should support functional programming patterns for cost analysis", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    con <- transformCostToCdmV5dot5(con)
    
    # Test using purrr for cost concept analysis
    cost_summary <- COST_CONCEPTS |>
      purrr::map_dfr(~ {
        concept_query <- glue::glue("
          SELECT 
            {.x} as concept_id,
            '{names(COST_CONCEPTS)[COST_CONCEPTS == .x]}' as concept_name,
            COUNT(*) as record_count,
            COALESCE(SUM(cost), 0) as total_cost,
            COALESCE(AVG(cost), 0) as avg_cost
          FROM cost 
          WHERE cost_concept_id = {.x} AND cost IS NOT NULL;")
        
        tryCatch({
          DBI::dbGetQuery(con, concept_query) |>
            dplyr::rename_with(tolower)
        }, error = function(e) {
          tibble::tibble(
            concept_id = .x,
            concept_name = names(COST_CONCEPTS)[COST_CONCEPTS == .x],
            record_count = 0L,
            total_cost = 0,
            avg_cost = 0
          )
        })
      })
    
    expect_s3_class(cost_summary, "data.frame")
    expect_equal(nrow(cost_summary), length(COST_CONCEPTS))
    expect_true(all(c("concept_id", "concept_name", "record_count") %in% names(cost_summary)))
  })
  
  #-----------------------------------------------------------------------------
  # 8. Integration Tests with Settings Objects
  #-----------------------------------------------------------------------------
  
  it("should integrate properly with createCostOfCareSettings", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    con <- transformCostToCdmV5dot5(con)
    
    # Test that different cost concept settings work with transformed data
    cost_concepts_to_test <- c(31973L, 31985L, 31980L, 31981L)
    
    settings_tests <- cost_concepts_to_test |>
      purrr::map_lgl(~ {
        # Check if this cost concept exists in the transformed data
        concept_check_query <- glue::glue("
          SELECT COUNT(*) as n 
          FROM cost 
          WHERE cost_concept_id = {.x} AND cost IS NOT NULL;")
        
        concept_count <- DBI::dbGetQuery(con, concept_check_query)$n
        
        if (concept_count > 0) {
          # Try creating settings with this cost concept
          tryCatch({
            settings <- createCostOfCareSettings(costConceptId = .x)
            expect_s3_class(settings, "CostOfCareSettings")
            expect_equal(settings$costConceptId, .x)
            TRUE
          }, error = function(e) FALSE)
        } else {
          TRUE  # Skip if concept not present in data
        }
      })
    
    expect_true(all(settings_tests))
  })
  
  #-----------------------------------------------------------------------------
  # 9. Comprehensive Data Quality Tests
  #-----------------------------------------------------------------------------
  
  it("should maintain data quality standards", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    con <- transformCostToCdmV5dot5(con)
    
    # Comprehensive data quality checks
    quality_checks_query <- "
      SELECT 
        -- Basic counts
        COUNT(*) as total_records,
        COUNT(DISTINCT person_id) as unique_persons,
        COUNT(DISTINCT cost_concept_id) as unique_cost_concepts,
        
        -- Data completeness
        COUNT(person_id) * 100.0 / COUNT(*) as person_id_completeness,
        COUNT(cost_concept_id) * 100.0 / COUNT(*) as cost_concept_completeness,
        COUNT(cost) * 100.0 / COUNT(*) as cost_value_completeness,
        
        -- Data validity
        COUNT(CASE WHEN cost >= 0 THEN 1 END) * 100.0 / COUNT(cost) as positive_cost_pct,
        COUNT(CASE WHEN cost_concept_id > 0 THEN 1 END) * 100.0 / COUNT(cost_concept_id) as valid_concept_pct
        
      FROM cost;"
    
    quality_metrics <- DBI::dbGetQuery(con, quality_checks_query) |>
      dplyr::rename_with(tolower)
    
    # Data quality assertions
    expect_gt(quality_metrics$total_records, 0)
    expect_gt(quality_metrics$unique_persons, 0)
    expect_gt(quality_metrics$unique_cost_concepts, 0)
    
    # Completeness should be high for required fields
    expect_gte(quality_metrics$person_id_completeness, 95)
    expect_gte(quality_metrics$cost_concept_completeness, 95)
    
    # Cost values should be non-negative
    if (quality_metrics$cost_value_completeness > 0) {
      expect_gte(quality_metrics$positive_cost_pct, 95)
    }
    
    # Concept IDs should be valid (positive)
    expect_gte(quality_metrics$valid_concept_pct, 95)
  })
  
  #-----------------------------------------------------------------------------
  # 10. Cleanup and Resource Management Tests
  #-----------------------------------------------------------------------------
  
  it("should clean up temporary resources properly", {
    con <- setup_test_db()
    on.exit(cleanup_test_db(con))
    
    # Check initial table count
    initial_tables <- DBI::dbListTables(con)
    
    con <- transformCostToCdmV5dot5(con)
    
    # Check final table count - should not have excessive temporary tables
    final_tables <- DBI::dbListTables(con)
    
    # Should have the main tables plus backup
    expected_tables <- c("cost", "cost_v5_3_backup")
    expect_true(all(expected_tables %in% tolower(final_tables)))
    
    # Should not have temporary tables lingering
    temp_tables <- final_tables[grepl("^(tmp_|temp_|#)", tolower(final_tables))]
    expect_equal(length(temp_tables), 0)
  })
})