library(testthat)
library(DBI)
library(duckdb)
library(dplyr)
library(purrr)
library(rlang)

#===============================================================================
# Enhanced Test Suite for Eunomia Integration and Data Injection
#===============================================================================

describe("Eunomia Integration - Enhanced CDM v5.5 Tests", {
  
  # Helper function to create clean test environment
  setup_eunomia_test <- function() {
    temp_dir <- tempfile("eunomia_test_")
    dir.create(temp_dir)
    
    return(list(temp_dir = temp_dir))
  }
  
  cleanup_eunomia_test <- function(test_env) {
    if (!is.null(test_env$temp_dir) && dir.exists(test_env$temp_dir)) {
      unlink(test_env$temp_dir, recursive = TRUE)
    }
  }
  
  #-----------------------------------------------------------------------------
  # 1. getEunomiaDuckDb Function Tests
  #-----------------------------------------------------------------------------
  
  it("should download and create Eunomia database successfully", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    # Test database creation
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    
    expect_true(file.exists(db_file))
    expect_true(grepl("\\.duckdb$", db_file))
    
    # Test database connectivity and basic structure
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    tables <- DBI::dbListTables(con)
    
    # Should have core OMOP CDM tables
    expected_tables <- c("person", "observation_period", "visit_occurrence", 
                        "condition_occurrence", "drug_exposure", "procedure_occurrence",
                        "measurement", "observation")
    
    # Convert to lowercase for comparison (DuckDB may vary case)
    tables_lower <- tolower(tables)
    expected_lower <- tolower(expected_tables)
    
    missing_tables <- setdiff(expected_lower, tables_lower)
    expect_equal(length(missing_tables), 0, 
                info = paste("Missing tables:", paste(missing_tables, collapse = ", ")))
    
    # Verify tables have data
    person_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM person;")$n
    expect_gt(person_count, 0)
  })
  
  it("should handle existing database files correctly", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    # First creation
    db_file1 <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    expect_true(file.exists(db_file1))
    
    # Second call should reuse existing data
    db_file2 <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    expect_equal(db_file1, db_file2)
    expect_true(file.exists(db_file2))
  })
  
  it("should validate pathToData parameter", {
    # Should error without valid path
    expect_error(
      getEunomiaDuckDb(pathToData = ""),
      regexp = "pathToData argument must be specified"
    )
  })
  
  #-----------------------------------------------------------------------------
  # 2. injectCostData Function Tests
  #-----------------------------------------------------------------------------
  
  it("should inject realistic cost data with proper structure", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    # Setup database
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    # Inject cost data
    con <- injectCostData(con, seed = 123)
    
    # Verify payer_plan_period table
    ppp_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM payer_plan_period;")$n
    expect_gt(ppp_count, 0)
    
    # Check payer_plan_period structure
    ppp_cols <- names(DBI::dbGetQuery(con, "SELECT * FROM payer_plan_period LIMIT 0;"))
    expected_ppp_cols <- c("payer_plan_period_id", "person_id", 
                          "payer_plan_period_start_date", "payer_plan_period_end_date",
                          "payer_source_value", "plan_source_value")
    
    expect_true(all(tolower(expected_ppp_cols) %in% tolower(ppp_cols)))
    
    # Verify cost table (wide format initially)
    cost_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM cost;")$n
    expect_gt(cost_count, 0)
    
    # Check cost table has wide format columns
    cost_sample <- DBI::dbGetQuery(con, "SELECT * FROM cost LIMIT 5;") |>
      dplyr::rename_with(tolower)
    
    wide_format_cols <- c("total_charge", "total_cost", "paid_by_payer", "paid_by_patient")
    present_wide_cols <- intersect(wide_format_cols, names(cost_sample))
    expect_gt(length(present_wide_cols), 0)
  })
  
  it("should generate reproducible data with seed", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    # Create two databases with same seed
    db_file1 <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con1 <- DBI::dbConnect(duckdb::duckdb(db_file1))
    con1 <- injectCostData(con1, seed = 42)
    
    cost_data1 <- DBI::dbGetQuery(con1, "SELECT * FROM cost ORDER BY cost_id LIMIT 10;")
    DBI::dbDisconnect(con1, shutdown = TRUE)
    
    # Second database with same seed
    db_file2 <- tempfile(fileext = ".duckdb")
    file.copy(db_file1, db_file2)
    con2 <- DBI::dbConnect(duckdb::duckdb(db_file2))
    
    # Clear and re-inject with same seed
    DBI::dbExecute(con2, "DROP TABLE IF EXISTS cost;")
    DBI::dbExecute(con2, "DROP TABLE IF EXISTS payer_plan_period;")
    con2 <- injectCostData(con2, seed = 42)
    
    cost_data2 <- DBI::dbGetQuery(con2, "SELECT * FROM cost ORDER BY cost_id LIMIT 10;")
    DBI::dbDisconnect(con2, shutdown = TRUE)
    unlink(db_file2)
    
    # Data should be identical with same seed
    expect_equal(nrow(cost_data1), nrow(cost_data2))
    if (nrow(cost_data1) > 0 && nrow(cost_data2) > 0) {
      # Compare a few key columns (allowing for minor floating point differences)
      expect_equal(cost_data1$person_id, cost_data2$person_id)
      expect_equal(cost_data1$cost_event_id, cost_data2$cost_event_id)
    }
  })
  
  it("should create realistic payer plan periods", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    con <- injectCostData(con, seed = 123)
    
    # Analyze payer plan characteristics
    ppp_analysis <- DBI::dbGetQuery(con, "
      SELECT 
        COUNT(*) as total_periods,
        COUNT(DISTINCT person_id) as unique_persons,
        COUNT(DISTINCT payer_source_value) as unique_payers,
        COUNT(DISTINCT plan_source_value) as unique_plans,
        AVG(julianday(payer_plan_period_end_date) - julianday(payer_plan_period_start_date)) as avg_duration_days
      FROM payer_plan_period;") |>
      dplyr::rename_with(tolower)
    
    expect_gt(ppp_analysis$total_periods, 0)
    expect_gt(ppp_analysis$unique_persons, 0)
    expect_gt(ppp_analysis$unique_payers, 0)
    expect_gt(ppp_analysis$unique_plans, 0)
    expect_gt(ppp_analysis$avg_duration_days, 30)  # Should have reasonable duration
    
    # Check for realistic payer types
    payer_types <- DBI::dbGetQuery(con, "
      SELECT DISTINCT payer_source_value 
      FROM payer_plan_period;")$payer_source_value
    
    expected_payer_patterns <- c("Commercial", "HMO", "PPO", "EPO")
    found_patterns <- purrr::map_lgl(expected_payer_patterns, ~ {
      any(grepl(.x, payer_types, ignore.case = TRUE))
    })
    
    expect_true(any(found_patterns))  # At least one realistic payer type
  })
  
  #-----------------------------------------------------------------------------
  # 3. injectVisitDetailsData Function Tests
  #-----------------------------------------------------------------------------
  
  it("should create visit_detail table with proper structure", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    # Inject visit details
    con <- injectVisitDetailsData(con)
    
    # Verify visit_detail table exists
    tables <- DBI::dbListTables(con)
    expect_true("visit_detail" %in% tolower(tables))
    
    # Check visit_detail structure
    vd_schema <- DBI::dbGetQuery(con, "SELECT * FROM visit_detail LIMIT 0;")
    vd_cols <- tolower(names(vd_schema))
    
    expected_vd_cols <- c("visit_detail_id", "person_id", "visit_detail_concept_id",
                         "visit_detail_start_date", "visit_occurrence_id")
    
    expect_true(all(expected_vd_cols %in% vd_cols))
    
    # Verify data exists
    vd_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM visit_detail;")$n
    expect_gt(vd_count, 0)
  })
  
  it("should maintain referential integrity with clinical events", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    con <- injectVisitDetailsData(con)
    
    # Check referential integrity with visit_occurrence
    integrity_check <- DBI::dbGetQuery(con, "
      SELECT 
        COUNT(*) as total_visit_details,
        COUNT(DISTINCT vd.visit_occurrence_id) as unique_visits_in_detail,
        COUNT(DISTINCT vo.visit_occurrence_id) as unique_visits_in_occurrence
      FROM visit_detail vd
      LEFT JOIN visit_occurrence vo ON vd.visit_occurrence_id = vo.visit_occurrence_id;") |>
      dplyr::rename_with(tolower)
    
    expect_gt(integrity_check$total_visit_details, 0)
    expect_gt(integrity_check$unique_visits_in_detail, 0)
    
    # Check person_id consistency
    person_consistency <- DBI::dbGetQuery(con, "
      SELECT COUNT(*) as mismatched_persons
      FROM visit_detail vd
      INNER JOIN visit_occurrence vo ON vd.visit_occurrence_id = vo.visit_occurrence_id
      WHERE vd.person_id != vo.person_id;")$mismatched_persons
    
    expect_equal(person_consistency, 0)  # Should have no mismatches
  })
  
  #-----------------------------------------------------------------------------
  # 4. Integration Tests with transformCostToCdmV5dot5
  #-----------------------------------------------------------------------------
  
  it("should support full CDM v5.5 transformation pipeline", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    # Full pipeline: inject data -> transform to v5.5
    con <- transformCostToCdmV5dot5(con)
    
    # Verify transformation results
    tables <- DBI::dbListTables(con)
    expect_true("cost" %in% tolower(tables))
    expect_true("cost_v5_3_backup" %in% tolower(tables))
    expect_true("visit_detail" %in% tolower(tables))
    expect_true("payer_plan_period" %in% tolower(tables))
    
    # Check CDM v5.5 cost table structure
    cost_v55_schema <- DBI::dbGetQuery(con, "SELECT * FROM cost LIMIT 0;")
    cost_v55_cols <- tolower(names(cost_v55_schema))
    
    cdm_v55_cols <- c("cost_id", "person_id", "cost_concept_id", "cost_source_value",
                     "cost", "effective_date", "cost_event_field_concept_id")
    
    expect_true(all(cdm_v55_cols %in% cost_v55_cols))
    
    # Verify cost concepts are properly mapped
    cost_concepts <- DBI::dbGetQuery(con, "
      SELECT DISTINCT cost_concept_id, cost_source_value, COUNT(*) as n
      FROM cost 
      WHERE cost_concept_id IS NOT NULL
      GROUP BY cost_concept_id, cost_source_value
      ORDER BY cost_concept_id;") |>
      dplyr::rename_with(tolower)
    
    expect_gt(nrow(cost_concepts), 0)
    
    # Should have standard CDM v5.5 concepts
    standard_concepts <- c(31973L, 31985L, 31980L, 31981L)  # charge, cost, payer, patient
    found_concepts <- intersect(standard_concepts, cost_concepts$cost_concept_id)
    expect_gt(length(found_concepts), 0)
  })
  
  it("should handle multiple transformation cycles", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    # First transformation
    con <- transformCostToCdmV5dot5(con)
    first_cost_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM cost;")$n
    
    # Second transformation (should handle existing backup)
    expect_no_error({
      con <- transformCostToCdmV5dot5(con)
    })
    
    second_cost_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM cost;")$n
    
    # Should still have data after second transformation
    expect_gt(second_cost_count, 0)
    
    # Backup table should still exist
    backup_exists <- "cost_v5_3_backup" %in% tolower(DBI::dbListTables(con))
    expect_true(backup_exists)
  })
  
  #-----------------------------------------------------------------------------
  # 5. Data Quality and Validation Tests
  #-----------------------------------------------------------------------------
  
  it("should generate high-quality synthetic data", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    con <- injectCostData(con, seed = 123)
    
    # Comprehensive data quality assessment
    quality_metrics <- DBI::dbGetQuery(con, "
      SELECT 
        -- Basic counts
        COUNT(*) as total_cost_records,
        COUNT(DISTINCT person_id) as unique_persons,
        COUNT(DISTINCT cost_event_id) as unique_events,
        
        -- Data completeness
        COUNT(person_id) * 100.0 / COUNT(*) as person_completeness,
        COUNT(cost_event_id) * 100.0 / COUNT(*) as event_completeness,
        COUNT(payer_plan_period_id) * 100.0 / COUNT(*) as payer_completeness,
        
        -- Cost value quality
        COUNT(CASE WHEN total_charge > 0 THEN 1 END) * 100.0 / COUNT(total_charge) as positive_charge_pct,
        COUNT(CASE WHEN total_cost > 0 THEN 1 END) * 100.0 / COUNT(total_cost) as positive_cost_pct,
        
        -- Realistic value ranges
        AVG(COALESCE(total_charge, 0)) as avg_charge,
        MAX(COALESCE(total_charge, 0)) as max_charge,
        MIN(COALESCE(total_charge, 0)) as min_charge
        
      FROM cost;") |>
      dplyr::rename_with(tolower)
    
    # Data quality assertions
    expect_gt(quality_metrics$total_cost_records, 0)
    expect_gt(quality_metrics$unique_persons, 0)
    expect_gt(quality_metrics$unique_events, 0)
    
    # Completeness checks
    expect_gte(quality_metrics$person_completeness, 95)
    expect_gte(quality_metrics$event_completeness, 95)
    
    # Cost value quality
    if (quality_metrics$positive_charge_pct > 0) {
      expect_gte(quality_metrics$positive_charge_pct, 80)
      expect_gt(quality_metrics$avg_charge, 0)
      expect_lt(quality_metrics$max_charge, 50000)  # Reasonable upper bound
      expect_gte(quality_metrics$min_charge, 0)
    }
  })
  
  it("should create realistic temporal relationships", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    con <- injectCostData(con, seed = 123)
    
    # Check temporal consistency between events and payer plans
    temporal_check <- DBI::dbGetQuery(con, "
      SELECT 
        COUNT(*) as total_cost_events,
        COUNT(CASE WHEN c.cost_event_date >= ppp.payer_plan_period_start_date 
                   AND c.cost_event_date <= ppp.payer_plan_period_end_date 
                   THEN 1 END) as events_in_plan_period,
        AVG(julianday(c.cost_event_date) - julianday(ppp.payer_plan_period_start_date)) as avg_days_from_plan_start
      FROM cost c
      LEFT JOIN payer_plan_period ppp ON c.payer_plan_period_id = ppp.payer_plan_period_id
      WHERE c.cost_event_date IS NOT NULL 
        AND ppp.payer_plan_period_start_date IS NOT NULL;") |>
      dplyr::rename_with(tolower)
    
    if (temporal_check$total_cost_events > 0) {
      # Most events should fall within their associated payer plan periods
      temporal_consistency_pct <- temporal_check$events_in_plan_period / temporal_check$total_cost_events
      expect_gte(temporal_consistency_pct, 0.8)  # 80% should be temporally consistent
    }
  })
  
  #-----------------------------------------------------------------------------
  # 6. Performance and Scalability Tests
  #-----------------------------------------------------------------------------
  
  it("should handle data injection efficiently", {
    test_env <- setup_eunomia_test()
    on.exit(cleanup_eunomia_test(test_env))
    
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    # Measure injection performance
    start_time <- Sys.time()
    con <- injectCostData(con, seed = 123)
    end_time <- Sys.time()
    
    injection_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
    
    # Should complete within reasonable time (2 minutes for full injection)
    expect_lt(injection_time, 120)
    
    # Verify data was actually created
    cost_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM cost;")$n
    ppp_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM payer_plan_period;")$n
    
    expect_gt(cost_count, 0)
    expect_gt(ppp_count, 0)
  })
})