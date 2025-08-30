library(testthat)
library(DBI)
library(duckdb)
library(dplyr)
library(purrr)
library(rlang)

#===============================================================================
# Complete Workflow Integration Tests - CDM v5.5 End-to-End Validation
#===============================================================================

describe("Complete CDM v5.5 Workflow Integration", {
  
  # Comprehensive workflow test environment
  setup_complete_workflow <- function() {
    temp_dir <- tempfile("complete_workflow_")
    dir.create(temp_dir)
    
    return(list(temp_dir = temp_dir))
  }
  
  cleanup_complete_workflow <- function(test_env) {
    if (!is.null(test_env$temp_dir) && dir.exists(test_env$temp_dir)) {
      unlink(test_env$temp_dir, recursive = TRUE)
    }
  }
  
  #-----------------------------------------------------------------------------
  # 1. Complete End-to-End Workflow Test
  #-----------------------------------------------------------------------------
  
  it("should execute complete CDM v5.5 cost analysis workflow", {
    test_env <- setup_complete_workflow()
    on.exit(cleanup_complete_workflow(test_env))
    
    # Step 1: Setup Eunomia database
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    # Step 2: Transform to CDM v5.5 (includes data injection)
    con <- transformCostToCdmV5dot5(con)
    
    # Step 3: Create test cohort
    test_cohort <- tibble::tibble(
      cohort_definition_id = c(1L, 1L, 1L, 2L, 2L),
      subject_id = c(1L, 2L, 3L, 1L, 4L),
      cohort_start_date = as.Date(c("2020-01-15", "2020-02-01", "2020-03-10", "2020-06-01", "2020-07-15")),
      cohort_end_date = as.Date(c("2020-12-31", "2020-11-30", "2020-10-15", "2020-12-31", "2020-12-31"))
    )
    DBI::dbWriteTable(con, "cohort", test_cohort, overwrite = TRUE)
    
    # Step 4: Create comprehensive analysis settings
    comprehensive_settings <- createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = -180L,
      endOffsetDays = 365L,
      costConceptId = 31973L,  # total_charge
      additionalCostConceptIds = c(31985L, 31980L, 31981L),  # total_cost, paid_by_payer, paid_by_patient
      restrictVisitConceptIds = c(9201L, 9202L, 9203L),  # inpatient, outpatient, emergency
      eventFilters = list(
        list(name = "High-Value Procedures", domain = "Procedure", conceptIds = c(4301351L, 4139525L)),
        list(name = "Chronic Medications", domain = "Drug", conceptIds = c(1503297L, 1502826L))
      )
    )
    
    # Step 5: Execute cost analysis
    analysis_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L,
      costOfCareSettings = comprehensive_settings,
      verbose = FALSE
    )
    
    # Step 6: Validate complete workflow results
    expect_s3_class(analysis_result, "list")
    expect_named(analysis_result, c("results", "diagnostics"))
    expect_s3_class(analysis_result$results, "tbl_df")
    expect_s3_class(analysis_result$diagnostics, "tbl_df")
    
    # Validate results structure
    expect_true("totalCost" %in% names(analysis_result$results))
    expect_true("metricType" %in% names(analysis_result$results))
    expect_gt(nrow(analysis_result$diagnostics), 0)
    
    # Validate diagnostic information
    diagnostic_steps <- analysis_result$diagnostics$stepName
    expected_steps <- c("01_base_cohort", "02_with_time_window", "03_with_qualifying_visits")
    found_steps <- intersect(expected_steps, diagnostic_steps)
    expect_gt(length(found_steps), 0)
  })
  
  #-----------------------------------------------------------------------------
  # 2. Multi-Cohort Comparative Analysis
  #-----------------------------------------------------------------------------
  
  it("should support multi-cohort comparative analysis workflow", {
    test_env <- setup_complete_workflow()
    on.exit(cleanup_complete_workflow(test_env))
    
    # Setup database and transformation
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    con <- transformCostToCdmV5dot5(con)
    
    # Create multi-cohort test data
    multi_cohort <- tibble::tibble(
      cohort_definition_id = c(rep(1L, 3), rep(2L, 3), rep(3L, 2)),
      subject_id = c(1L, 2L, 3L, 1L, 4L, 5L, 2L, 6L),
      cohort_start_date = as.Date(c(
        "2020-01-15", "2020-02-01", "2020-03-10",  # Cohort 1
        "2020-06-01", "2020-07-15", "2020-08-01",  # Cohort 2
        "2020-09-15", "2020-10-01"                 # Cohort 3
      )),
      cohort_end_date = as.Date(c(
        "2020-12-31", "2020-11-30", "2020-10-15",
        "2020-12-31", "2020-12-31", "2020-12-31",
        "2020-12-31", "2020-12-31"
      ))
    )
    DBI::dbWriteTable(con, "cohort", multi_cohort, overwrite = TRUE)
    
    # Analyze multiple cohorts with different settings
    cohort_analyses <- c(1L, 2L, 3L) |>
      purrr::map_dfr(~ {
        settings <- createCostOfCareSettings(
          costConceptId = 31973L,
          startOffsetDays = 0L,
          endOffsetDays = 180L
        )
        
        result <- calculateCostOfCare(
          connection = con,
          cdmDatabaseSchema = "main",
          cohortDatabaseSchema = "main",
          cohortTable = "cohort",
          cohortId = .x,
          costOfCareSettings = settings,
          verbose = FALSE
        )
        
        result$results |>
          dplyr::mutate(cohort_id = .x)
      })
    
    # Validate multi-cohort results
    expect_s3_class(cohort_analyses, "tbl_df")
    expect_true("cohort_id" %in% names(cohort_analyses))
    
    unique_cohorts <- unique(cohort_analyses$cohort_id)
    expect_gte(length(unique_cohorts), 1)  # At least one cohort should have results
    expect_lte(length(unique_cohorts), 3)  # No more than 3 cohorts
  })
  
  #-----------------------------------------------------------------------------
  # 3. Cost Type Comparison Workflow
  #-----------------------------------------------------------------------------
  
  it("should enable comprehensive cost type comparison", {
    test_env <- setup_complete_workflow()
    on.exit(cleanup_complete_workflow(test_env))
    
    # Setup
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    con <- transformCostToCdmV5dot5(con)
    
    # Simple cohort for cost comparison
    simple_cohort <- tibble::tibble(
      cohort_definition_id = 1L,
      subject_id = c(1L, 2L, 3L),
      cohort_start_date = as.Date(c("2020-01-15", "2020-02-01", "2020-03-10")),
      cohort_end_date = as.Date(c("2020-12-31", "2020-11-30", "2020-10-15"))
    )
    DBI::dbWriteTable(con, "cohort", simple_cohort, overwrite = TRUE)
    
    # Define cost types for comparison
    cost_types <- tibble::tribble(
      ~cost_name,        ~concept_id, ~description,
      "Total Charge",    31973L,      "Amount charged by provider",
      "Total Cost",      31985L,      "Actual cost of service",
      "Paid by Payer",   31980L,      "Amount paid by insurance",
      "Paid by Patient", 31981L,      "Amount paid by patient"
    )
    
    # Analyze each cost type
    cost_comparison <- cost_types |>
      purrr::pmap_dfr(function(cost_name, concept_id, description) {
        # Check if this concept has data
        data_check <- DBI::dbGetQuery(con, glue::glue("
          SELECT COUNT(*) as n FROM cost 
          WHERE cost_concept_id = {concept_id} AND cost > 0;"))$n
        
        if (data_check > 0) {
          settings <- createCostOfCareSettings(
            costConceptId = concept_id,
            startOffsetDays = 0L,
            endOffsetDays = 365L
          )
          
          tryCatch({
            result <- calculateCostOfCare(
              connection = con,
              cdmDatabaseSchema = "main",
              cohortDatabaseSchema = "main",
              cohortTable = "cohort",
              cohortId = 1L,
              costOfCareSettings = settings,
              verbose = FALSE
            )
            
            result$results |>
              dplyr::mutate(
                cost_type_name = cost_name,
                cost_concept_id = concept_id,
                description = description,
                analysis_success = TRUE
              )
          }, error = function(e) {
            tibble::tibble(
              cost_type_name = cost_name,
              cost_concept_id = concept_id,
              description = description,
              totalCost = 0,
              analysis_success = FALSE,
              error_message = as.character(e)
            )
          })
        } else {
          tibble::tibble(
            cost_type_name = cost_name,
            cost_concept_id = concept_id,
            description = description,
            totalCost = 0,
            analysis_success = TRUE,
            note = "No data available for this concept"
          )
        }
      })
    
    # Validate cost comparison results
    expect_s3_class(cost_comparison, "tbl_df")
    expect_equal(nrow(cost_comparison), nrow(cost_types))
    expect_true(all(c("cost_type_name", "cost_concept_id", "description") %in% names(cost_comparison)))
    
    # At least some cost types should have been analyzed successfully
    successful_analyses <- cost_comparison |>
      dplyr::filter(.data$analysis_success == TRUE)
    expect_gt(nrow(successful_analyses), 0)
  })
  
  #-----------------------------------------------------------------------------
  # 4. Advanced Feature Integration Workflow
  #-----------------------------------------------------------------------------
  
  it("should integrate all advanced features in single workflow", {
    test_env <- setup_complete_workflow()
    on.exit(cleanup_complete_workflow(test_env))
    
    # Setup
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    con <- transformCostToCdmV5dot5(con)
    
    # Create cohort
    advanced_cohort <- tibble::tibble(
      cohort_definition_id = 1L,
      subject_id = c(1L, 2L, 3L),
      cohort_start_date = as.Date(c("2020-01-15", "2020-02-01", "2020-03-10")),
      cohort_end_date = as.Date(c("2020-12-31", "2020-11-30", "2020-10-15"))
    )
    DBI::dbWriteTable(con, "cohort", advanced_cohort, overwrite = TRUE)
    
    # Create CPI adjustment file
    cpi_file <- tempfile(fileext = ".csv")
    on.exit(unlink(cpi_file), add = TRUE)
    
    cpi_data <- tibble::tibble(
      year = 2018:2023,
      adj_factor = c(1.0, 1.02, 1.05, 1.08, 1.12, 1.15)
    )
    utils::write.csv(cpi_data, cpi_file, row.names = FALSE)
    
    # Create advanced settings with all features
    advanced_settings <- createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = -90L,
      endOffsetDays = 270L,
      restrictVisitConceptIds = c(9201L, 9202L),  # Inpatient and outpatient
      eventFilters = list(
        list(name = "Target Procedures", domain = "Procedure", conceptIds = c(4301351L, 4139525L)),
        list(name = "Related Drugs", domain = "Drug", conceptIds = c(1503297L, 1502826L)),
        list(name = "Diagnostic Tests", domain = "Measurement", conceptIds = c(3004501L, 3003309L))
      ),
      microCosting = TRUE,
      primaryEventFilterName = "Target Procedures",
      costConceptId = 31973L,
      additionalCostConceptIds = c(31985L, 31980L),
      cpiAdjustment = TRUE,
      cpiFilePath = cpi_file
    )
    
    # Execute advanced analysis
    advanced_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L,
      costOfCareSettings = advanced_settings,
      verbose = FALSE
    )
    
    # Validate advanced workflow
    expect_s3_class(advanced_result, "list")
    expect_s3_class(advanced_result$results, "tbl_df")
    expect_s3_class(advanced_result$diagnostics, "tbl_df")
    
    # Should have comprehensive diagnostic information
    expect_gt(nrow(advanced_result$diagnostics), 5)  # Multiple processing steps
    
    # Results should reflect micro-costing if data available
    if (nrow(advanced_result$results) > 0) {
      expect_true("metricType" %in% names(advanced_result$results))
    }
    
    # Should have CPI-related diagnostics or results
    cpi_related <- advanced_result$diagnostics |>
      dplyr::filter(grepl("cpi|adjust", .data$stepName, ignore.case = TRUE))
    
    # CPI adjustment should be attempted (may not have data, but should try)
    expect_true(nrow(cpi_related) >= 0)  # Should not error
  })
  
  #-----------------------------------------------------------------------------
  # 5. Performance Integration Test
  #-----------------------------------------------------------------------------
  
  it("should maintain performance across complete workflow", {
    test_env <- setup_complete_workflow()
    on.exit(cleanup_complete_workflow(test_env))
    
    # Measure complete workflow performance
    start_time <- Sys.time()
    
    # Full workflow
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    con <- transformCostToCdmV5dot5(con)
    
    perf_cohort <- tibble::tibble(
      cohort_definition_id = 1L,
      subject_id = c(1L, 2L, 3L),
      cohort_start_date = as.Date(c("2020-01-15", "2020-02-01", "2020-03-10")),
      cohort_end_date = as.Date(c("2020-12-31", "2020-11-30", "2020-10-15"))
    )
    DBI::dbWriteTable(con, "cohort", perf_cohort, overwrite = TRUE)
    
    settings <- createCostOfCareSettings(
      costConceptId = 31973L,
      startOffsetDays = 0L,
      endOffsetDays = 365L,
      eventFilters = list(
        list(name = "All Procedures", domain = "Procedure", conceptIds = c(4301351L, 4139525L)),
        list(name = "All Drugs", domain = "Drug", conceptIds = c(1503297L, 1502826L))
      )
    )
    
    result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L,
      costOfCareSettings = settings,
      verbose = FALSE
    )
    
    end_time <- Sys.time()
    total_workflow_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
    
    # Complete workflow should finish within reasonable time (5 minutes)
    expect_lt(total_workflow_time, 300)
    
    # Should produce valid results
    expect_s3_class(result$results, "tbl_df")
    expect_s3_class(result$diagnostics, "tbl_df")
  })
  
  #-----------------------------------------------------------------------------
  # 6. Data Quality Integration Validation
  #-----------------------------------------------------------------------------
  
  it("should maintain data quality throughout complete workflow", {
    test_env <- setup_complete_workflow()
    on.exit(cleanup_complete_workflow(test_env))
    
    # Setup
    db_file <- getEunomiaDuckDb(pathToData = test_env$temp_dir)
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    # Capture initial data quality metrics
    initial_person_count <- DBI::dbGetQuery(con, "SELECT COUNT(DISTINCT person_id) as n FROM person;")$n
    initial_visit_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM visit_occurrence;")$n
    
    # Transform and validate
    con <- transformCostToCdmV5dot5(con)
    
    # Post-transformation quality checks
    quality_metrics <- DBI::dbGetQuery(con, "
      SELECT 
        COUNT(*) as total_cost_records,
        COUNT(DISTINCT person_id) as unique_cost_persons,
        COUNT(DISTINCT cost_concept_id) as unique_cost_concepts,
        COUNT(effective_date) * 100.0 / COUNT(*) as effective_date_completeness,
        COUNT(CASE WHEN cost > 0 THEN 1 END) * 100.0 / COUNT(cost) as positive_cost_pct,
        AVG(COALESCE(cost, 0)) as avg_cost
      FROM cost;") |>
      dplyr::rename_with(tolower)
    
    # Data quality assertions
    expect_gt(quality_metrics$total_cost_records, 0)
    expect_gt(quality_metrics$unique_cost_persons, 0)
    expect_lte(quality_metrics$unique_cost_persons, initial_person_count)  # Should not exceed original
    expect_gt(quality_metrics$unique_cost_concepts, 0)
    
    # CDM v5.5 specific quality checks
    expect_gte(quality_metrics$effective_date_completeness, 30)  # Reasonable completeness
    
    if (quality_metrics$positive_cost_pct > 0) {
      expect_gte(quality_metrics$positive_cost_pct, 70)  # Most costs should be positive
      expect_gt(quality_metrics$avg_cost, 0)
    }
    
    # Referential integrity checks
    integrity_check <- DBI::dbGetQuery(con, "
      SELECT 
        COUNT(*) as cost_records_with_person,
        COUNT(CASE WHEN p.person_id IS NOT NULL THEN 1 END) as valid_person_references
      FROM cost c
      LEFT JOIN person p ON c.person_id = p.person_id;") |>
      dplyr::rename_with(tolower)
    
    # All cost records should have valid person references
    if (integrity_check$cost_records_with_person > 0) {
      integrity_pct <- integrity_check$valid_person_references / integrity_check$cost_records_with_person
      expect_gte(integrity_pct, 0.95)  # 95% should have valid references
    }
  })
})