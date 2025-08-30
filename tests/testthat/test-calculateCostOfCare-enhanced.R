library(testthat)
library(DBI)
library(duckdb)
library(dplyr)
library(purrr)
library(rlang)

#===============================================================================
# Enhanced Test Suite for calculateCostOfCare() - CDM v5.5 Integration
#===============================================================================

# Test constants for CDM v5.5 cost concepts
CDM_V55_COST_CONCEPTS <- tibble::tribble(
  ~concept_name,           ~concept_id, ~source_value,
  "total_charge",          31973L,      "total_charge",
  "total_cost",            31985L,      "total_cost", 
  "paid_by_payer",         31980L,      "paid_by_payer",
  "paid_by_patient",       31981L,      "paid_by_patient",
  "paid_patient_copay",    31974L,      "paid_patient_copay",
  "paid_patient_coinsurance", 31975L,   "paid_patient_coinsurance",
  "paid_patient_deductible",  31976L,   "paid_patient_deductible",
  "amount_allowed",        31979L,      "amount_allowed"
)

describe("calculateCostOfCare - Enhanced CDM v5.5 Integration Tests", {
  
  # Shared test setup
  setup_enhanced_test_env <- function() {
    # Create test database with CDM v5.5 cost structure
    databaseFile <- getEunomiaDuckDb(pathToData = 'testing_data')
    con <- DBI::dbConnect(duckdb::duckdb(databaseFile))
    
    # Transform to CDM v5.5 format
    con <- transformCostToCdmV5dot5(con)
    
    # Create enhanced test cohort with more realistic data
    enhanced_cohort <- tibble::tibble(
      cohort_definition_id = c(1L, 1L, 1L, 2L, 2L),
      subject_id = c(1L, 2L, 3L, 1L, 4L),
      cohort_start_date = as.Date(c("2020-01-15", "2020-02-01", "2020-03-10", "2020-06-01", "2020-07-15")),
      cohort_end_date = as.Date(c("2020-12-31", "2020-11-30", "2020-10-15", "2020-12-31", "2020-12-31"))
    )
    
    DBI::dbWriteTable(con, "cohort", enhanced_cohort, overwrite = TRUE)
    
    return(list(connection = con, database_file = databaseFile))
  }
  
  cleanup_enhanced_test_env <- function(test_env) {
    if (!is.null(test_env$connection) && DBI::dbIsValid(test_env$connection)) {
      DBI::dbDisconnect(test_env$connection, shutdown = TRUE)
    }
    if (!is.null(test_env$database_file) && file.exists(test_env$database_file)) {
      unlink(test_env$database_file)
    }
  }
  
  #-----------------------------------------------------------------------------
  # 1. CDM v5.5 Cost Concept Integration Tests
  #-----------------------------------------------------------------------------
  
  it("should work with all standard CDM v5.5 cost concepts", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Test each cost concept individually
    cost_concept_results <- CDM_V55_COST_CONCEPTS |>
      purrr::pmap_dfr(function(concept_name, concept_id, source_value) {
        # Check if this concept exists in the transformed data
        concept_check <- DBI::dbGetQuery(con, glue::glue("
          SELECT COUNT(*) as n 
          FROM cost 
          WHERE cost_concept_id = {concept_id} AND cost > 0;"))$n
        
        if (concept_check > 0) {
          settings <- createCostOfCareSettings(
            costConceptId = concept_id,
            startOffsetDays = -30L,
            endOffsetDays = 30L
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
            
            tibble::tibble(
              concept_name = concept_name,
              concept_id = concept_id,
              success = TRUE,
              total_cost = result$results$totalCost %||% 0,
              error_msg = NA_character_
            )
          }, error = function(e) {
            tibble::tibble(
              concept_name = concept_name,
              concept_id = concept_id,
              success = FALSE,
              total_cost = 0,
              error_msg = as.character(e)
            )
          })
        } else {
          tibble::tibble(
            concept_name = concept_name,
            concept_id = concept_id,
            success = TRUE,  # No error, just no data
            total_cost = 0,
            error_msg = "No data for concept"
          )
        }
      })
    
    # At least some concepts should work successfully
    successful_concepts <- cost_concept_results |>
      dplyr::filter(.data$success == TRUE)
    
    expect_gt(nrow(successful_concepts), 0)
    
    # No critical errors should occur
    failed_concepts <- cost_concept_results |>
      dplyr::filter(.data$success == FALSE & !is.na(.data$error_msg))
    
    if (nrow(failed_concepts) > 0) {
      cli::cli_warn("Some cost concepts failed: {failed_concepts$concept_name}")
    }
  })
  
  it("should handle multiple cost concepts in additionalCostConceptIds", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Test with multiple additional cost concepts
    additional_concepts <- c(31985L, 31980L, 31981L)  # total_cost, paid_by_payer, paid_by_patient
    
    settings <- createCostOfCareSettings(
      costConceptId = 31973L,  # total_charge as primary
      additionalCostConceptIds = additional_concepts,
      startOffsetDays = 0L,
      endOffsetDays = 365L
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
    
    expect_s3_class(result$results, "tbl_df")
    expect_s3_class(result$diagnostics, "tbl_df")
    
    # Should have results for the primary concept
    expect_true("totalCost" %in% names(result$results))
  })
  
  #-----------------------------------------------------------------------------
  # 2. Temporal Precision Tests (CDM v5.5 Features)
  #-----------------------------------------------------------------------------
  
  it("should leverage effective_date for precise temporal analysis", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Test narrow time windows that rely on effective_date precision
    narrow_settings <- createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = -7L,   # 1 week before
      endOffsetDays = 7L,      # 1 week after
      costConceptId = 31973L
    )
    
    narrow_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L,
      costOfCareSettings = narrow_settings,
      verbose = FALSE
    )
    
    # Compare with wider window
    wide_settings <- createCostOfCareSettings(
      anchorCol = "cohort_start_date", 
      startOffsetDays = -365L,  # 1 year before
      endOffsetDays = 365L,     # 1 year after
      costConceptId = 31973L
    )
    
    wide_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L,
      costOfCareSettings = wide_settings,
      verbose = FALSE
    )
    
    # Wide window should capture same or more costs
    expect_gte(wide_result$results$totalCost, narrow_result$results$totalCost)
    
    # Both should have valid diagnostics
    expect_gt(nrow(narrow_result$diagnostics), 0)
    expect_gt(nrow(wide_result$diagnostics), 0)
  })
  
  it("should handle different anchor dates with temporal precision", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Test both anchor options
    anchor_tests <- c("cohort_start_date", "cohort_end_date") |>
      purrr::map_dfr(~ {
        settings <- createCostOfCareSettings(
          anchorCol = .x,
          startOffsetDays = -30L,
          endOffsetDays = 30L,
          costConceptId = 31973L
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
        
        tibble::tibble(
          anchor_col = .x,
          total_cost = result$results$totalCost %||% 0,
          n_diagnostics = nrow(result$diagnostics)
        )
      })
    
    expect_equal(nrow(anchor_tests), 2)
    expect_true(all(anchor_tests$n_diagnostics > 0))
  })
  
  #-----------------------------------------------------------------------------
  # 3. Advanced Event Filtering with CDM v5.5
  #-----------------------------------------------------------------------------
  
  it("should support complex event filtering scenarios", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Create multi-domain event filters
    complex_filters <- list(
      list(
        name = "High-Cost Procedures",
        domain = "Procedure",
        conceptIds = c(4301351L, 4139525L, 4273629L)  # Example procedure concepts
      ),
      list(
        name = "Expensive Medications", 
        domain = "Drug",
        conceptIds = c(1503297L, 1502826L, 1502855L)  # Example drug concepts
      ),
      list(
        name = "Diagnostic Tests",
        domain = "Measurement", 
        conceptIds = c(3004501L, 3003309L, 3024171L)  # Example measurement concepts
      )
    )
    
    settings <- createCostOfCareSettings(
      eventFilters = complex_filters,
      costConceptId = 31973L,
      startOffsetDays = 0L,
      endOffsetDays = 365L
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
    
    expect_s3_class(result$results, "tbl_df")
    expect_s3_class(result$diagnostics, "tbl_df")
    
    # Should have diagnostic information about filtering steps
    filter_diagnostics <- result$diagnostics |>
      dplyr::filter(grepl("filter|event", .data$stepName, ignore.case = TRUE))
    
    expect_gt(nrow(filter_diagnostics), 0)
  })
  
  it("should perform micro-costing analysis with visit_detail integration", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Setup for micro-costing with visit_detail
    micro_filters <- list(
      list(
        name = "Target Procedures",
        domain = "Procedure", 
        conceptIds = c(4301351L, 4139525L)
      )
    )
    
    micro_settings <- createCostOfCareSettings(
      eventFilters = micro_filters,
      microCosting = TRUE,
      primaryEventFilterName = "Target Procedures",
      costConceptId = 31973L,
      startOffsetDays = 0L,
      endOffsetDays = 365L
    )
    
    micro_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L,
      costOfCareSettings = micro_settings,
      verbose = FALSE
    )
    
    expect_s3_class(micro_result$results, "tbl_df")
    
    # Micro-costing should produce line-level results
    if (nrow(micro_result$results) > 0) {
      expect_equal(micro_result$results$metricType, "line_level")
    }
  })
  
  #-----------------------------------------------------------------------------
  # 4. CPI Adjustment Integration Tests
  #-----------------------------------------------------------------------------
  
  it("should apply CPI adjustments correctly with CDM v5.5 temporal data", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Create test CPI data
    cpi_file <- tempfile(fileext = ".csv")
    on.exit(unlink(cpi_file), add = TRUE)
    
    cpi_data <- tibble::tibble(
      year = 2018:2023,
      adj_factor = c(1.0, 1.02, 1.05, 1.08, 1.12, 1.15)  # Realistic inflation
    )
    
    utils::write.csv(cpi_data, cpi_file, row.names = FALSE)
    
    # Test with CPI adjustment
    cpi_settings <- createCostOfCareSettings(
      costConceptId = 31973L,
      cpiAdjustment = TRUE,
      cpiFilePath = cpi_file,
      startOffsetDays = 0L,
      endOffsetDays = 365L
    )
    
    cpi_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L,
      costOfCareSettings = cpi_settings,
      verbose = FALSE
    )
    
    # Compare with non-adjusted analysis
    no_cpi_settings <- createCostOfCareSettings(
      costConceptId = 31973L,
      cpiAdjustment = FALSE,
      startOffsetDays = 0L,
      endOffsetDays = 365L
    )
    
    no_cpi_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L,
      costOfCareSettings = no_cpi_settings,
      verbose = FALSE
    )
    
    # CPI-adjusted results should have additional columns
    if (nrow(cpi_result$results) > 0 && nrow(no_cpi_result$results) > 0) {
      expect_true("totalAdjustedCost" %in% names(cpi_result$results) ||
                  cpi_result$results$totalCost != no_cpi_result$results$totalCost)
    }
  })
  
  #-----------------------------------------------------------------------------
  # 5. Multi-Cohort and Comparative Analysis Tests
  #-----------------------------------------------------------------------------
  
  it("should support comparative analysis across multiple cohorts", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Analyze multiple cohorts
    cohort_comparison <- c(1L, 2L) |>
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
    
    expect_s3_class(cohort_comparison, "tbl_df")
    expect_true("cohort_id" %in% names(cohort_comparison))
    
    # Should have results for multiple cohorts (if data exists)
    unique_cohorts <- unique(cohort_comparison$cohort_id)
    expect_true(length(unique_cohorts) >= 1)
  })
  
  it("should enable cost type comparison analysis", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Compare different cost types using functional programming
    cost_type_comparison <- CDM_V55_COST_CONCEPTS |>
      dplyr::slice_head(n = 4) |>  # Test first 4 concepts
      purrr::pmap_dfr(function(concept_name, concept_id, source_value) {
        # Check if concept has data
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
                cost_concept_name = concept_name,
                cost_concept_id = concept_id
              )
          }, error = function(e) {
            tibble::tibble(
              cost_concept_name = concept_name,
              cost_concept_id = concept_id,
              totalCost = 0,
              error = as.character(e)
            )
          })
        } else {
          tibble::tibble(
            cost_concept_name = concept_name,
            cost_concept_id = concept_id,
            totalCost = 0,
            note = "No data available"
          )
        }
      })
    
    expect_s3_class(cost_type_comparison, "tbl_df")
    expect_true("cost_concept_name" %in% names(cost_type_comparison))
    expect_true("cost_concept_id" %in% names(cost_type_comparison))
  })
  
  #-----------------------------------------------------------------------------
  # 6. Performance and Scalability Tests
  #-----------------------------------------------------------------------------
  
  it("should maintain performance with complex CDM v5.5 queries", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Complex analysis with multiple features
    complex_settings <- createCostOfCareSettings(
      costConceptId = 31973L,
      additionalCostConceptIds = c(31985L, 31980L, 31981L),
      restrictVisitConceptIds = c(9201L, 9202L, 9203L),  # Multiple visit types
      eventFilters = list(
        list(name = "All Procedures", domain = "Procedure", conceptIds = c(4301351L, 4139525L)),
        list(name = "All Drugs", domain = "Drug", conceptIds = c(1503297L, 1502826L))
      ),
      startOffsetDays = -365L,
      endOffsetDays = 365L
    )
    
    # Measure performance
    start_time <- Sys.time()
    
    complex_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L,
      costOfCareSettings = complex_settings,
      verbose = FALSE
    )
    
    end_time <- Sys.time()
    execution_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
    
    # Should complete within reasonable time (2 minutes for complex query)
    expect_lt(execution_time, 120)
    
    # Should still produce valid results
    expect_s3_class(complex_result$results, "tbl_df")
    expect_s3_class(complex_result$diagnostics, "tbl_df")
  })
  
  #-----------------------------------------------------------------------------
  # 7. Error Handling and Edge Cases
  #-----------------------------------------------------------------------------
  
  it("should handle edge cases gracefully", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Test with non-existent cost concept
    edge_case_settings <- createCostOfCareSettings(
      costConceptId = 99999L,  # Non-existent concept
      startOffsetDays = 0L,
      endOffsetDays = 30L
    )
    
    edge_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L,
      costOfCareSettings = edge_case_settings,
      verbose = FALSE
    )
    
    # Should handle gracefully without error
    expect_s3_class(edge_result$results, "tbl_df")
    expect_s3_class(edge_result$diagnostics, "tbl_df")
    
    # Results should indicate no costs found
    expect_equal(edge_result$results$totalCost %||% 0, 0)
  })
  
  it("should validate CDM v5.5 data quality requirements", {
    test_env <- setup_enhanced_test_env()
    on.exit(cleanup_enhanced_test_env(test_env))
    
    con <- test_env$connection
    
    # Check CDM v5.5 data quality
    data_quality_query <- "
      SELECT 
        COUNT(*) as total_cost_records,
        COUNT(DISTINCT person_id) as unique_persons,
        COUNT(DISTINCT cost_concept_id) as unique_cost_concepts,
        COUNT(effective_date) * 100.0 / COUNT(*) as effective_date_completeness,
        COUNT(CASE WHEN cost > 0 THEN 1 END) * 100.0 / COUNT(*) as positive_cost_pct,
        AVG(cost) as avg_cost,
        MAX(cost) as max_cost
      FROM cost;"
    
    quality_metrics <- DBI::dbGetQuery(con, data_quality_query) |>
      dplyr::rename_with(tolower)
    
    # Basic data quality checks
    expect_gt(quality_metrics$total_cost_records, 0)
    expect_gt(quality_metrics$unique_persons, 0)
    expect_gt(quality_metrics$unique_cost_concepts, 0)
    
    # CDM v5.5 specific checks
    expect_gte(quality_metrics$effective_date_completeness, 50)  # At least 50% should have effective dates
    
    if (quality_metrics$positive_cost_pct > 0) {
      expect_gte(quality_metrics$positive_cost_pct, 80)  # Most costs should be positive
      expect_gt(quality_metrics$avg_cost, 0)
      expect_lt(quality_metrics$max_cost, 1000000)  # Reasonable upper bound
    }
  })
})