library(testthat)
library(dplyr)
library(Andromeda)

#===============================================================================
# Test Suite for resultsFormat.R - FeatureExtraction Integration
#===============================================================================

describe("createCostCovariateData", {
  
  # Setup test data
  setup_test_data <- function() {
    # Mock aggregated results (as would come from database)
    aggregated_results <- tibble(
      total_person_days = 36500L,
      total_person_months = 1200.0,
      total_person_quarters = 400.0,
      total_person_years = 100.0,
      metric_type = "visit_level",
      total_cost = 50000.0,
      total_adjusted_cost = 55000.0,
      n_persons_with_cost = 85L,
      distinct_visits = 150L,
      distinct_events = 150L,
      cost_pppm = 41.67,
      adjusted_cost_pppm = 45.83,
      cost_pppy = 500.0,
      adjusted_cost_pppy = 550.0,
      events_per_1000_py = 1500.0
    ) |>
      # Transform to metric_type, metric_name, metric_value format
      tidyr::pivot_longer(
        cols = c(total_cost, total_adjusted_cost, n_persons_with_cost, 
                distinct_visits, distinct_events, cost_pppm, adjusted_cost_pppm,
                cost_pppy, adjusted_cost_pppy, events_per_1000_py),
        names_to = "metric_name",
        values_to = "metric_value"
      ) |>
      select(metric_type, metric_name, metric_value)
    
    # Mock person-level results
    person_level_results <- tibble(
      person_id = c(1001L, 1002L, 1003L, 1004L, 1005L),
      cost = c(1250.50, 2100.75, 0.0, 850.25, 3200.00),
      adjusted_cost = c(1375.55, 2310.83, 0.0, 935.28, 3520.00)
    )
    
    # Mock settings
    settings <- list(
      costConceptId = 31973L,
      currencyConceptId = 44818668L,
      startOffsetDays = -365L,
      endOffsetDays = 0L,
      anchorCol = "cohort_start_date",
      cpiAdjustment = TRUE,
      microCosting = FALSE,
      hasVisitRestriction = FALSE,
      hasEventFilters = FALSE,
      eventFilters = NULL,
      restrictVisitConceptIds = NULL
    )
    class(settings) <- "CostOfCareSettings"
    
    # Mock diagnostics
    diagnostics <- tibble(
      step_name = c("00_initial_cohort", "01_person_subset", "02_valid_window", "03_with_qualifying_visits", "04_with_cost"),
      n_persons = c(100L, 100L, 95L, 90L, 85L),
      n_events = c(100L, NA, NA, 150L, 150L)
    )
    
    list(
      aggregated_results = list(results = aggregated_results, diagnostics = diagnostics),
      person_level_results = list(results = person_level_results, diagnostics = diagnostics),
      settings = settings
    )
  }
  
  test_data <- setup_test_data()
  
  #-----------------------------------------------------------------------------
  # Aggregated Results Tests
  #-----------------------------------------------------------------------------
  
  it("should create CovariateData from aggregated results", {
    covariateData <- createCostCovariateData(
      costResults = test_data$aggregated_results,
      costOfCareSettings = test_data$settings,
      cohortId = 1L,
      databaseId = "TestDB",
      analysisId = 1000L,
      aggregated = TRUE
    )
    
    # Check class and structure
    expect_s3_class(covariateData, "CovariateData")
    expect_named(covariateData, c("covariates", "covariateRef", "analysisRef", "metaData"))
    
    # Check covariates table
    covariates <- covariateData$covariates |> collect()
    expect_s3_class(covariates, "tbl_df")
    expect_named(covariates, c("rowId", "covariateId", "covariateValue"))
    expect_equal(unique(covariates$rowId), 1L)  # Should be cohort ID
    expect_true(all(covariates$covariateValue >= 0))
    
    # Check covariate reference
    covariateRef <- covariateData$covariateRef |> collect()
    expect_s3_class(covariateRef, "tbl_df")
    expect_named(covariateRef, c("covariateId", "covariateName", "analysisId", "conceptId"))
    expect_true(all(covariateRef$analysisId == 1000L))
    expect_true(all(covariateRef$conceptId == 31973L))
    
    # Check analysis reference
    analysisRef <- covariateData$analysisRef |> collect()
    expect_s3_class(analysisRef, "tbl_df")
    expect_equal(analysisRef$analysisId, 1000L)
    expect_equal(analysisRef$domainId, "Cost")
    expect_equal(analysisRef$startDay, -365L)
    expect_equal(analysisRef$endDay, 0L)
    
    # Check metadata
    metaData <- attr(covariateData, "metaData")
    expect_type(metaData, "list")
    expect_equal(metaData$cohortId, 1L)
    expect_equal(metaData$databaseId, "TestDB")
    expect_equal(metaData$analysisType, "aggregated")
    expect_equal(metaData$costConceptId, 31973L)
  })
  
  it("should auto-detect aggregated format", {
    covariateData <- createCostCovariateData(
      costResults = test_data$aggregated_results,
      costOfCareSettings = test_data$settings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    metaData <- attr(covariateData, "metaData")
    expect_equal(metaData$analysisType, "aggregated")
  })
  
  #-----------------------------------------------------------------------------
  # Person-Level Results Tests
  #-----------------------------------------------------------------------------
  
  it("should create CovariateData from person-level results", {
    covariateData <- createCostCovariateData(
      costResults = test_data$person_level_results,
      costOfCareSettings = test_data$settings,
      cohortId = 1L,
      databaseId = "TestDB",
      analysisId = 1000L,
      aggregated = FALSE
    )
    
    # Check class and structure
    expect_s3_class(covariateData, "CovariateData")
    expect_named(covariateData, c("covariates", "covariateRef", "analysisRef", "metaData"))
    
    # Check covariates table
    covariates <- covariateData$covariates |> collect()
    expect_s3_class(covariates, "tbl_df")
    expect_named(covariates, c("rowId", "covariateId", "covariateValue"))
    
    # Should have person IDs as rowIds
    expected_persons <- test_data$person_level_results$results |> 
      filter(cost > 0) |> 
      pull(person_id)
    expect_true(all(unique(covariates$rowId) %in% expected_persons))
    
    # Should have multiple covariate types (cost, adjusted_cost, has_cost)
    unique_covariates <- unique(covariates$covariateId)
    expect_gte(length(unique_covariates), 2)  # At least cost and has_cost
    
    # Check covariate reference
    covariateRef <- covariateData$covariateRef |> collect()
    expect_s3_class(covariateRef, "tbl_df")
    expect_true(all(covariateRef$analysisId == 1000L))
    
    # Check metadata
    metaData <- attr(covariateData, "metaData")
    expect_equal(metaData$analysisType, "person_level")
  })
  
  it("should auto-detect person-level format", {
    covariateData <- createCostCovariateData(
      costResults = test_data$person_level_results,
      costOfCareSettings = test_data$settings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    metaData <- attr(covariateData, "metaData")
    expect_equal(metaData$analysisType, "person_level")
  })
  
  it("should handle CPI adjustment in person-level results", {
    covariateData <- createCostCovariateData(
      costResults = test_data$person_level_results,
      costOfCareSettings = test_data$settings,
      cohortId = 1L,
      databaseId = "TestDB",
      aggregated = FALSE
    )
    
    # Should have both cost and adjusted_cost covariates
    covariateRef <- covariateData$covariateRef |> collect()
    covariate_names <- covariateRef$covariateName
    
    expect_true(any(str_detect(covariate_names, "Amount")))
    expect_true(any(str_detect(covariate_names, "CPI-Adjusted")))
  })
  
  it("should handle results without CPI adjustment", {
    # Modify settings to disable CPI adjustment
    settings_no_cpi <- test_data$settings
    settings_no_cpi$cpiAdjustment <- FALSE
    
    # Remove adjusted_cost column
    results_no_cpi <- test_data$person_level_results
    results_no_cpi$results <- results_no_cpi$results |> select(-adjusted_cost)
    
    covariateData <- createCostCovariateData(
      costResults = results_no_cpi,
      costOfCareSettings = settings_no_cpi,
      cohortId = 1L,
      databaseId = "TestDB",
      aggregated = FALSE
    )
    
    # Should not have adjusted_cost covariate
    covariateRef <- covariateData$covariateRef |> collect()
    covariate_names <- covariateRef$covariateName
    
    expect_false(any(str_detect(covariate_names, "CPI-Adjusted")))
  })
  
  #-----------------------------------------------------------------------------
  # Andromeda Integration Tests
  #-----------------------------------------------------------------------------
  
  it("should handle Andromeda input objects", {
    # Create Andromeda object with aggregated results
    andromedaResults <- andromeda()
    andromedaResults$results <- test_data$aggregated_results$results
    
    costResults <- list(
      results = andromedaResults$results,
      diagnostics = test_data$aggregated_results$diagnostics
    )
    
    covariateData <- createCostCovariateData(
      costResults = costResults,
      costOfCareSettings = test_data$settings,
      cohortId = 1L,
      databaseId = "TestDB",
      aggregated = TRUE
    )
    
    expect_s3_class(covariateData, "CovariateData")
    
    # Cleanup
    close(andromedaResults)
  })
  
  #-----------------------------------------------------------------------------
  # Event Filter Integration Tests
  #-----------------------------------------------------------------------------
  
  it("should handle event filters in metadata", {
    # Add event filters to settings
    settings_with_filters <- test_data$settings
    settings_with_filters$hasEventFilters <- TRUE
    settings_with_filters$eventFilters <- list(
      list(name = "Diabetes Diagnoses", domain = "Condition", conceptIds = c(201820L, 443238L)),
      list(name = "Diabetes Medications", domain = "Drug", conceptIds = c(1503297L, 1502826L))
    )
    
    covariateData <- createCostCovariateData(
      costResults = test_data$aggregated_results,
      costOfCareSettings = settings_with_filters,
      cohortId = 1L,
      databaseId = "TestDB",
      aggregated = TRUE
    )
    
    metaData <- attr(covariateData, "metaData")
    expect_length(metaData$eventFilters, 2)
    expect_equal(metaData$eventFilters[[1]]$name, "Diabetes Diagnoses")
    
    # Check that covariate names reflect filtering
    covariateRef <- covariateData$covariateRef |> collect()
    expect_true(any(str_detect(covariateRef$covariateName, "filtered by")))
  })
  
  #-----------------------------------------------------------------------------
  # Error Handling Tests
  #-----------------------------------------------------------------------------
  
  it("should validate input parameters", {
    expect_error(
      createCostCovariateData(
        costResults = list(wrong = "structure"),
        costOfCareSettings = test_data$settings,
        cohortId = 1L,
        databaseId = "TestDB"
      ),
      "must contain 'results' and 'diagnostics'"
    )
    
    expect_error(
      createCostCovariateData(
        costResults = test_data$aggregated_results,
        costOfCareSettings = "not_a_settings_object",
        cohortId = 1L,
        databaseId = "TestDB"
      )
    )
    
    expect_error(
      createCostCovariateData(
        costResults = test_data$aggregated_results,
        costOfCareSettings = test_data$settings,
        cohortId = "not_numeric",
        databaseId = "TestDB"
      )
    )
  })
  
  #-----------------------------------------------------------------------------
  # Cost Concept Integration Tests
  #-----------------------------------------------------------------------------
  
  it("should handle different cost concepts correctly", {
    cost_concepts <- list(
      list(id = 31973L, name = "Total Charge"),
      list(id = 31985L, name = "Total Cost"),
      list(id = 31980L, name = "Paid by Payer"),
      list(id = 31981L, name = "Paid by Patient")
    )
    
    for (concept in cost_concepts) {
      settings <- test_data$settings
      settings$costConceptId <- concept$id
      
      covariateData <- createCostCovariateData(
        costResults = test_data$person_level_results,
        costOfCareSettings = settings,
        cohortId = 1L,
        databaseId = "TestDB",
        aggregated = FALSE
      )
      
      covariateRef <- covariateData$covariateRef |> collect()
      expect_true(any(str_detect(covariateRef$covariateName, concept$name)))
      expect_true(all(covariateRef$conceptId == concept$id))
    }
  })
})

#===============================================================================
# Test Suite for S3 Methods and Utility Functions
#===============================================================================

describe("CovariateData S3 Methods", {
  
  # Setup test CovariateData object
  setup_covariate_data <- function() {
    test_data <- tibble(
      person_id = c(1001L, 1002L, 1003L),
      cost = c(1250.50, 2100.75, 850.25)
    )
    
    settings <- list(
      costConceptId = 31973L,
      startOffsetDays = -365L,
      endOffsetDays = 0L,
      anchorCol = "cohort_start_date",
      cpiAdjustment = FALSE,
      eventFilters = NULL
    )
    class(settings) <- "CostOfCareSettings"
    
    costResults <- list(
      results = test_data,
      diagnostics = tibble(step_name = "test", n_persons = 3L, n_events = 3L)
    )
    
    createCostCovariateData(
      costResults = costResults,
      costOfCareSettings = settings,
      cohortId = 1L,
      databaseId = "TestDB",
      aggregated = FALSE
    )
  }
  
  covariateData <- setup_covariate_data()
  
  it("should have proper class and structure", {
    expect_s3_class(covariateData, "CovariateData")
    expect_true(is.CovariateData(covariateData))
    expect_false(is.CovariateData(list()))
  })
  
  it("should print summary correctly", {
    expect_output(summary(covariateData), "Cost Analysis CovariateData Summary")
    expect_output(summary(covariateData), "Database ID: TestDB")
    expect_output(summary(covariateData), "Analysis Type: person_level")
    expect_output(summary(covariateData), "Cost Concept ID: 31973")
  })
  
  it("should print object correctly", {
    expect_output(print(covariateData), "CovariateData object")
    expect_output(print(covariateData), "Type: Cost Analysis")
    expect_output(print(covariateData), "Database: TestDB")
  })
  
  it("should extract covariate values for specific people", {
    values <- getCovariateValues(covariateData, c(1001L, 1002L))
    
    expect_s3_class(values, "tbl_df")
    expect_named(values, c("rowId", "covariateId", "covariateValue", "covariateName"))
    expect_true(all(values$rowId %in% c(1001L, 1002L)))
  })
  
  it("should convert to wide format", {
    wide_data <- toWideFormat(covariateData)
    
    expect_s3_class(wide_data, "tbl_df")
    expect_true("rowId" %in% names(wide_data))
    
    # Should have covariate columns
    covariate_cols <- names(wide_data)[names(wide_data) != "rowId"]
    expect_gte(length(covariate_cols), 1)
  })
})

#===============================================================================
# Test Suite for Convenience Functions
#===============================================================================

describe("Convenience Functions", {
  
  it("should work with convertToFeatureExtractionFormat", {
    test_data <- tibble(
      person_id = c(1001L, 1002L),
      cost = c(1250.50, 2100.75)
    )
    
    settings <- list(
      costConceptId = 31973L,
      startOffsetDays = 0L,
      endOffsetDays = 365L,
      cpiAdjustment = FALSE
    )
    class(settings) <- "CostOfCareSettings"
    
    costResults <- list(
      results = test_data,
      diagnostics = tibble(step_name = "test", n_persons = 2L, n_events = 2L)
    )
    
    covariateData <- convertToFeatureExtractionFormat(
      costResults = costResults,
      costOfCareSettings = settings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    expect_s3_class(covariateData, "CovariateData")
  })
})

#===============================================================================
# Integration Tests with Real Database Structure
#===============================================================================

describe("Database Integration", {
  
  it("should handle realistic database result structures", {
    # Simulate realistic aggregated results as they would come from the SQL
    realistic_aggregated <- tibble(
      total_person_days = 36500L,
      total_person_months = 1200.0,
      total_person_quarters = 400.0,
      total_person_years = 100.0,
      metric_type = "visit_level",
      total_cost = 50000.0,
      total_adjusted_cost = 55000.0,
      n_persons_with_cost = 85L,
      distinct_visits = 150L,
      distinct_events = 150L,
      cost_pppm = 41.67,
      adjusted_cost_pppm = 45.83,
      cost_pppq = 125.0,
      adjusted_cost_pppq = 137.5,
      cost_pppy = 500.0,
      adjusted_cost_pppy = 550.0,
      events_per_1000_py = 1500.0
    )
    
    settings <- list(
      costConceptId = 31973L,
      currencyConceptId = 44818668L,
      startOffsetDays = -365L,
      endOffsetDays = 0L,
      anchorCol = "cohort_start_date",
      cpiAdjustment = TRUE,
      microCosting = FALSE,
      hasVisitRestriction = FALSE,
      hasEventFilters = FALSE
    )
    class(settings) <- "CostOfCareSettings"
    
    diagnostics <- tibble(
      step_name = c("00_initial_cohort", "01_person_subset", "02_valid_window", 
                   "03_with_qualifying_visits", "04_with_cost", "99_completed"),
      n_persons = c(100L, 100L, 95L, 90L, 85L, NA),
      n_events = c(100L, NA, NA, 150L, 150L, NA)
    )
    
    costResults <- list(results = realistic_aggregated, diagnostics = diagnostics)
    
    # Test that it can handle the realistic structure
    expect_no_error({
      covariateData <- createCostCovariateData(
        costResults = costResults,
        costOfCareSettings = settings,
        cohortId = 1L,
        databaseId = "RealDB",
        aggregated = TRUE
      )
    })
  })
  
  it("should handle micro-costing results", {
    # Simulate line-level results
    line_level_results <- tibble(
      total_person_days = 36500L,
      total_person_months = 1200.0,
      total_person_quarters = 400.0,
      total_person_years = 100.0,
      metric_type = "line_level",
      total_cost = 75000.0,
      total_adjusted_cost = 82500.0,
      n_persons_with_cost = 85L,
      distinct_visits = -1L,  # Not applicable for line-level
      distinct_events = 250L,  # Visit details
      cost_pppm = 62.5,
      adjusted_cost_pppm = 68.75,
      cost_pppy = 750.0,
      adjusted_cost_pppy = 825.0,
      events_per_1000_py = 2500.0
    )
    
    settings <- list(
      costConceptId = 31973L,
      microCosting = TRUE,
      cpiAdjustment = TRUE,
      hasEventFilters = TRUE,
      eventFilters = list(
        list(name = "Primary Filter", domain = "Procedure", conceptIds = c(123L, 456L))
      ),
      primaryEventFilterName = "Primary Filter"
    )
    class(settings) <- "CostOfCareSettings"
    
    costResults <- list(
      results = line_level_results,
      diagnostics = tibble(step_name = "test", n_persons = 85L, n_events = 250L)
    )
    
    expect_no_error({
      covariateData <- createCostCovariateData(
        costResults = costResults,
        costOfCareSettings = settings,
        cohortId = 1L,
        databaseId = "MicroCostDB",
        aggregated = TRUE
      )
    })
    
    # Check that micro-costing is reflected in metadata
    metaData <- attr(covariateData, "metaData")
    expect_true(metaData$microCosting)
  })
})

#===============================================================================
# Performance and Memory Tests
#===============================================================================

describe("Performance and Memory", {
  
  it("should handle large person-level datasets efficiently", {
    # Create a larger dataset
    n_people <- 10000
    large_results <- tibble(
      person_id = 1:n_people,
      cost = runif(n_people, 0, 5000),
      adjusted_cost = runif(n_people, 0, 5500)
    )
    
    settings <- list(
      costConceptId = 31973L,
      cpiAdjustment = TRUE
    )
    class(settings) <- "CostOfCareSettings"
    
    costResults <- list(
      results = large_results,
      diagnostics = tibble(step_name = "test", n_persons = n_people, n_events = n_people)
    )
    
    # Should complete without error and in reasonable time
    start_time <- Sys.time()
    
    covariateData <- createCostCovariateData(
      costResults = costResults,
      costOfCareSettings = settings,
      cohortId = 1L,
      databaseId = "LargeDB",
      aggregated = FALSE
    )
    
    end_time <- Sys.time()
    processing_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
    
    expect_s3_class(covariateData, "CovariateData")
    expect_lt(processing_time, 10)  # Should complete within 10 seconds
    
    # Check that all people with non-zero costs are included
    covariates <- covariateData$covariates |> collect()
    people_with_costs <- large_results |> filter(cost > 0) |> nrow()
    expect_gte(nrow(covariates), people_with_costs)
  })
})