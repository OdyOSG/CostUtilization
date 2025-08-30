library(testthat)
library(dplyr)
library(Andromeda)

#===============================================================================
# Test Suite for resultsFormat.R - FeatureExtraction Integration
#===============================================================================

describe("createCostCovariateData", {
  
  # Helper function to create mock aggregated results
  .createMockAggregatedResults <- function() {
    results <- dplyr::tibble(
      metric_type = c("visit_level", "visit_level", "visit_level", "visit_level", "visit_level"),
      metric_name = c("total_person_days", "total_cost", "cost_pppm", "n_persons_with_cost", "distinct_visits"),
      metric_value = c(985999, 47367.5, 1.46, 18, 18)
    )

    andromeda_obj <- Andromeda::andromeda(
      results = results
    )
    
    return(andromeda_obj)
  }
  
  # Helper function to create mock person-level results
  .createMockPersonLevelResults <- function() {
    results <- dplyr::tibble(
      person_id = c(1001L, 1002L, 1003L, 1004L, 1005L),
      cost = c(1250.75, 890.25, 2100.50, 450.00, 1800.25),
      adjusted_cost = c(1375.83, 979.28, 2310.55, 495.00, 1980.28)
    )

    andromeda_obj <- Andromeda::andromeda(
      results = results
    )
    
    return(andromeda_obj)
  }
  
  # Helper function to create mock settings
  .createMockSettings <- function(cpiAdjustment = FALSE, microCosting = FALSE, hasEventFilters = FALSE) {
    settings <- list(
      costConceptId = 31973L,
      currencyConceptId = 44818668L,
      anchorCol = "cohort_start_date",
      startOffsetDays = 0L,
      endOffsetDays = 365L,
      cpiAdjustment = cpiAdjustment,
      microCosting = microCosting,
      hasEventFilters = hasEventFilters,
      hasVisitRestriction = FALSE,
      nFilters = if (hasEventFilters) 1L else 0L,
      eventFilters = if (hasEventFilters) list(list(name = "Test Filter", domain = "Drug", conceptIds = c(1234L))) else NULL
    )
    class(settings) <- "CostOfCareSettings"
    return(settings)
  }
  
  #-----------------------------------------------------------------------------
  # Format Detection Tests
  #-----------------------------------------------------------------------------
  
  it("should correctly detect aggregated result format", {
    mockResults <- .createMockAggregatedResults()
    mockSettings <- .createMockSettings()
    
    # Test internal function directly
    format <- CostUtilization:::.detectResultFormat(mockResults)
    expect_equal(format, "aggregated")
    
    Andromeda::close(mockResults)
  })
  
  it("should correctly detect person-level result format", {
    mockResults <- .createMockPersonLevelResults()
    mockSettings <- .createMockSettings()
    
    # Test internal function directly
    format <- CostUtilization:::.detectResultFormat(mockResults)
    expect_equal(format, "person_level")
    
    Andromeda::close(mockResults)
  })
  
  #-----------------------------------------------------------------------------
  # Aggregated Results Conversion Tests
  #-----------------------------------------------------------------------------
  
  it("should convert aggregated results to CovariateData format", {
    mockResults <- .createMockAggregatedResults()
    mockSettings <- .createMockSettings()
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB",
      analysisId = 1000L
    )
    
    # Check object structure
    expect_s3_class(covariateData, "CovariateData")
    expect_s3_class(covariateData, "Andromeda")
    
    # Check required tables exist
    expect_true("covariates" %in% names(covariateData))
    expect_true("covariateRef" %in% names(covariateData))
    expect_true("analysisRef" %in% names(covariateData))
    
    # Check covariates structure
    covariates <- covariateData$covariates %>% dplyr::collect()
    expect_true(all(c("rowId", "covariateId", "covariateValue") %in% names(covariates)))
    expect_gt(nrow(covariates), 0)
    
    # Check covariate IDs are systematic
    expect_true(all(covariates$covariateId >= 1000000)) # analysisId * 1000 + offset
    
    # Check metadata
    metaData <- attr(covariateData, "metaData")
    expect_equal(metaData$analysisId, 1000L)
    expect_equal(metaData$cohortId, 1L)
    expect_equal(metaData$databaseId, "TestDB")
    expect_equal(metaData$resultFormat, "aggregated")
    
    Andromeda::close(mockResults)
    Andromeda::close(covariateData)
  })
  
  it("should handle aggregated results with CPI adjustment metadata", {
    mockResults <- .createMockAggregatedResults()
    mockSettings <- .createMockSettings(cpiAdjustment = TRUE)
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    # Check CPI adjustment is recorded in metadata
    metaData <- attr(covariateData, "metaData")
    expect_true(metaData$cpiAdjustment)
    
    Andromeda::close(mockResults)
  })
  
  #-----------------------------------------------------------------------------
  # Person-Level Results Conversion Tests
  #-----------------------------------------------------------------------------
  
  it("should convert person-level results to CovariateData format", {
    mockResults <- .createMockPersonLevelResults()
    mockSettings <- .createMockSettings()
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB",
      analysisId = 2000L
    )
    
    # Check object structure
    expect_s3_class(covariateData, "CovariateData")
    expect_s3_class(covariateData, "Andromeda")
    
    # Check covariates structure
    covariates <- covariateData$covariates %>% dplyr::collect()
    expect_true(all(c("rowId", "covariateId", "covariateValue") %in% names(covariates)))
    
    # Should have entries for both cost and adjusted_cost
    expect_gte(nrow(covariates), 10) # 5 people * 2 cost types
    
    # Check person IDs are preserved as rowIds
    expect_true(all(covariates$rowId %in% c(1001L, 1002L, 1003L, 1004L, 1005L)))
    
    # Check covariate IDs are systematic (2000 * 1000 + 300 + offset)
    expect_true(all(covariates$covariateId >= 2000000))
    
    # Check metadata
    metaData <- attr(covariateData, "metaData")
    expect_equal(metaData$resultFormat, "person_level")
    
    Andromeda::close(mockResults)
    Andromeda::close(covariateData)
  })
  
  it("should handle person-level results without adjusted_cost column", {
    # Create results with only cost column
    results <- dplyr::tibble(
      person_id = c(1001L, 1002L, 1003L),
      cost = c(1250.75, 890.25, 2100.50)
    )
    
    diagnostics <- dplyr::tibble(
      step_name = c("01_cohort_persons"),
      n_persons = c(3),
      n_events = c(3)
    )
    
    mockResults <- Andromeda::andromeda(
      results = results,
      diagnostics = diagnostics
    )
    
    mockSettings <- .createMockSettings()
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    # Should still work with only cost column
    covariates <- covariateData$covariates %>% dplyr::collect()
    expect_gte(nrow(covariates), 3) # At least 3 people with cost values
    
    Andromeda::close(mockResults)
    Andromeda::close(covariateData)
  })
  
  #-----------------------------------------------------------------------------
  # Covariate Reference Tests
  #-----------------------------------------------------------------------------
  
  it("should create proper covariate references", {
    mockResults <- .createMockAggregatedResults()
    mockSettings <- .createMockSettings()
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    covariateRef <- covariateData$covariateRef %>% dplyr::collect()
    
    # Check structure
    expect_true(all(c("covariateId", "covariateName", "analysisId", "conceptId") %in% names(covariateRef)))
    
    # Check covariate names are descriptive
    expect_true(all(stringr::str_detect(covariateRef$covariateName, "Total Charge")))
    expect_true(any(stringr::str_detect(covariateRef$covariateName, "\\(0 to 365 days\\)")))
    
    # Check concept ID is preserved
    expect_true(all(covariateRef$conceptId == 31973L))
    
    Andromeda::close(mockResults)
    Andromeda::close(covariateData)
  })
  
  #-----------------------------------------------------------------------------
  # Analysis Reference Tests
  #-----------------------------------------------------------------------------
  
  it("should create proper analysis references", {
    mockResults <- .createMockAggregatedResults()
    mockSettings <- .createMockSettings(microCosting = TRUE, hasEventFilters = TRUE)
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB",
      analysisId = 1500L
    )
    
    analysisRef <- covariateData$analysisRef %>% dplyr::collect()
    
    # Check structure
    expect_true(all(c("analysisId", "analysisName", "domainId", "startDay", "endDay") %in% names(analysisRef)))
    
    # Check values
    expect_equal(analysisRef$analysisId, 1500L)
    expect_equal(analysisRef$domainId, "Cost")
    expect_equal(analysisRef$startDay, 0L)
    expect_equal(analysisRef$endDay, 365L)
    
    # Check analysis name includes modifiers
    expect_true(stringr::str_detect(analysisRef$analysisName, "Line-Level"))
    expect_true(stringr::str_detect(analysisRef$analysisName, "Event-Filtered"))
    
    Andromeda::close(mockResults)
    Andromeda::close(covariateData)
  })
  
  #-----------------------------------------------------------------------------
  # Metadata Tests
  #-----------------------------------------------------------------------------
  
  it("should create comprehensive metadata", {
    mockResults <- .createMockPersonLevelResults()
    mockSettings <- .createMockSettings(cpiAdjustment = TRUE, hasEventFilters = TRUE)
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 123L,
      databaseId = "MyDatabase",
      analysisId = 5000L
    )
    
    metaData <- attr(covariateData, "metaData")
    
    # Check core metadata
    expect_equal(metaData$analysisId, 5000L)
    expect_equal(metaData$cohortId, 123L)
    expect_equal(metaData$databaseId, "MyDatabase")
    expect_equal(metaData$resultFormat, "person_level")
    
    # Check cost settings
    expect_equal(metaData$costConceptId, 31973L)
    expect_equal(metaData$currencyConceptId, 44818668L)
    expect_true(metaData$cpiAdjustment)
    expect_true(metaData$hasEventFilters)
    expect_equal(metaData$nFilters, 1L)
    
    # Check time window
    expect_equal(metaData$startOffsetDays, 0L)
    expect_equal(metaData$endOffsetDays, 365L)
    
    # Check package info
    expect_true(!is.null(metaData$packageVersion))
    expect_true(!is.null(metaData$creationTime))
    
    Andromeda::close(mockResults)
    Andromeda::close(covariateData)
  })
  
  #-----------------------------------------------------------------------------
  # S3 Methods Tests
  #-----------------------------------------------------------------------------
  
  it("should have working summary method", {
    mockResults <- .createMockAggregatedResults()
    mockSettings <- .createMockSettings()
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    # Test summary method doesn't error
    expect_output(summary(covariateData), "CovariateData Summary")
    expect_output(summary(covariateData), "Analysis Information")
    expect_output(summary(covariateData), "Cost Analysis Settings")
    expect_output(summary(covariateData), "Data Summary")
    
    Andromeda::close(mockResults)
    Andromeda::close(covariateData)
  })
  
  it("should have working print method", {
    mockResults <- .createMockPersonLevelResults()
    mockSettings <- .createMockSettings()
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    # Test print method doesn't error
    expect_output(print(covariateData), "CovariateData object")
    expect_output(print(covariateData), "Tables:")
    
    Andromeda::close(mockResults)
    Andromeda::close(covariateData)
  })
  
  #-----------------------------------------------------------------------------
  # Convenience Function Tests
  #-----------------------------------------------------------------------------
  
  it("should work with convenience function", {
    mockResults <- .createMockAggregatedResults()
    mockSettings <- .createMockSettings()
    
    covariateData <- convertToFeatureExtractionFormat(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    expect_s3_class(covariateData, "CovariateData")
    
    metaData <- attr(covariateData, "metaData")
    expect_equal(metaData$databaseId, "TestDB")
    expect_equal(metaData$analysisId, 1000L) # Default value
    
    Andromeda::close(mockResults)
    Andromeda::close(covariateData)
  })
  
  #-----------------------------------------------------------------------------
  # Error Handling Tests
  #-----------------------------------------------------------------------------
  
  it("should error with invalid input types", {
    mockSettings <- .createMockSettings()
    
    # Test with non-Andromeda object
    expect_error(
      createCostCovariateData(
        costResults = list(results = data.frame()),
        costOfCareSettings = mockSettings,
        cohortId = 1L,
        databaseId = "TestDB"
      ),
      "Andromeda"
    )
    
    # Test with invalid settings
    mockResults <- .createMockAggregatedResults()
    expect_error(
      createCostCovariateData(
        costResults = mockResults,
        costOfCareSettings = list(),
        cohortId = 1L,
        databaseId = "TestDB"
      ),
      "CostOfCareSettings"
    )
    
    Andromeda::close(mockResults)
  })
  
  it("should error with unrecognizable result format", {
    # Create results with unexpected column structure
    results <- dplyr::tibble(
      unexpected_col1 = c(1, 2, 3),
      unexpected_col2 = c("a", "b", "c")
    )
    
    mockResults <- Andromeda::andromeda(results = results)
    mockSettings <- .createMockSettings()
    
    expect_error(
      createCostCovariateData(
        costResults = mockResults,
        costOfCareSettings = mockSettings,
        cohortId = 1L,
        databaseId = "TestDB"
      ),
      "Unable to detect valid result format"
    )
    
    Andromeda::close(mockResults)
  })
  
  #-----------------------------------------------------------------------------
  # Integration Tests
  #-----------------------------------------------------------------------------
  
  it("should handle different cost concept IDs correctly", {
    mockResults <- .createMockAggregatedResults()
    
    # Test different cost concepts
    costConcepts <- c(31973L, 31985L, 31980L, 31981L)
    
    for (conceptId in costConcepts) {
      mockSettings <- .createMockSettings()
      mockSettings$costConceptId <- conceptId
      
      covariateData <- createCostCovariateData(
        costResults = mockResults,
        costOfCareSettings = mockSettings,
        cohortId = 1L,
        databaseId = "TestDB"
      )
      
      # Check concept ID is preserved
      covariateRef <- covariateData$covariateRef %>% dplyr::collect()
      expect_true(all(covariateRef$conceptId == conceptId))
      
      # Check covariate names reflect cost type
      if (conceptId == 31985L) {
        expect_true(any(stringr::str_detect(covariateRef$covariateName, "Total Cost")))
      } else if (conceptId == 31980L) {
        expect_true(any(stringr::str_detect(covariateRef$covariateName, "Paid by Payer")))
      }
      
      Andromeda::close(covariateData)
    }
    
    Andromeda::close(mockResults)
  })
  
  it("should handle large datasets efficiently", {
    # Create larger mock dataset
    n_people <- 1000
    results <- dplyr::tibble(
      person_id = 1:n_people,
      cost = runif(n_people, 100, 5000),
      adjusted_cost = cost * 1.1
    )
    
    diagnostics <- dplyr::tibble(
      step_name = "test",
      n_persons = n_people,
      n_events = n_people
    )
    
    mockResults <- Andromeda::andromeda(
      results = results,
      diagnostics = diagnostics
    )
    
    mockSettings <- .createMockSettings()
    
    # Should handle large dataset without error
    expect_no_error({
      covariateData <- createCostCovariateData(
        costResults = mockResults,
        costOfCareSettings = mockSettings,
        cohortId = 1L,
        databaseId = "TestDB"
      )
    })
    
    # Check results
    covariates <- covariateData$covariates %>% dplyr::collect()
    expect_gte(nrow(covariates), n_people) # At least one covariate per person
    
    Andromeda::close(mockResults)
    Andromeda::close(covariateData)
  })
  
  #-----------------------------------------------------------------------------
  # Memory Management Tests
  #-----------------------------------------------------------------------------
  
  it("should properly manage Andromeda objects", {
    mockResults <- .createMockPersonLevelResults()
    mockSettings <- .createMockSettings()
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    # Should be able to close objects without error
    expect_no_error(Andromeda::close(mockResults))
    expect_no_error(Andromeda::close(covariateData))
  })
  
})
