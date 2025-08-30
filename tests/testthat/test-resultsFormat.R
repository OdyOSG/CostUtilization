# File: tests/testthat/test-resultsFormat.R

library(testthat)
library(dplyr)
library(Andromeda)
library(stringr)

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
    on.exit(Andromeda::close(mockResults), add = TRUE)
    
    format <- CostUtilization:::.detectResultFormat(mockResults)
    expect_equal(format, "aggregated")
  })
  
  it("should correctly detect person-level result format", {
    mockResults <- .createMockPersonLevelResults()
    on.exit(Andromeda::close(mockResults), add = TRUE)
    
    format <- CostUtilization:::.detectResultFormat(mockResults)
    expect_equal(format, "person_level")
  })
  
  #-----------------------------------------------------------------------------
  # Aggregated Results Conversion Tests
  #-----------------------------------------------------------------------------
  
  it("should convert aggregated results to CovariateData format", {
    mockResults <- .createMockAggregatedResults()
    mockSettings <- .createMockSettings()
    on.exit({
      Andromeda::close(mockResults)
      if (exists("covariateData")) Andromeda::close(covariateData)
    }, add = TRUE)
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB",
      analysisId = 1000L
    )
    
    expect_s4_class(covariateData, "Andromeda")
    expect_true(all(c("covariates", "covariateRef", "analysisRef") %in% names(covariateData)))
    
    covariates <- covariateData$covariates %>% dplyr::collect()
    expect_true(all(c("rowId", "covariateId", "covariateValue") %in% names(covariates)))
    expect_gt(nrow(covariates), 0)
    expect_true(all(covariates$covariateId >= 1000000))
    
    metaData <- attr(covariateData, "metaData")
    expect_equal(metaData$analysisId, 1000L)
    expect_equal(metaData$cohortId, 1L)
    expect_equal(metaData$resultFormat, "aggregated")
  })
  
  #-----------------------------------------------------------------------------
  # Person-Level Results Conversion Tests
  #-----------------------------------------------------------------------------
  
  it("should convert person-level results to CovariateData format", {
    mockResults <- .createMockPersonLevelResults()
    mockSettings <- .createMockSettings()
    on.exit({
      Andromeda::close(mockResults)
      if (exists("covariateData")) Andromeda::close(covariateData)
    }, add = TRUE)
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB",
      analysisId = 2000L
    )
    
    expect_s4_class(covariateData, "Andromeda")
    
    covariates <- covariateData$covariates %>% dplyr::collect()
    expect_true(all(c("rowId", "covariateId", "covariateValue") %in% names(covariates)))
    expect_equal(nrow(covariates), 10) # 5 people * 2 cost types
    expect_true(all(covariates$rowId %in% c(1001L, 1002L, 1003L, 1004L, 1005L)))
    expect_true(all(covariates$covariateId >= 2000000))
    
    metaData <- attr(covariateData, "metaData")
    expect_equal(metaData$resultFormat, "person_level")
  })
  
  it("should handle person-level results without adjusted_cost column", {
    results <- dplyr::tibble(
      person_id = c(1001L, 1002L),
      cost = c(1250.75, 890.25)
    )
    mockResults <- Andromeda::andromeda(results = results)
    mockSettings <- .createMockSettings()
    on.exit({
      Andromeda::close(mockResults)
      if (exists("covariateData")) Andromeda::close(covariateData)
    }, add = TRUE)
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    covariates <- covariateData$covariates %>% dplyr::collect()
    expect_equal(nrow(covariates), 2)
  })
  
  #-----------------------------------------------------------------------------
  # Refactoring Validation
  #-----------------------------------------------------------------------------
  
  it("refactored person-level pivot produces correct structure and values", {
    mockResults <- .createMockPersonLevelResults()
    mockSettings <- .createMockSettings()
    on.exit({
      Andromeda::close(mockResults)
      if (exists("covariateData")) Andromeda::close(covariateData)
    }, add = TRUE)
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    
    covariatesTbl <- covariateData$covariates |> dplyr::collect()
    expect_equal(nrow(covariatesTbl), 10)
    
    person1Covs <- covariatesTbl |> dplyr::filter(rowId == 1001)
    expect_equal(nrow(person1Covs), 2)
    
    mockPerson1Data <- mockResults$results |> dplyr::collect() |> dplyr::filter(person_id == 1001)
    expect_true(mockPerson1Data$cost %in% person1Covs$covariateValue)
    expect_true(mockPerson1Data$adjusted_cost %in% person1Covs$covariateValue)
  })
  
  #-----------------------------------------------------------------------------
  # Covariate & Analysis Reference Tests
  #-----------------------------------------------------------------------------
  
  it("should create proper covariate and analysis references", {
    mockResults <- .createMockAggregatedResults()
    mockSettings <- .createMockSettings(microCosting = TRUE, hasEventFilters = TRUE)
    on.exit({
      Andromeda::close(mockResults)
      if (exists("covariateData")) Andromeda::close(covariateData)
    }, add = TRUE)
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB",
      analysisId = 1500L
    )
    
    covariateRef <- covariateData$covariateRef %>% dplyr::collect()
    expect_true(all(c("covariateId", "covariateName", "analysisId", "conceptId") %in% names(covariateRef)))
    expect_true(all(stringr::str_detect(covariateRef$covariateName, "Total Charge")))
    expect_true(all(stringr::str_detect(covariateRef$covariateName, "\\(0 to 365 days\\)")))
    expect_true(all(covariateRef$conceptId == 31973L))
    
    analysisRef <- covariateData$analysisRef %>% dplyr::collect()
    expect_true(all(c("analysisId", "analysisName", "domainId", "startDay", "endDay") %in% names(analysisRef)))
    expect_equal(analysisRef$analysisId, 1500L)
    expect_equal(analysisRef$domainId, "Cost")
    expect_true(stringr::str_detect(analysisRef$analysisName, "Line-Level"))
  })
  
  #-----------------------------------------------------------------------------
  # S4 Methods Tests
  #-----------------------------------------------------------------------------
  
  it("should have a working print method", {
    mockResults <- .createMockPersonLevelResults()
    mockSettings <- .createMockSettings()
    on.exit({
      Andromeda::close(mockResults)
      if (exists("covariateData")) Andromeda::close(covariateData)
    }, add = TRUE)
    
    covariateData <- createCostCovariateData(
      costResults = mockResults,
      costOfCareSettings = mockSettings,
      cohortId = 1L,
      databaseId = "TestDB"
    )
    expect_output(print(covariateData), "Andromeda object")
  })
  
})
