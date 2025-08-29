library(testthat)

#===============================================================================
# Test Suite for createCostOfCareSettings()
#===============================================================================

describe("createCostOfCareSettings", {
  
  #-----------------------------------------------------------------------------
  # Happy Path Tests: Valid Inputs
  #-----------------------------------------------------------------------------
  
  it("should return the correct default settings when called with no arguments", {
    settings <- createCostOfCareSettings()
    
    # Verify the class
    expect_s3_class(settings, "CostOfCareSettings")
    
    # Verify all default values
    expect_equal(settings$anchorCol, "cohort_start_date")
    expect_equal(settings$startOffsetDays, 0L)
    expect_equal(settings$endOffsetDays, 365L)
    expect_false(settings$hasVisitRestriction)
    expect_null(settings$restrictVisitConceptIds)
    expect_false(settings$hasEventFilters)
    expect_null(settings$eventFilters)
    expect_equal(settings$nFilters, 0L)
    expect_false(settings$microCosting)
    expect_null(settings$primaryEventFilterName)
    expect_equal(settings$costConceptId, 31973L)
    expect_equal(settings$currencyConceptId, 44818668L)
    expect_null(settings$additionalCostConceptIds)
    expect_false(settings$cpiAdjustment)
    expect_null(settings$cpiFilePath)
  })
  
  it("should correctly capture all specified custom parameters", {
    # Create a dummy CPI file for the test
    dummy_cpi_file <- tempfile(fileext = ".csv")
    on.exit(unlink(dummy_cpi_file), add = TRUE) # Ensure cleanup
    write.csv(data.frame(year = 2023, adj_factor = 1.05), dummy_cpi_file, row.names = FALSE)
    
    event_filters <- list(
      list(name = "Drug Exposures", domain = "Drug", conceptIds = c(123, 456)),
      list(name = "Inpatient Visits", domain = "Visit", conceptIds = c(9201))
    )
    
    settings <- createCostOfCareSettings(
      anchorCol = "cohort_end_date",
      startOffsetDays = -180L,
      endOffsetDays = 0L,
      restrictVisitConceptIds = c(9201, 9202),
      eventFilters = event_filters,
      microCosting = TRUE,
      primaryEventFilterName = "Drug Exposures",
      costConceptId = 1001,
      currencyConceptId = 1002,
      additionalCostConceptIds = c(2001, 2002),
      cpiAdjustment = TRUE,
      cpiFilePath = dummy_cpi_file
    )
    
    # Verify all custom values
    expect_equal(settings$anchorCol, "cohort_end_date")
    expect_equal(settings$startOffsetDays, -180L)
    expect_equal(settings$endOffsetDays, 0L)
    expect_true(settings$hasVisitRestriction)
    expect_equal(settings$restrictVisitConceptIds, c(9201, 9202))
    expect_true(settings$hasEventFilters)
    expect_equal(settings$eventFilters, event_filters)
    expect_equal(settings$nFilters, 2L)
    expect_true(settings$microCosting)
    expect_equal(settings$primaryEventFilterName, "Drug Exposures")
    expect_equal(settings$costConceptId, 1001L)
    expect_equal(settings$currencyConceptId, 1002L)
    expect_equal(settings$additionalCostConceptIds, c(2001, 2002))
    expect_true(settings$cpiAdjustment)
    expect_equal(settings$cpiFilePath, dummy_cpi_file)
  })
  
  #-----------------------------------------------------------------------------
  # Sad Path Tests: Invalid Inputs and Edge Cases
  #-----------------------------------------------------------------------------
  
  it("should throw an error for invalid 'anchorCol'", {
    expect_error(createCostOfCareSettings(anchorCol = "invalid_column"))
  })
  
  it("should throw an error if 'endOffsetDays' is not greater than 'startOffsetDays'", {
    expect_error(createCostOfCareSettings(startOffsetDays = 100, endOffsetDays = 100))
    expect_error(createCostOfCareSettings(startOffsetDays = 100, endOffsetDays = 99))
  })
  
  it("should throw an error for malformed 'eventFilters'", {
    # Not a list
    expect_error(createCostOfCareSettings(eventFilters = "not_a_list"))
    # Missing 'name' field
    expect_error(createCostOfCareSettings(eventFilters = list(list(domain = "Drug", conceptIds = c(1)))))
    # Invalid domain
    expect_error(createCostOfCareSettings(eventFilters = list(list(name = "A", domain = "InvalidDomain", conceptIds = c(1)))))
    # Duplicate names
    expect_error(createCostOfCareSettings(eventFilters = list(
      list(name = "Filter1", domain = "Drug", conceptIds = c(1)),
      list(name = "Filter1", domain = "Condition", conceptIds = c(2))
    )))
  })
  
  it("should throw an error if 'microCosting' is TRUE without a valid 'primaryEventFilterName'", {
    event_filters <- list(list(name = "Drug", domain = "Drug", conceptIds = c(1)))
    # Missing primary filter name
    expect_error(createCostOfCareSettings(microCosting = TRUE, eventFilters = event_filters))
    # Primary filter name not found in eventFilters
    expect_error(createCostOfCareSettings(microCosting = TRUE, eventFilters = event_filters, primaryEventFilterName = "NonExistentFilter"))
  })
  
  it("should throw an error if 'cpiAdjustment' is TRUE without a valid 'cpiFilePath'", {
    # Missing file path
    expect_error(createCostOfCareSettings(cpiAdjustment = TRUE, cpiFilePath = NULL))
    # File does not exist
    expect_error(createCostOfCareSettings(cpiAdjustment = TRUE, cpiFilePath = "non_existent_file.csv"))
  })
  
})
