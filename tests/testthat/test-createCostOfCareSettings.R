# tests/testthat/test-createCostOfCareSettings.R

test_that("createCostOfCareSettings creates valid settings object", {
  # Test basic settings creation
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365
  )
  
  expect_s3_class(settings, "CostOfCareSettings")
  expect_equal(settings$anchorCol, "cohort_start_date")
  expect_equal(settings$startOffsetDays, 0)
  expect_equal(settings$endOffsetDays, 365)
  expect_false(settings$hasVisitRestriction)
  expect_false(settings$hasEventFilters)
  expect_false(settings$microCosting)
})

test_that("createCostOfCareSettings validates anchor column", {
  # Valid anchor columns
  expect_no_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365
    )
  )
  
  expect_no_error(
    createCostOfCareSettings(
      anchorCol = "cohort_end_date",
      startOffsetDays = 0,
      endOffsetDays = 365
    )
  )
  
  # Invalid anchor column
  expect_error(
    createCostOfCareSettings(
      anchorCol = "invalid_column",
      startOffsetDays = 0,
      endOffsetDays = 365
    ),
    "Must be element of set"
  )
})

test_that("createCostOfCareSettings validates time window", {
  # Valid time windows
  expect_no_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = -365,
      endOffsetDays = 365
    )
  )
  
  # Invalid: end before start
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 100,
      endOffsetDays = 50
    ),
    "endOffsetDays.*must be greater than.*startOffsetDays"
  )
  
  # Invalid: non-integer values
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 1.5,
      endOffsetDays = 365
    )
  )
})

test_that("createCostOfCareSettings handles visit restrictions", {
  # Valid visit restrictions
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    restrictVisitConceptIds = c(9201, 9203)
  )
  
  expect_true(settings$hasVisitRestriction)
  expect_equal(settings$restrictVisitConceptIds, c(9201, 9203))
  
  # Invalid: non-integer concept IDs
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      restrictVisitConceptIds = c("invalid", "concepts")
    )
  )
  
  # Invalid: negative concept IDs
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      restrictVisitConceptIds = c(-1, 0)
    )
  )
})

test_that("createCostOfCareSettings handles event filters", {
  # Valid event filters
  eventFilters <- list(
    list(
      name = "Diabetes Drugs",
      domain = "Drug",
      conceptIds = c(1503297, 1502826)
    ),
    list(
      name = "Diabetes Conditions",
      domain = "Condition",
      conceptIds = c(201820, 201826)
    )
  )
  
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    eventFilters = eventFilters
  )
  
  expect_true(settings$hasEventFilters)
  expect_equal(settings$nFilters, 2)
  expect_equal(settings$eventFilters, eventFilters)
  
  # Invalid: missing required fields
  invalidFilters <- list(
    list(
      name = "Missing Domain",
      conceptIds = c(1234)
    )
  )
  
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = invalidFilters
    )
  )
})

test_that("createCostOfCareSettings handles micro-costing", {
  # Valid micro-costing setup
  eventFilters <- list(
    list(
      name = "Primary Events",
      domain = "Procedure",
      conceptIds = c(4336464)
    )
  )
  
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 90,
    eventFilters = eventFilters,
    microCosting = TRUE,
    primaryEventFilterName = "Primary Events"
  )
  
  expect_true(settings$microCosting)
  expect_equal(settings$primaryEventFilterName, "Primary Events")
  
  # Invalid: micro-costing without primary filter
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 90,
      microCosting = TRUE
    )
  )
  
  # Invalid: primary filter name not in event filters
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 90,
      eventFilters = eventFilters,
      microCosting = TRUE,
      primaryEventFilterName = "Non-existent Filter"
    ),
    "primaryEventFilterName.*must match a name in.*eventFilters"
  )
})

test_that("createCostOfCareSettings handles cost concepts", {
  # Valid cost concepts
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    costConceptId = 31980,
    currencyConceptId = 44818669
  )
  
  expect_equal(settings$costConceptId, 31980)
  expect_equal(settings$currencyConceptId, 44818669)
  
  # Default values
  defaultSettings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365
  )
  
  expect_equal(defaultSettings$costConceptId, 31978)  # Default total charge
  expect_equal(defaultSettings$currencyConceptId, 44818668)  # Default USD
  
  # Invalid: non-integer concept IDs
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      costConceptId = "invalid"
    )
  )
})

test_that("createCostOfCareSettings validates event filter structure", {
  # Test various invalid filter structures
  
  # Missing name
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = list(
        list(
          domain = "Drug",
          conceptIds = c(1234)
        )
      )
    )
  )
  
  # Missing domain
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = list(
        list(
          name = "Test Filter",
          conceptIds = c(1234)
        )
      )
    )
  )
  
  # Missing conceptIds
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = list(
        list(
          name = "Test Filter",
          domain = "Drug"
        )
      )
    )
  )
  
  # Empty conceptIds
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = list(
        list(
          name = "Test Filter",
          domain = "Drug",
          conceptIds = integer(0)
        )
      )
    )
  )
  
  # Non-unique filter names
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = list(
        list(
          name = "Duplicate Name",
          domain = "Drug",
          conceptIds = c(1234)
        ),
        list(
          name = "Duplicate Name",
          domain = "Condition",
          conceptIds = c(5678)
        )
      )
    )
  )
})

test_that("createCostOfCareSettings creates complete settings object", {
  # Create settings with all options
  eventFilters <- list(
    list(
      name = "Test Drugs",
      domain = "Drug",
      conceptIds = c(1118084, 1124300)
    ),
    list(
      name = "Test Procedures",
      domain = "Procedure",
      conceptIds = c(4336464, 4178904)
    )
  )
  
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_end_date",
    startOffsetDays = -90,
    endOffsetDays = 30,
    restrictVisitConceptIds = c(9201, 9203, 9202),
    eventFilters = eventFilters,
    microCosting = TRUE,
    primaryEventFilterName = "Test Procedures",
    costConceptId = 31980,
    currencyConceptId = 44818669
  )
  
  # Verify all fields are set correctly
  expect_equal(settings$anchorCol, "cohort_end_date")
  expect_equal(settings$startOffsetDays, -90)
  expect_equal(settings$endOffsetDays, 30)
  expect_true(settings$hasVisitRestriction)
  expect_equal(length(settings$restrictVisitConceptIds), 3)
  expect_true(settings$hasEventFilters)
  expect_equal(settings$nFilters, 2)
  expect_true(settings$microCosting)
  expect_equal(settings$primaryEventFilterName, "Test Procedures")
  expect_equal(settings$costConceptId, 31980)
  expect_equal(settings$currencyConceptId, 44818669)
  
  # Verify the object has the correct class
  expect_s3_class(settings, "CostOfCareSettings")
})