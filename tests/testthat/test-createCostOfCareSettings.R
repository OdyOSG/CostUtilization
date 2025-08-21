# Test createCostOfCareSettings function

test_that("createCostOfCareSettings creates valid settings object", {
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365
  )
  
  expect_s3_class(settings, "CostOfCareSettings")
  expect_type(settings, "list")
  expect_equal(settings$anchorCol, "cohort_start_date")
  expect_equal(settings$startOffsetDays, 0)
  expect_equal(settings$endOffsetDays, 365)
  expect_false(settings$hasVisitRestriction)
  expect_false(settings$hasEventFilters)
  expect_false(settings$microCosting)
  expect_equal(settings$costConceptId, 31978L)
  expect_equal(settings$currencyConceptId, 44818668L)
})

test_that("createCostOfCareSettings validates anchor column", {
  expect_error(
    createCostOfCareSettings(
      anchorCol = "invalid_column",
      startOffsetDays = 0,
      endOffsetDays = 365
    ),
    "Must be element of set"
  )
  
  # Valid anchor columns
  settings1 <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365
  )
  expect_equal(settings1$anchorCol, "cohort_start_date")
  
  settings2 <- createCostOfCareSettings(
    anchorCol = "cohort_end_date",
    startOffsetDays = -365,
    endOffsetDays = 0
  )
  expect_equal(settings2$anchorCol, "cohort_end_date")
})

test_that("createCostOfCareSettings validates offset days", {
  # endOffsetDays must be greater than startOffsetDays
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 100,
      endOffsetDays = 50
    ),
    "endOffsetDays.*must be greater than.*startOffsetDays"
  )
  
  # Equal values should also error
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 100,
      endOffsetDays = 100
    ),
    "endOffsetDays.*must be greater than.*startOffsetDays"
  )
  
  # Negative values are allowed
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = -365,
    endOffsetDays = -1
  )
  expect_equal(settings$startOffsetDays, -365)
  expect_equal(settings$endOffsetDays, -1)
})

test_that("createCostOfCareSettings handles visit restrictions", {
  # No restriction
  settings1 <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    restrictVisitConceptIds = NULL
  )
  expect_false(settings1$hasVisitRestriction)
  expect_null(settings1$restrictVisitConceptIds)
  
  # With restriction
  visitIds <- c(9201, 9203, 9202)
  settings2 <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    restrictVisitConceptIds = visitIds
  )
  expect_true(settings2$hasVisitRestriction)
  expect_equal(settings2$restrictVisitConceptIds, visitIds)
  
  # Invalid visit concept IDs
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      restrictVisitConceptIds = c(0, -1)
    ),
    "Element 2 is not >= 1"
  )
  
  # Duplicate IDs should be rejected
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      restrictVisitConceptIds = c(9201, 9201, 9203)
    ),
    "Contains duplicated values"
  )
})

test_that("createCostOfCareSettings validates event filters", {
  # Valid event filters
  validFilters <- list(
    list(
      name = "Diabetes",
      domain = "Condition",
      conceptIds = c(201820, 201826)
    ),
    list(
      name = "Metformin",
      domain = "Drug",
      conceptIds = c(1503297)
    )
  )
  
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    eventFilters = validFilters
  )
  
  expect_true(settings$hasEventFilters)
  expect_equal(settings$nFilters, 2)
  expect_equal(settings$eventFilters, validFilters)
  
  # Invalid structure - not a list
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = "not a list"
    ),
    "eventFilters.*must be a list"
  )
  
  # Missing required fields
  invalidFilters1 <- list(
    list(
      domain = "Condition",
      conceptIds = c(201820)
    )
  )
  
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = invalidFilters1
    ),
    "must have a 'name' field"
  )
  
  # Invalid domain
  invalidFilters2 <- list(
    list(
      name = "Test",
      domain = "InvalidDomain",
      conceptIds = c(201820)
    )
  )
  
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = invalidFilters2
    ),
    "has invalid domain"
  )
  
  # Duplicate names
  invalidFilters3 <- list(
    list(
      name = "Duplicate",
      domain = "Condition",
      conceptIds = c(201820)
    ),
    list(
      name = "Duplicate",
      domain = "Drug",
      conceptIds = c(1503297)
    )
  )
  
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = invalidFilters3
    ),
    "Event filter names must be unique"
  )
})

test_that("createCostOfCareSettings validates micro-costing parameters", {
  # Micro-costing without event filters should error
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      microCosting = TRUE,
      primaryEventFilterName = "Test"
    ),
    "primaryEventFilterName.*must match a name in.*eventFilters"
  )
  
  # Micro-costing with non-existent filter name
  filters <- list(
    list(
      name = "Filter1",
      domain = "Drug",
      conceptIds = c(1503297)
    )
  )
  
  expect_error(
    createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0,
      endOffsetDays = 365,
      eventFilters = filters,
      microCosting = TRUE,
      primaryEventFilterName = "NonExistent"
    ),
    "primaryEventFilterName.*must match a name in.*eventFilters"
  )
  
  # Valid micro-costing setup
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    eventFilters = filters,
    microCosting = TRUE,
    primaryEventFilterName = "Filter1"
  )
  
  expect_true(settings$microCosting)
  expect_equal(settings$primaryEventFilterName, "Filter1")
})

test_that("createCostOfCareSettings validates concept IDs", {
  # Valid concept IDs
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    costConceptId = 31980L,
    currencyConceptId = 44818669L
  )
  
  expect_equal(settings$costConceptId, 31980L)
  expect_equal(settings$currencyConceptId, 44818669L)
  
  # Non-integer values should be coerced
  settings2 <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    costConceptId = 31980.5,
    currencyConceptId = 44818669.9
  )
  
  expect_equal(settings2$costConceptId, 31980L)
  expect_equal(settings2$currencyConceptId, 44818669L)
})

test_that("createCostOfCareSettings returns consistent structure", {
  # Minimal settings
  settings1 <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365
  )
  
  # Full settings
  settings2 <- createCostOfCareSettings(
    anchorCol = "cohort_end_date",
    startOffsetDays = -90,
    endOffsetDays = 0,
    restrictVisitConceptIds = c(9201, 9203),
    eventFilters = list(
      list(name = "Test", domain = "Drug", conceptIds = c(1, 2, 3))
    ),
    microCosting = TRUE,
    primaryEventFilterName = "Test",
    costConceptId = 31980L,
    currencyConceptId = 44818669L
  )
  
  # Both should have the same structure
  expect_setequal(names(settings1), names(settings2))
  
  # Check all expected fields exist
  expectedFields <- c(
    "anchorCol", "startOffsetDays", "endOffsetDays",
    "hasVisitRestriction", "restrictVisitConceptIds",
    "hasEventFilters", "eventFilters", "nFilters",
    "microCosting", "primaryEventFilterName",
    "costConceptId", "currencyConceptId"
  )
  
  expect_true(all(expectedFields %in% names(settings1)))
})