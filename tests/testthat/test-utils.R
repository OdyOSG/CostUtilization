# tests/testthat/test-utils.R

test_that("validateEventFilters catches invalid structures", {
  # Valid event filters
  validFilters <- list(
    list(
      name = "Test Filter",
      domain = "Drug",
      conceptIds = c(1234, 5678)
    )
  )
  
  expect_no_error(validateEventFilters(validFilters))
  
  # Missing name
  invalidFilters1 <- list(
    list(
      domain = "Drug",
      conceptIds = c(1234)
    )
  )
  
  expect_error(validateEventFilters(invalidFilters1))
  
  # Missing domain
  invalidFilters2 <- list(
    list(
      name = "Test",
      conceptIds = c(1234)
    )
  )
  
  expect_error(validateEventFilters(invalidFilters2))
  
  # Missing conceptIds
  invalidFilters3 <- list(
    list(
      name = "Test",
      domain = "Drug"
    )
  )
  
  expect_error(validateEventFilters(invalidFilters3))
  
  # Empty conceptIds
  invalidFilters4 <- list(
    list(
      name = "Test",
      domain = "Drug",
      conceptIds = integer(0)
    )
  )
  
  expect_error(validateEventFilters(invalidFilters4))
  
  # Non-unique names
  invalidFilters5 <- list(
    list(
      name = "Duplicate",
      domain = "Drug",
      conceptIds = c(1234)
    ),
    list(
      name = "Duplicate",
      domain = "Condition",
      conceptIds = c(5678)
    )
  )
  
  expect_error(validateEventFilters(invalidFilters5))
  
  # Invalid domain
  invalidFilters6 <- list(
    list(
      name = "Test",
      domain = "InvalidDomain",
      conceptIds = c(1234)
    )
  )
  
  expect_error(validateEventFilters(invalidFilters6))
  
  # Non-integer conceptIds
  invalidFilters7 <- list(
    list(
      name = "Test",
      domain = "Drug",
      conceptIds = c("not", "integers")
    )
  )
  
  expect_error(validateEventFilters(invalidFilters7))
})

test_that("createExampleEventFilters generates valid filters", {
  # Test diabetes filters
  diabetesFilters <- createExampleEventFilters("diabetes")
  
  expect_type(diabetesFilters, "list")
  expect_true(length(diabetesFilters) > 0)
  
  # Validate structure
  for (filter in diabetesFilters) {
    expect_true("name" %in% names(filter))
    expect_true("domain" %in% names(filter))
    expect_true("conceptIds" %in% names(filter))
    expect_true(length(filter$conceptIds) > 0)
  }
  
  # Should pass validation
  expect_no_error(validateEventFilters(diabetesFilters))
  
  # Test cardiovascular filters
  cvFilters <- createExampleEventFilters("cardiovascular")
  
  expect_type(cvFilters, "list")
  expect_true(length(cvFilters) > 0)
  expect_no_error(validateEventFilters(cvFilters))
  
  # Test hypertension filters
  htnFilters <- createExampleEventFilters("hypertension")
  
  expect_type(htnFilters, "list")
  expect_true(length(htnFilters) > 0)
  expect_no_error(validateEventFilters(htnFilters))
  
  # Test invalid condition
  expect_error(
    createExampleEventFilters("invalid_condition"),
    "Unknown condition"
  )
})

test_that("formatCostResults formats output correctly", {
  # Create mock results
  mockResults <- list(
    results = data.frame(
      total_person_days = 36500,
      total_person_months = 1200,
      total_person_years = 100,
      metric_type = "visit_level",
      total_cost = 1500000,
      n_persons_with_cost = 85,
      distinct_visits = 450,
      distinct_visit_dates = 320,
      distinct_visit_details = -1,
      cost_pppm = 1250,
      visits_per_1000_py = 4500,
      visit_dates_per_1000_py = 3200,
      visit_details_per_1000_py = -1
    ),
    diagnostics = data.frame(
      step_name = c("00_initial_cohort", "01_person_subset", "02_valid_window", 
                    "03_with_qualifying_visits", "04_with_cost"),
      n_persons = c(100, 100, 95, 90, 85),
      n_events = c(NA, NA, NA, 450, 450)
    )
  )
  
  # Test summary format
  summary <- formatCostResults(mockResults, format = "summary")
  
  expect_type(summary, "list")
  expect_true("cohort_size" %in% names(summary))
  expect_true("analysis_summary" %in% names(summary))
  expect_true("cost_metrics" %in% names(summary))
  expect_true("utilization_metrics" %in% names(summary))
  
  expect_equal(summary$cohort_size, 100)
  expect_equal(summary$cost_metrics$total_cost, 1500000)
  expect_equal(summary$cost_metrics$cost_pppm, 1250)
  
  # Test detailed format
  detailed <- formatCostResults(mockResults, format = "detailed")
  
  expect_equal(detailed, mockResults)
  
  # Test invalid format
  expect_error(
    formatCostResults(mockResults, format = "invalid"),
    "format must be one of"
  )
})

test_that("calculatePersonTime computes correct values", {
  # Test basic calculation
  personDays <- 365250  # 1000 person-years
  
  personTime <- calculatePersonTime(personDays)
  
  expect_type(personTime, "list")
  expect_equal(personTime$person_days, 365250)
  expect_equal(personTime$person_months, 12000, tolerance = 0.1)
  expect_equal(personTime$person_quarters, 4000, tolerance = 0.1)
  expect_equal(personTime$person_years, 1000, tolerance = 0.1)
  
  # Test zero days
  zeroTime <- calculatePersonTime(0)
  
  expect_equal(zeroTime$person_days, 0)
  expect_equal(zeroTime$person_months, 0)
  expect_equal(zeroTime$person_quarters, 0)
  expect_equal(zeroTime$person_years, 0)
  
  # Test fractional years
  fractionalDays <- 180  # About half a year
  
  fractionalTime <- calculatePersonTime(fractionalDays)
  
  expect_equal(fractionalTime$person_days, 180)
  expect_true(fractionalTime$person_months > 5 && fractionalTime$person_months < 7)
  expect_true(fractionalTime$person_years < 1)
})

test_that("computeRates calculates correct rates", {
  # Test normal calculation
  rates <- computeRates(
    numerator = 100,
    personYears = 1000,
    scale = 1000  # Per 1000 person-years
  )
  
  expect_equal(rates, 100)  # 100 per 1000 person-years
  
  # Test with zero denominator
  zeroRates <- computeRates(
    numerator = 100,
    personYears = 0,
    scale = 1000
  )
  
  expect_equal(zeroRates, 0)
  
  # Test with different scales
  rate100 <- computeRates(100, 1000, scale = 100)
  expect_equal(rate100, 10)  # 10 per 100 person-years
  
  rate10000 <- computeRates(100, 1000, scale = 10000)
  expect_equal(rate10000, 1000)  # 1000 per 10000 person-years
})

test_that("aggregateCosts handles different aggregation levels", {
  # Create mock cost data
  costData <- data.frame(
    person_id = rep(1:10, each = 5),
    visit_id = 1:50,
    cost = runif(50, 100, 1000),
    cost_date = seq(as.Date("2010-01-01"), by = "week", length.out = 50)
  )
  
  # Test person-level aggregation
  personLevel <- aggregateCosts(costData, level = "person")
  
  expect_equal(nrow(personLevel), 10)
  expect_true("total_cost" %in% names(personLevel))
  expect_true("n_visits" %in% names(personLevel))
  
  # Test visit-level aggregation
  visitLevel <- aggregateCosts(costData, level = "visit")
  
  expect_equal(nrow(visitLevel), 50)
  
  # Test overall aggregation
  overall <- aggregateCosts(costData, level = "overall")
  
  expect_equal(nrow(overall), 1)
  expect_equal(overall$total_cost, sum(costData$cost))
})