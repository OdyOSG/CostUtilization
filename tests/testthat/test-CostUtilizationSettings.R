library(testthat)

test_that("createCostUtilizationSettings creates object with default values", {
  settings <- createCostUtilizationSettings()
  
  expect_s3_class(settings, "costUtilizationSettings")
  expect_equal(settings$timeWindows, list(c(-365, 0)))
  expect_false(settings$useInCohortWindow)
  expect_null(settings$costDomains)
  expect_null(settings$conceptSetDefinition)
  expect_equal(settings$currencyConceptId, 44818668)
  expect_equal(settings$aggregate, c("pppm", "pppy"))
})

test_that("createCostUtilizationSettings accepts 'in cohort' window only", {
  settings <- createCostUtilizationSettings(
    timeWindows = NULL,
    useInCohortWindow = TRUE
  )
  
  expect_s3_class(settings, "costUtilizationSettings")
  expect_null(settings$timeWindows)
  expect_true(settings$useInCohortWindow)
})

test_that("createCostUtilizationSettings allows both fixed and 'in cohort' windows", {
  settings <- createCostUtilizationSettings(
    timeWindows = list(c(-30, -1)),
    useInCohortWindow = TRUE
  )
  
  expect_s3_class(settings, "costUtilizationSettings")
  expect_length(settings$timeWindows, 1)
  expect_true(settings$useInCohortWindow)
})

test_that("createCostUtilizationSettings catches invalid inputs", {
  expect_error(
    createCostUtilizationSettings(timeWindows = NULL, useInCohortWindow = FALSE),
    "At least one windowing strategy must be used"
  )
  expect_error(createCostUtilizationSettings(useInCohortWindow = "TRUE"))
  expect_error(createCostUtilizationSettings(timeWindows = "invalid"))
  expect_error(createCostUtilizationSettings(aggregate = "pppd", currencyConceptId = "usd"))
})

test_that("useConceptSet takes precedence over costDomains", {
  # cli_warn is expected here
  settings <- expect_warning(createCostUtilizationSettings(
    useConceptSet = c(1, 2, 3),
    costDomains = c("Drug", "Condition")
  ))
  
  expect_null(settings$costDomains)
  expect_s3_class(settings$conceptSetDefinition, "tbl_df")
  expect_equal(nrow(settings$conceptSetDefinition), 3)
})

test_that(".parseConceptSet handles different input types", {
  # Numeric vector
  parsed_numeric <- .parseConceptSet(c(101, 102))
  expect_equal(parsed_numeric$conceptId, c(101, 102))
  expect_equal(parsed_numeric$isExcluded, c(FALSE, FALSE))
  expect_equal(parsed_numeric$includeDescendants, c(TRUE, TRUE))
  
  # List from JSON
  json_list <- list(
    list(concept = list(CONCEPT_ID = 201), isExcluded = FALSE, includeDescendants = TRUE),
    list(concept = list(CONCEPT_ID = 202), isExcluded = TRUE, includeDescendants = FALSE)
  )
  parsed_list <- .parseConceptSet(json_list)
  expect_equal(parsed_list$conceptId, c(201, 202))
  expect_equal(parsed_list$isExcluded, c(FALSE, TRUE))
  expect_equal(parsed_list$includeDescendants, c(TRUE, FALSE))
})