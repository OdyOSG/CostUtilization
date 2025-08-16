library(SqlRender)
library(testthat)
test_that("SQL template renders and translates across dialects", {
  sql <- readSql(system.file("sql", "main_cost_utilization.sql", package = "CostUtilization"))
  dialects <- c("postgresql", "sql server", "oracle", "snowflake", "bigquery")
  
  # Test Case 1: Baseline (no filters, visit-level)
  params_base <- list(microCosting = FALSE, hasVisitRestriction = FALSE, hasEventFilters = FALSE)
  for (dialect in dialects) {
    expect_no_error(render(sql, ... = params_base))
    expect_no_error(translate(render(sql, ... = params_base), targetDialect = dialect))
  }
  
  # Test Case 2: Micro-costing with all filters
  params_full <- list(microCosting = TRUE, hasVisitRestriction = TRUE, hasEventFilters = TRUE, nFilters = 2)
  for (dialect in dialects) {
    expect_no_error(render(sql, ... = params_full))
    expect_no_error(translate(render(sql, ... = params_full), targetDialect = dialect))
  }
})

test_that("calculateCostOfCare runs end-to-end and returns correct structure", {
  # This test requires a live database connection with a seeded synthetic CDM
  skip_if_not(dbIsValid(conn), "Database connection is not available")
  
  # Basic run
  res_base <- calculateCostOfCare(
    conn = conn,
    cdmSchema = "cdm",
    cohortSchema = "results",
    cohortTable = "cohort",
    cohortId = 1L,
    returnFormat = "list"
  )
  
  expect_type(res_base, "list")
  expect_named(res_base, c("results", "diagnostics"))
  expect_s3_class(res_base$results, "tbl_df")
  expect_gt(nrow(res_base$results), 0)
  expect_equal(res_base$results$metric_type, "visitLevel")
  
  # Micro-costing run with filters
  res_micro <- calculateCostOfCare(
    conn = conn,
    cdmSchema = "cdm",
    cohortSchema = "results",
    cohortTable = "cohort",
    cohortId = 1L,
    microCosting = TRUE,
    eventFilters = list(
      list(name = "some_proc", concepts = c(40481997L), domainScope = "Procedure")
    ),
    returnFormat = "list"
  )
  
  expect_equal(res_micro$results$metric_type, "lineLevel")
})