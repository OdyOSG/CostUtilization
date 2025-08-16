# tests/testthat/test-cost-injection.R

testthat::test_that("injectCostData creates required tables and data", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  result <- injectCostData(connection, seed = 42)
  testthat::expect_s4_class(result, "DatabaseConnectorConnection")
  
  tables <- tolower(DatabaseConnector::getTableNames(connection, "main"))
  testthat::expect_true("payer_plan_period" %in% tables)
  testthat::expect_true("cost" %in% tables)
  
  payerCount <- DatabaseConnector::querySql(
    connection,
    "SELECT COUNT(*) AS n FROM main.payer_plan_period"
  )
  testthat::expect_gt(payerCount$n, 0)
})

testthat::test_that("injectCostData handles an empty database gracefully", {
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(dbms = "sqlite", server = ":memory:")
  )
  
  # Create an empty observation_period table to simulate an empty CDM
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE main.observation_period (
      person_id INTEGER,
      observation_period_start_date DATE,
      observation_period_end_date DATE
    );
  ")
  
  # The function should issue a warning and complete without error
  testthat::expect_warning(
    injectCostData(connection),
    "No observation periods found"
  )
})

testthat::test_that("transformCostToCdmV5dot4 creates proper long format", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  # Run the full transformation process
  transformCostToCdmV5dot4(connectionDetails)
  
  tables <- tolower(DatabaseConnector::getTableNames(connection, "main"))
  testthat::expect_true("cost_v5_3_backup" %in% tables)
  
  costStructure <- DatabaseConnector::querySql(connection, "PRAGMA table_info(cost)")
  actualColumns <- tolower(costStructure$name)
  expectedColumns <- c("cost_id", "person_id", "cost_event_id",
                       "cost_concept_id", "cost", "incurred_date")
  
  testthat::expect_true(all(expectedColumns %in% actualColumns))
})

testthat::test_that("transformCostToCdmV5dot4 auto-injects data if cost table is missing", {
  # This test verifies the new, more robust behavior
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = ":memory:"
  )
  
  # This should NOT error, but instead run injectCostData first
  testthat::expect_no_error({
    connection <- transformCostToCdmV5dot4(connectionDetails)
    withr::defer(DatabaseConnector::disconnect(connection))
  })
})

testthat::test_that("cost generation produces realistic values", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  injectCostData(connection, seed = 123)
  
  costs <- DatabaseConnector::querySql(connection, "SELECT * FROM main.cost LIMIT 100") |>
    dplyr::rename_with(camelCaseToSnakeCase) # Helper to convert to camelCase
  
  testthat::expect_true(all(costs$totalCharge >= costs$totalCost, na.rm = TRUE))
  testthat::expect_true(all(costs$totalPaid <= costs$totalCost, na.rm = TRUE))
  testthat::expect_true(all(costs$paidByPayer >= 0, na.rm = TRUE))
  testthat::expect_true(all(costs$paidByPatient >= 0, na.rm = TRUE))
})

testthat::test_that("payer plan generation creates non-overlapping periods", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  injectCostData(connection, seed = 456)
  
  plans <- DatabaseConnector::querySql(
    connection,
    "SELECT * FROM main.payer_plan_period ORDER BY person_id, payer_plan_period_start_date"
  )
  
  plansDf <- plans |>
    dplyr::rename_with(camelCaseToSnakeCase) |>
    dplyr::group_by(personId) |>
    dplyr::arrange(payerPlanPeriodStartDate) |>
    dplyr::mutate(
      prevEnd = dplyr::lag(payerPlanPeriodEndDate),
      overlap = !is.na(prevEnd) & payerPlanPeriodStartDate <= prevEnd
    )
  
  testthat::expect_false(any(plansDf$overlap, na.rm = TRUE))
})

testthat::test_that("cost generation can be restricted to specific domains", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- withr::local_db_connection(
    DatabaseConnector::connect(connectionDetails)
  )
  
  injectCostData(
    connection,
    seed = 789,
    costDomains = c("Drug", "Visit")
  )
  
  costDomains <- DatabaseConnector::querySql(
    connection,
    "SELECT DISTINCT cost_domain_id FROM main.cost"
  ) |>
    dplyr::rename(costDomainId = COST_DOMAIN_ID)
  
  testthat::expect_setequal(costDomains$costDomainId, c("Drug", "Visit"))
})

# Helper function for renaming columns in tests
camelCaseToSnakeCase <- function(x) {
  x <- tolower(x)
  gsub("_(.)", "\\U\\1", x, perl = TRUE)
}