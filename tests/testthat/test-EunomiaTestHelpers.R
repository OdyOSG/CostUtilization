library(testthat)

test_that("transformCostToCdmV5dot5 handles missing tables gracefully", {
  td <- Eunomia::getEunomiaConnectionDetails()
  con <- transformCostToCdmV5dot5(connectionDetails = td, cdmDatabaseSchema = "main")
  on.exit(DatabaseConnector::disconnect(con))
  expect_s4_class(con, "DatabaseConnectorConnection")
})