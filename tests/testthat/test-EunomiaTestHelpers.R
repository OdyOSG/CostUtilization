library(testthat)

# The setup file is not sourced here because we are testing the setup functions themselves.
# A clean environment is needed.

test_that("injectCostData and transformCostToCdmV5_5 work sequentially", {
  # Setup a fresh, clean Eunomia instance for this specific test
  eunomia_dir_test <- tempfile("eunomia_test_")
  dir.create(eunomia_dir_test)
  
  # Get connection details and establish a connection
  td <- getEunomiaConnectionDetails(databaseFile = file.path(eunomia_dir_test, "eunomia.sqlite"))
  conn <- DatabaseConnector::connect(td)
  
  on.exit({
    DatabaseConnector::disconnect(conn)
    unlink(eunomia_dir_test, recursive = TRUE, force = TRUE)
  })
  
  # 1. Test injectCostData
  expect_true(injectCostData(connectionDetails = td, cdmDatabaseSchema = "main"))
  
  tables <- tolower(DatabaseConnector::getTableNames(conn, "main"))
  expect_true("cost" %in% tables)
  expect_true("payer_plan_period" %in% tables)
  
  # The injected cost table should be "wide"
  cost_cols_wide <- tolower(DatabaseConnector::getTableNames(conn, "main", "cost"))
  expect_true("total_charge" %in% cost_cols_wide)
  expect_false("incurred_date" %in% cost_cols_wide) # Key indicator of long format
  
  # 2. Test transformCostToCdmV5_5
  expect_true(transformCostToCdmV5_5(connectionDetails = td, cdmDatabaseSchema = "main"))
  
  tables_after <- tolower(DatabaseConnector::getTableNames(conn, "main"))
  expect_true("cost" %in% tables_after)
  expect_true("cost_v5_3_backup" %in% tables_after) # Backup table should exist
  
  # The new cost table should be "long"
  cost_cols_long <- tolower(DatabaseConnector::getTableNames(conn, "main", "cost"))
  expect_true("incurred_date" %in% cost_cols_long)
  expect_true("cost_concept_id" %in% cost_cols_long)
  expect_false("total_charge" %in% cost_cols_long)
})

test_that("transformCostToCdmV5_5 handles missing tables gracefully", {
  # Setup a fresh, clean Eunomia instance
  eunomia_dir_test <- tempfile("eunomia_err_")
  dir.create(eunomia_dir_test)
  td <- getEunomiaConnectionDetails(databaseFile = file.path(eunomia_dir_test, "eunomia.sqlite"))
  on.exit(unlink(eunomia_dir_test, recursive = TRUE, force = TRUE))
  
  # Expect error because the 'cost' table doesn't exist
  expect_error(transformCostToCdmV5_5(connectionDetails = td, cdmDatabaseSchema = "main"))
})