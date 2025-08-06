# This file is sourced automatically by testthat.
# It sets up a consistent Eunomia database environment for all tests.

library(Eunomia)
library(withr)

# Create a temporary directory for the database
eunomia_dir <- tempfile("eunomia_")
dir.create(eunomia_dir)

# Get connection details and establish a connection
connectionDetails <- getEunomiaConnectionDetails(databaseFile = file.path(eunomia_dir, "eunomia.sqlite"))

# Defer cleanup to the end of the testing session
withr::defer(
  {
    DatabaseConnector::disconnect(connection)
    unlink(eunomia_dir, recursive = TRUE, force = TRUE)
  },
  testthat::teardown_env()
)

# --- Inject and Transform Cost Data ---
# This block ensures the database is ready for testing CostUtilization functions.
# It creates a wide-format cost table and then transforms it to the modern
# long format required by the main analysis functions.
message("Setting up Eunomia database for testing: Injecting and transforming cost data to 5.5")
con <- transformCostToCdmV5dot5(connectionDetails)
message("Eunomia setup complete.")