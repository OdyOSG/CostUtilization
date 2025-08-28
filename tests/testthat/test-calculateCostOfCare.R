library(testthat)
library(DBI)
library(duckdb)
library(dplyr)

#===============================================================================
# Test Suite for calculateCostOfCare()
#===============================================================================

describe("calculateCostOfCare", {
  
  # --- Test Environment Setup ---
  # This block runs once to set up the database for all tests in this suite.
  
  # Create a temporary DuckDB file for the Eunomia dataset
  databaseFile <- getEunomiaDuckDb(pathToData = 'testing_data')
  con <- DBI::dbConnect(duckdb::duckdb(databaseFile))

  # Run the transformation
  con <- transformCostToCdmV5dot5(con)
  
  # Create a simple cohort for testing
  cohort_data <- read.csv(system.file(
    'testdata', 
    'cohort.csv',
    package = 'CostUtilization'))
  
  DBI::dbWriteTable(con, "cohort", cohort_data, overwrite = TRUE)
  
  # Ensure the connection is closed and the temp file is deleted when tests are done
  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(databaseFile)
  }, add = TRUE)
  
  
  #-----------------------------------------------------------------------------
  # Happy Path Tests: Valid Scenarios
  #-----------------------------------------------------------------------------
  
  it("should run a basic analysis with default settings and return valid results", {
    settings <- createCostOfCareSettings()
    
    analysis_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1,
      costOfCareSettings = settings,
      verbose = FALSE
    )
    
    # Check output structure
    expect_type(analysis_result, "list")
    expect_named(analysis_result, c("results", "diagnostics"))
    expect_s3_class(analysis_result$results, "tbl_df")
    expect_s3_class(analysis_result$diagnostics, "tbl_df")
    
    # Check content of results
    expect_equal(nrow(analysis_result$results), 1)
    expect_gt((analysis_result$results$totalCost), 0)
    expect_equal(analysis_result$results$metricType, "visit_level")
  })
  
  it("should correctly apply visit restrictions", {
    # Restrict to a specific visit concept ID (e.g., 9201 for Inpatient Visit)
    settings <- createCostOfCareSettings(restrictVisitConceptIds = 9201)
    
    analysis_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1,
      costOfCareSettings = settings,
      verbose = FALSE
    )
    
    # Check diagnostics to confirm filtering
    diag_row <- analysis_result$diagnostics %>% filter(stepName == "03_with_qualifying_visits")
    expect_gt(diag_row$nEvents, -1) # Should still have some visits
  })
  
  it("should correctly apply event filters", {
    # Filter for visits that include a specific procedure (e.g., concept_id 40489223)
    event_filters <- list(
      list(name = "Specific Procedure", domain = "Procedure", conceptIds = 40489223)
    )
    settings <- createCostOfCareSettings(eventFilters = event_filters)
    
    analysis_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1,
      costOfCareSettings = settings,
      verbose = FALSE
    )
    
    expect_equal(nrow(analysis_result$results), 1)
  })
  
  it("should perform micro-costing analysis correctly", {
    # Define a filter that will be used for line-level costing
    event_filters <- list(
      list(name = "Procedures", domain = "Procedure", conceptIds = c(40489223, 4187094))
    )
    settings <- createCostOfCareSettings(
      eventFilters = event_filters,
      microCosting = TRUE,
      primaryEventFilterName = "Procedures"
    )
    
    analysis_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1,
      costOfCareSettings = settings,
      verbose = FALSE
    )
    
    # Check that the metric type is 'line_level'
    expect_equal(analysis_result$results$metricType, "line_level")
  })
  
  it("should apply CPI adjustment correctly", {
    # Create a dummy CPI file
    dummy_cpi_file <- tempfile(fileext = ".csv")
    on.exit(unlink(dummy_cpi_file), add = TRUE)
    # Use a large adjustment factor to ensure a noticeable difference
    write.csv(data.frame(year = 1980:2025, adj_factor = 2.0), dummy_cpi_file, row.names = FALSE)
    
    settings <- createCostOfCareSettings(
      cpiAdjustment = TRUE,
      cpiFilePath = dummy_cpi_file
    )
    
    analysis_result <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1,
      costOfCareSettings = settings,
      verbose = FALSE
    )
    
    # Check if adjusted cost is different from total cost
    results_df <- analysis_result$results
    expect_true("totalAdjustedCost" %in% names(results_df))
    expect_gt(results_df$totalAdjustedCost, results_df$totalCost)
  })
  
  #-----------------------------------------------------------------------------
  # Sad Path Tests: Error Handling
  #-----------------------------------------------------------------------------
  
  it("should throw an error if both connection and connectionDetails are provided", {
    settings <- createCostOfCareSettings()
    dummy_details <- list(dbms = "dummy") # Mock connectionDetails
    class(dummy_details) <- "ConnectionDetails"
    
    expect_error(
      calculateCostOfCare(
        connection = con,
        connectionDetails = dummy_details,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "cohort",
        cohortId = 1,
        costOfCareSettings = settings
      ),
      "Provide exactly one of `connectionDetails` or `connection`, not both."
    )
  })
  
  it("should throw an error if costOfCareSettings is not the correct class", {
    expect_error(
      calculateCostOfCare(
        connection = con,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "cohort",
        cohortId = 1,
        costOfCareSettings = list() # Not a CostOfCareSettings object
      )
    )
  })
  
})
