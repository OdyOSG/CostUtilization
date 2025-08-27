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
  databaseFile <- getEunomiaDuckDb()
  con <- DBI::dbConnect(duckdb::duckdb(databaseFile))

  # Run the transformation
  con <- transformCostToCdmV5dot5(con)
  
  # Create a simple cohort for testing
  cohort_data <- dplyr::tibble(
    cohort_definition_id = 1L,
    subject_id = c(
      1,2,3,5,6,7,9,11,12,16,17,18,19,23,28,30,32,35,36,38,
      40,41,42,43,49,53,57,61,63,64,65,66,67,69,70,72,74,78,
      79,80,81,82,84,86,90,94,95,96,97,99,101,102,103,105,
      110,111,114,115,116,117,119,120,123,124,126,129,133,
      135,137,140,141,143,144,145,148,149,152,153,154,155
    ),
    cohort_start_date = as.Date(c(
      "1953-02-06","1920-07-01","1920-09-16","1971-03-12","1965-06-23",
      "1968-12-06","1980-11-14","1955-10-23","1964-08-22","1972-04-10",
      "1951-04-10","1967-07-16","1949-11-26","1986-04-28","1979-11-30",
      "1971-01-19","1944-06-24","1960-06-20","1959-01-28","1965-08-17",
      "1952-10-07","1974-08-20","1913-07-15","1957-01-22","1973-03-08",
      "1962-12-13","1963-11-15","1968-08-18","1956-10-10","1975-07-25",
      "1968-07-29","1959-04-23","1972-11-10","1947-11-05","1948-08-11",
      "1952-02-23","1973-09-28","1967-02-07","1947-10-17","1937-07-09",
      "1956-08-27","1961-08-25","1974-05-16","1952-04-03","1961-04-30",
      "1954-05-17","1962-04-05","1925-08-26","1943-09-19","1959-06-22",
      "1930-05-06","1974-01-16","1972-03-02","1975-03-28","1979-08-28",
      "1976-05-03","1973-04-07","1960-12-25","1927-07-05","1969-10-03",
      "1956-02-01","1978-06-06","1951-08-14","1984-09-07","1947-10-22",
      "1975-08-03","1988-11-03","1973-07-07","1972-08-23","1970-02-05",
      "1985-03-24","1965-01-09","1977-02-07","1950-10-21","1980-09-04",
      "1942-06-15","1969-03-19","1943-09-11","1977-09-11","1958-12-12"
    )),
    cohort_end_date = as.Date(c(
      "1954-02-06","1921-07-01","1921-09-16","1972-03-11","1966-06-23",
      "1969-12-06","1981-11-14","1956-10-22","1965-08-22","1973-04-10",
      "1952-04-09","1968-07-15","1950-11-26","1987-04-28","1980-11-29",
      "1972-01-19","1945-06-24","1961-06-20","1960-01-28","1966-08-17",
      "1953-10-07","1975-08-20","1914-07-15","1958-01-22","1974-03-08",
      "1963-12-13","1964-11-14","1969-08-18","1957-10-10","1976-07-24",
      "1969-07-29","1960-04-22","1973-11-10","1948-11-04","1949-08-11",
      "1953-02-22","1974-09-28","1968-02-07","1948-10-16","1938-07-09",
      "1957-08-27","1962-08-25","1975-05-16","1953-04-03","1962-04-30",
      "1955-05-17","1963-04-05","1926-08-26","1944-09-18","1960-06-21",
      "1931-05-06","1975-01-16","1973-03-02","1976-03-27","1980-08-27",
      "1977-05-03","1974-04-07","1961-12-25","1928-07-04","1970-10-03",
      "1957-01-31","1979-06-06","1952-08-13","1985-09-07","1948-10-21",
      "1976-08-02","1989-11-03","1974-07-07","1973-08-23","1971-02-05",
      "1986-03-24","1966-01-09","1978-02-07","1951-10-21","1981-09-04",
      "1943-06-15","1970-03-19","1944-09-10","1978-09-11","1959-12-12"
    ))
  )
  
  DBI::dbWriteTable(con, "cohort", cohort_data, overwrite = TRUE)
  
  # Ensure the connection is closed and the temp file is deleted when tests are done
  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(dbFile)
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
    expect_gt(analysis_result$results$totalCost, 0)
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
    expect_gt(diag_row$nEvents, 0) # Should still have some visits
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
    expect_gt(analysis_result$results$totalCost, 0)
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
    expect_gt(analysis_result$results$totalCost, 0)
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
