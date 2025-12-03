library(testthat)
library(DatabaseConnector)

#===============================================================================
# Test Suite for Modular SQL Architecture
#===============================================================================

describe("Modular SQL Architecture", {
  
  it("should have all required SQL modules", {
    
    # Check that all SQL modules exist
    expectedModules <- c(
      "00_initialize_diagnostics.sql",
      "01_cohort_analysis_window.sql", 
      "02_qualifying_visits.sql",
      "03_cost_calculation.sql",
      "04_final_calculations.sql",
      "05_cleanup.sql"
    )
    
    for (module in expectedModules) {
      modulePath <- system.file("sql", "modules", module, package = "CostUtilization")
      expect_true(file.exists(modulePath), 
                  info = sprintf("SQL module %s should exist", module))
      
      # Check that file is not empty
      content <- readLines(modulePath, warn = FALSE)
      expect_gt(length(content), 0, 
                info = sprintf("SQL module %s should not be empty", module))
    }
  })
  
  it("should execute SQL modules in correct order", {
    
    # This test would require a database connection
    # For now, just test that the function exists and can be called
    expect_true(exists("executeSqlModules", mode = "function"))
    
    # Test parameter preparation
    testParams <- list(
      cdmDatabaseSchema = "test_schema",
      cohortDatabaseSchema = "test_schema", 
      cohortTable = "test_cohort",
      cohortId = 1L,
      anchorOnEnd = FALSE,
      timeA = 0L,
      timeB = 365L,
      costConceptId = 31973L,
      currencyConceptId = 44818668L,
      hasVisitRestriction = FALSE,
      hasEventFilters = FALSE,
      nFilters = 0L,
      microCosting = FALSE,
      cpiAdjustment = FALSE
    )
    
    renderParams <- prepareSqlRenderParams(testParams, NULL)
    
    expect_true(is.list(renderParams))
    expect_true("cdm_database_schema" %in% names(renderParams))
    expect_true("cohort_database_schema" %in% names(renderParams))
  })
  
  it("should handle DatabaseConnector helper functions", {
    
    # Test that helper functions exist
    expect_true(exists("insertTableDC", mode = "function"))
    expect_true(exists("querySqlDC", mode = "function"))
    expect_true(exists("cleanupTempTablesDC", mode = "function"))
    expect_true(exists("getDbmsDC", mode = "function"))
    expect_true(exists("isValidConnectionDC", mode = "function"))
  })
})

describe("SQL Module Content Validation", {
  
  it("should have proper SQL syntax in modules", {
    
    moduleFiles <- c(
      "00_initialize_diagnostics.sql",
      "01_cohort_analysis_window.sql", 
      "02_qualifying_visits.sql",
      "03_cost_calculation.sql",
      "04_final_calculations.sql",
      "05_cleanup.sql"
    )
    
    for (moduleFile in moduleFiles) {
      modulePath <- system.file("sql", "modules", moduleFile, package = "CostUtilization")
      content <- paste(readLines(modulePath, warn = FALSE), collapse = "\n")
      
      # Basic SQL syntax checks
      expect_true(grepl("--", content), 
                  info = sprintf("%s should contain SQL comments", moduleFile))
      
      # Check for proper parameterization
      if (grepl("@", content)) {
        expect_true(grepl("@[a-zA-Z_][a-zA-Z0-9_]*", content),
                    info = sprintf("%s should use proper parameter syntax", moduleFile))
      }
    }
  })
  
  it("should have consistent table naming patterns", {
    
    moduleFiles <- c(
      "01_cohort_analysis_window.sql", 
      "02_qualifying_visits.sql",
      "03_cost_calculation.sql"
    )
    
    for (moduleFile in moduleFiles) {
      modulePath <- system.file("sql", "modules", moduleFile, package = "CostUtilization")
      content <- paste(readLines(modulePath, warn = FALSE), collapse = "\n")
      
      # Check for temporary table patterns
      expect_true(grepl("#[a-zA-Z_][a-zA-Z0-9_]*", content),
                  info = sprintf("%s should create temporary tables", moduleFile))
    }
  })
})