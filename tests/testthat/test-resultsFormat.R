library(testthat)
library(DBI)
library(duckdb)
library(dplyr)

#===============================================================================
# Test Suite for resultsFormat.R - FeatureExtraction Format Functions
#===============================================================================

describe("FeatureExtraction Format Functions", {
  
  # --- Test Environment Setup ---
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
  
  # Run a basic cost analysis to get test data
  settings <- createCostOfCareSettings(
    costConceptId = 31973L,
    startOffsetDays = 0L,
    endOffsetDays = 365L
  )
  
  costResults <- calculateCostOfCare(
    connection = con,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE
  )
  
  # Ensure the connection is closed and the temp file is deleted when tests are done
  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(databaseFile)
  }, add = TRUE)
  
  #-----------------------------------------------------------------------------
  # Tests for createCostCovariateData()
  #-----------------------------------------------------------------------------
  
  describe("createCostCovariateData", {
    
    it("should create a valid CovariateData object with basic settings", {
      covariateData <- createCostCovariateData(
        costResults = costResults,
        costOfCareSettings = settings,
        cohortId = 1,
        databaseId = "TestDB",
        analysisId = 1L
      )
      
      # Check class and structure
      expect_s3_class(covariateData, "CovariateData")
      expect_s3_class(covariateData, "Andromeda")
      
      # Check required tables exist
      expect_true("covariates" %in% names(covariateData))
      expect_true("covariateRef" %in% names(covariateData))
      expect_true("analysisRef" %in% names(covariateData))
      
      # Check metadata
      metaData <- attr(covariateData, "metaData")
      expect_type(metaData, "list")
      expect_equal(metaData$databaseId, "TestDB")
      expect_equal(metaData$analysisId, 1L)
      expect_equal(metaData$packageName, "CostUtilization")
    })
    
    it("should handle CPI-adjusted results correctly", {
      # Create settings with CPI adjustment
      dummy_cpi_file <- tempfile(fileext = ".csv")
      on.exit(unlink(dummy_cpi_file), add = TRUE)
      write.csv(data.frame(year = 1980:2025, adj_factor = 1.5), dummy_cpi_file, row.names = FALSE)
      
      cpiSettings <- createCostOfCareSettings(
        costConceptId = 31973L,
        cpiAdjustment = TRUE,
        cpiFilePath = dummy_cpi_file
      )
      
      cpiResults <- calculateCostOfCare(
        connection = con,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "cohort",
        cohortId = 1,
        costOfCareSettings = cpiSettings,
        verbose = FALSE
      )
      
      covariateData <- createCostCovariateData(
        costResults = cpiResults,
        costOfCareSettings = cpiSettings,
        cohortId = 1
      )
      
      # Check that CPI-adjusted covariates are included
      covariateRef <- dplyr::collect(covariateData$covariateRef)
      expect_true(any(grepl("CPI Adjusted", covariateRef$covariateName)))
    })
    
    it("should handle micro-costing results correctly", {
      # Create settings for micro-costing
      event_filters <- list(
        list(name = "Procedures", domain = "Procedure", conceptIds = c(40489223, 4187094))
      )
      microSettings <- createCostOfCareSettings(
        eventFilters = event_filters,
        microCosting = TRUE,
        primaryEventFilterName = "Procedures"
      )
      
      microResults <- calculateCostOfCare(
        connection = con,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "cohort",
        cohortId = 1,
        costOfCareSettings = microSettings,
        verbose = FALSE
      )
      
      covariateData <- createCostCovariateData(
        costResults = microResults,
        costOfCareSettings = microSettings,
        cohortId = 1
      )
      
      # Check analysis reference reflects micro-costing
      analysisRef <- dplyr::collect(covariateData$analysisRef)
      expect_true(grepl("Line Level", analysisRef$analysisName))
      
      # Check metadata
      metaData <- attr(covariateData, "metaData")
      expect_equal(metaData$metricType, "line_level")
    })
    
    it("should create time reference for non-standard time windows", {
      # Create settings with custom time window
      customSettings <- createCostOfCareSettings(
        startOffsetDays = -180L,
        endOffsetDays = 180L
      )
      
      customResults <- calculateCostOfCare(
        connection = con,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "cohort",
        cohortId = 1,
        costOfCareSettings = customSettings,
        verbose = FALSE
      )
      
      covariateData <- createCostCovariateData(
        costResults = customResults,
        costOfCareSettings = customSettings,
        cohortId = 1
      )
      
      # Check time reference exists
      expect_true("timeRef" %in% names(covariateData))
      
      timeRef <- dplyr::collect(covariateData$timeRef)
      expect_equal(timeRef$startDay, -180L)
      expect_equal(timeRef$endDay, 180L)
    })
    
    it("should validate input parameters correctly", {
      # Test missing results
      expect_error(
        createCostCovariateData(
          costResults = list(),
          costOfCareSettings = settings,
          cohortId = 1
        ),
        "must.include"
      )
      
      # Test wrong settings class
      expect_error(
        createCostCovariateData(
          costResults = costResults,
          costOfCareSettings = list(),
          cohortId = 1
        ),
        "CostOfCareSettings"
      )
      
      # Test invalid cohortId
      expect_error(
        createCostCovariateData(
          costResults = costResults,
          costOfCareSettings = settings,
          cohortId = "invalid"
        )
      )
    })
  })
  
  #-----------------------------------------------------------------------------
  # Tests for convertToFeatureExtractionFormat()
  #-----------------------------------------------------------------------------
  
  describe("convertToFeatureExtractionFormat", {
    
    it("should convert results successfully with informative messages", {
      # Capture messages
      expect_message(
        covariateData <- convertToFeatureExtractionFormat(
          costResults = costResults,
          costOfCareSettings = settings,
          cohortId = 1,
          databaseId = "TestConversion"
        ),
        "Converting cost analysis results"
      )
      
      expect_message(
        convertToFeatureExtractionFormat(
          costResults = costResults,
          costOfCareSettings = settings,
          cohortId = 1
        ),
        "Conversion completed successfully"
      )
      
      # Check result is valid CovariateData
      expect_s3_class(covariateData, "CovariateData")
      
      metaData <- attr(covariateData, "metaData")
      expect_equal(metaData$databaseId, "TestConversion")
    })
  })
  
  #-----------------------------------------------------------------------------
  # Tests for Summary and Print Methods
  #-----------------------------------------------------------------------------
  
  describe("CovariateData Methods", {
    
    covariateData <- createCostCovariateData(
      costResults = costResults,
      costOfCareSettings = settings,
      cohortId = 1,
      databaseId = "TestMethods"
    )
    
    it("should provide informative summary", {
      # Capture output
      output <- capture.output(summary(covariateData))
      
      # Check key information is present
      expect_true(any(grepl("Cost Covariate Data Summary", output)))
      expect_true(any(grepl("Database ID: TestMethods", output)))
      expect_true(any(grepl("Analysis Parameters:", output)))
      expect_true(any(grepl("Number of Covariates:", output)))
      expect_true(any(grepl("Available Covariates:", output)))
    })
    
    it("should print correctly", {
      # Print should call summary
      output <- capture.output(print(covariateData))
      expect_true(any(grepl("Cost Covariate Data Summary", output)))
    })
    
    it("should handle objects without metadata gracefully", {
      # Create object without metadata
      testData <- Andromeda::andromeda()
      testData$covariateRef <- dplyr::tibble(
        covariateId = 1001L,
        covariateName = "Test Covariate",
        analysisId = 1L,
        conceptId = 0L
      )
      class(testData) <- c("CovariateData", class(testData))
      
      # Should not error
      expect_no_error(summary(testData))
    })
    
    it("should reject non-CovariateData objects", {
      expect_error(
        summary.CovariateData(list()),
        "must be of class 'CovariateData'"
      )
    })
  })
  
  #-----------------------------------------------------------------------------
  # Tests for Internal Helper Functions
  #-----------------------------------------------------------------------------
  
  describe("Internal Helper Functions", {
    
    it("should create analysis reference correctly", {
      analysisRef <- createAnalysisRef(settings, 1L)
      
      expect_s3_class(analysisRef, "tbl_df")
      expect_equal(nrow(analysisRef), 1)
      expect_equal(analysisRef$analysisId, 1L)
      expect_equal(analysisRef$domainId, "Cost")
      expect_equal(analysisRef$isBinary, "N")
      expect_equal(analysisRef$missingMeansZero, "Y")
      expect_true(grepl("Total Charge", analysisRef$description))
    })
    
    it("should create covariate reference correctly", {
      analysisRef <- createAnalysisRef(settings, 1L)
      covariateRef <- createCovariateRef(costResults$results, analysisRef, settings)
      
      expect_s3_class(covariateRef, "tbl_df")
      expect_gt(nrow(covariateRef), 0)
      expect_true(all(c("covariateId", "covariateName", "analysisId", "conceptId") %in% names(covariateRef)))
      
      # Check covariate IDs are properly generated
      expect_true(all(covariateRef$covariateId >= 1000L))
      expect_equal(unique(covariateRef$analysisId), 1L)
    })
    
    it("should extract covariate values correctly", {
      # Test with mock results
      mockResults <- dplyr::tibble(
        totalCost = 1000.50,
        totalAdjustedCost = 1200.75,
        costPppm = 83.375,
        distinctVisits = 5L,
        eventsPer1000Py = 250.0
      )
      
      # Test various extractions
      expect_equal(extractCovariateValue(mockResults, "Total Cost", 1001L), 1000.50)
      expect_equal(extractCovariateValue(mockResults, "Total Cost (CPI Adjusted)", 1002L), 1200.75)
      expect_equal(extractCovariateValue(mockResults, "Cost Per Person Per Month", 1003L), 83.375)
      expect_equal(extractCovariateValue(mockResults, "Number of Visits", 1010L), 5)
      expect_equal(extractCovariateValue(mockResults, "Events Per 1000 Person-Years", 1012L), 250.0)
      
      # Test fallback
      expect_equal(extractCovariateValue(mockResults, "Unknown Metric", 9999L), 0)
    })
    
    it("should create metadata correctly", {
      metaData <- createMetaData(settings, costResults, "TestDB", 1L)
      
      expect_type(metaData, "list")
      expect_equal(metaData$databaseId, "TestDB")
      expect_equal(metaData$analysisId, 1L)
      expect_equal(metaData$packageName, "CostUtilization")
      expect_true("call" %in% names(metaData))
      expect_true("executionTime" %in% names(metaData))
      expect_equal(metaData$isBinary, FALSE)
      expect_equal(metaData$isAggregated, TRUE)
    })
  })
  
  #-----------------------------------------------------------------------------
  # Integration Tests
  #-----------------------------------------------------------------------------
  
  describe("Integration Tests", {
    
    it("should work end-to-end with different cost concepts", {
      # Test with different cost concepts
      costConcepts <- c(31973L, 31985L, 31980L, 31981L)
      conceptNames <- c("Total Charge", "Total Cost", "Paid by Payer", "Paid by Patient")
      
      for (i in seq_along(costConcepts)) {
        testSettings <- createCostOfCareSettings(costConceptId = costConcepts[i])
        
        testResults <- calculateCostOfCare(
          connection = con,
          cdmDatabaseSchema = "main",
          cohortDatabaseSchema = "main",
          cohortTable = "cohort",
          cohortId = 1,
          costOfCareSettings = testSettings,
          verbose = FALSE
        )
        
        covariateData <- createCostCovariateData(
          costResults = testResults,
          costOfCareSettings = testSettings,
          cohortId = 1,
          analysisId = i
        )
        
        # Check analysis reference reflects correct cost concept
        analysisRef <- dplyr::collect(covariateData$analysisRef)
        expect_true(grepl(conceptNames[i], analysisRef$description))
        
        # Check covariate reference has appropriate names
        covariateRef <- dplyr::collect(covariateData$covariateRef)
        expect_gt(nrow(covariateRef), 0)
      }
    })
    
    it("should handle complex event filtering scenarios", {
      # Create complex event filters
      complexFilters <- list(
        list(name = "Diabetes Conditions", domain = "Condition", conceptIds = c(201820L, 443238L)),
        list(name = "Diabetes Drugs", domain = "Drug", conceptIds = c(1503297L, 1502826L)),
        list(name = "Lab Tests", domain = "Measurement", conceptIds = c(3004501L, 3003309L))
      )
      
      complexSettings <- createCostOfCareSettings(
        eventFilters = complexFilters,
        costConceptId = 31973L
      )
      
      complexResults <- calculateCostOfCare(
        connection = con,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "cohort",
        cohortId = 1,
        costOfCareSettings = complexSettings,
        verbose = FALSE
      )
      
      covariateData <- createCostCovariateData(
        costResults = complexResults,
        costOfCareSettings = complexSettings,
        cohortId = 1
      )
      
      # Check analysis reference reflects filtering
      analysisRef <- dplyr::collect(covariateData$analysisRef)
      expect_true(grepl("Filtered by domains", analysisRef$description))
      expect_true(grepl("Condition, Drug, Measurement", analysisRef$description))
      
      # Check metadata reflects event filtering
      metaData <- attr(covariateData, "metaData")
      expect_true(metaData$call$hasEventFilters)
    })
  })
})

#===============================================================================
# Performance and Memory Tests
#===============================================================================

describe("Performance and Memory", {
  
  it("should handle large result sets efficiently", {
    # This is a basic test - in practice, you'd want more comprehensive performance testing
    settings <- createCostOfCareSettings()
    
    # Time the conversion
    start_time <- Sys.time()
    
    covariateData <- createCostCovariateData(
      costResults = costResults,
      costOfCareSettings = settings,
      cohortId = 1
    )
    
    end_time <- Sys.time()
    conversion_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
    
    # Should complete reasonably quickly (adjust threshold as needed)
    expect_lt(conversion_time, 5.0)
    
    # Check Andromeda object is properly structured
    expect_true(Andromeda::isValidAndromeda(covariateData))
  })
  
  it("should properly clean up Andromeda objects", {
    covariateData <- createCostCovariateData(
      costResults = costResults,
      costOfCareSettings = settings,
      cohortId = 1
    )
    
    # Should be able to close without errors
    expect_no_error(Andromeda::close(covariateData))
  })
})