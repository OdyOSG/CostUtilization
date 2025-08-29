library(testthat)
library(DBI)
library(duckdb)
library(dplyr)

#===============================================================================
# Test Suite for asFeatureExtractionAggregated()
#===============================================================================

describe("asFeatureExtractionAggregated", {
  
  # Setup DuckDB + Eunomia
  databaseFile <- getEunomiaDuckDb(pathToData = 'testing_data')
  con <- DBI::dbConnect(duckdb::duckdb(databaseFile))
  on.exit({
    DBI::dbDisconnect(con, shutdown = TRUE)
    unlink(databaseFile)
  }, add = TRUE)
  
  # Transform cost table to CDM v5.5 and create small cohort
  con <- transformCostToCdmV5dot5(con)
  cohort_data <- read.csv(system.file('testdata','cohort.csv', package = 'CostUtilization'))
  DBI::dbWriteTable(con, "cohort", cohort_data, overwrite = TRUE)
  
  it("returns FE-compatible aggregated structure with expected components", {
    settings <- createCostOfCareSettings()
    analysis <- calculateCostOfCare(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1,
      costOfCareSettings = settings,
      verbose = FALSE
    )
    fe <- CostUtilization:::asFeatureExtractionAggregated(
      results = analysis$results,
      diagnostics = analysis$diagnostics,
      settings = settings,
      cohortId = 1,
      analysisId = 401L
    )
    
    # Structure
    expect_type(fe, "list")
    expect_true(all(c("aggregatedCovariates","aggregatedCovariateRef","analysisRef","metaData") %in% names(fe)))
    expect_s3_class(fe$aggregatedCovariates, "tbl_df")
    expect_s3_class(fe$aggregatedCovariateRef, "tbl_df")
    expect_s3_class(fe$analysisRef, "tbl_df")
    expect_s3_class(as_tibble(fe$metaData), "tbl_df")
    expect_true("AggregatedCovariateData" %in% class(fe))
    
    # Basic invariants
    popSize <- analysis$diagnostics %>% filter(stepName == "01_person_subset") %>% pull(nPersons) %>% as.numeric()
    expect_true(popSize > 0)
    expect_true(all(fe$aggregatedCovariates$countValue == popSize))
    
    # Reference tables contents
    expect_true(all(fe$aggregatedCovariateRef$analysisId == 401L))
    expect_true(nrow(fe$aggregatedCovariateRef) >= 10)
    
    # analysisRef fields
    expect_equal(fe$analysisRef$analysisId, 401L)
    expect_equal(fe$analysisRef$domainId, "Cost")
    expect_equal(fe$analysisRef$isBinary, 0L)
  })
  
})
