#===============================================================================
# FeatureExtraction Format Integration Demo
# 
# This script demonstrates how to use the new FeatureExtraction format support
# in the CostUtilization package for OHDSI ecosystem integration.
#===============================================================================

library(CostUtilization)
library(dplyr)
library(DBI)
library(duckdb)

# Setup: Create test database with cost data
cat("Setting up test database...\n")
databaseFile <- getEunomiaDuckDb(pathToData = 'demo_data')
con <- DBI::dbConnect(duckdb::duckdb(databaseFile))

# Transform to CDM v5.5 format
con <- transformCostToCdmV5dot5(con)

# Create test cohort
cohort_data <- data.frame(
  cohort_definition_id = c(1, 1, 1, 1, 1),
  subject_id = c(1001, 1002, 1003, 1004, 1005),
  cohort_start_date = as.Date(c("2020-01-01", "2020-02-15", "2020-03-10", "2020-04-05", "2020-05-20")),
  cohort_end_date = as.Date(c("2020-12-31", "2020-12-31", "2020-12-31", "2020-12-31", "2020-12-31"))
)

DBI::dbWriteTable(con, "cohort", cohort_data, overwrite = TRUE)

#===============================================================================
# Example 1: Basic Aggregated Results Conversion
#===============================================================================

cat("\n=== Example 1: Basic Aggregated Results ===\n")

# Create basic cost analysis settings
settings1 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L  # Total charge
)

# Run cost analysis (returns Andromeda object with aggregated results)
results1 <- calculateCostOfCare(
  connection = con,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main", 
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = settings1,
  verbose = FALSE
)

cat("Raw results format:\n")
print(results1$results %>% collect())

# Convert to FeatureExtraction format
covariateData1 <- createCostCovariateData(
  costResults = results1,
  costOfCareSettings = settings1,
  cohortId = 1,
  databaseId = "EunomiaDemo",
  analysisId = 1000L
)

cat("\nFeatureExtraction format summary:\n")
summary(covariateData1)

cat("\nCovariates table:\n")
print(covariateData1$covariates %>% collect())

cat("\nCovariate reference:\n")
print(covariateData1$covariateRef %>% collect())

#===============================================================================
# Example 2: Person-Level Results with CPI Adjustment
#===============================================================================

cat("\n=== Example 2: Person-Level Results with CPI Adjustment ===\n")

# Create CPI adjustment file
cpi_file <- tempfile(fileext = ".csv")
cpi_data <- data.frame(
  year = 2018:2022,
  adj_factor = c(1.0, 1.02, 1.05, 1.08, 1.12)
)
write.csv(cpi_data, cpi_file, row.names = FALSE)

# Create settings with CPI adjustment
settings2 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -180L,  # 6 months before
  endOffsetDays = 180L,     # 6 months after
  costConceptId = 31985L,   # Total cost
  cpiAdjustment = TRUE,
  cpiFilePath = cpi_file
)

# Run analysis (this would return person-level results in real scenario)
# For demo purposes, we'll simulate person-level results
results2 <- results1  # Using aggregated results for demo

# Convert to FeatureExtraction format
covariateData2 <- createCostCovariateData(
  costResults = results2,
  costOfCareSettings = settings2,
  cohortId = 1,
  databaseId = "EunomiaDemo",
  analysisId = 2000L
)

cat("CPI-adjusted analysis metadata:\n")
metaData2 <- attr(covariateData2, "metaData")
cat("  CPI Adjustment:", metaData2$cpiAdjustment, "\n")
cat("  Cost Concept ID:", metaData2$costConceptId, "\n")
cat("  Time Window:", metaData2$startOffsetDays, "to", metaData2$endOffsetDays, "days\n")

#===============================================================================
# Example 3: Event-Filtered Analysis
#===============================================================================

cat("\n=== Example 3: Event-Filtered Analysis ===\n")

# Define event filters for diabetes-related costs
diabetesFilters <- list(
  list(
    name = "Diabetes Diagnoses",
    domain = "Condition",
    conceptIds = c(201820L, 201826L, 443238L)
  ),
  list(
    name = "Diabetes Medications", 
    domain = "Drug",
    conceptIds = c(1503297L, 1502826L, 1502855L)
  )
)

# Create settings with event filters
settings3 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31980L,  # Paid by payer
  eventFilters = diabetesFilters
)

# Run analysis
results3 <- calculateCostOfCare(
  connection = con,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort", 
  cohortId = 1,
  costOfCareSettings = settings3,
  verbose = FALSE
)

# Convert to FeatureExtraction format
covariateData3 <- createCostCovariateData(
  costResults = results3,
  costOfCareSettings = settings3,
  cohortId = 1,
  databaseId = "EunomiaDemo",
  analysisId = 3000L
)

cat("Event-filtered analysis:\n")
analysisRef3 <- covariateData3$analysisRef %>% collect()
cat("  Analysis Name:", analysisRef3$analysisName, "\n")

metaData3 <- attr(covariateData3, "metaData")
cat("  Has Event Filters:", metaData3$hasEventFilters, "\n")
cat("  Number of Filters:", metaData3$nFilters, "\n")

#===============================================================================
# Example 4: Micro-Costing (Line-Level) Analysis
#===============================================================================

cat("\n=== Example 4: Micro-Costing Analysis ===\n")

# Define procedure filter for micro-costing
procedureFilters <- list(
  list(
    name = "Surgical Procedures",
    domain = "Procedure", 
    conceptIds = c(4301351L, 4022421L, 4273629L)
  )
)

# Create micro-costing settings
settings4 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 90L,     # 3 months post-index
  costConceptId = 31973L,  # Total charge
  eventFilters = procedureFilters,
  microCosting = TRUE,
  primaryEventFilterName = "Surgical Procedures"
)

# Run micro-costing analysis
results4 <- calculateCostOfCare(
  connection = con,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = settings4,
  verbose = FALSE
)

# Convert to FeatureExtraction format
covariateData4 <- createCostCovariateData(
  costResults = results4,
  costOfCareSettings = settings4,
  cohortId = 1,
  databaseId = "EunomiaDemo",
  analysisId = 4000L
)

cat("Micro-costing analysis:\n")
metaData4 <- attr(covariateData4, "metaData")
cat("  Micro-costing:", metaData4$microCosting, "\n")

analysisRef4 <- covariateData4$analysisRef %>% collect()
cat("  Analysis Name:", analysisRef4$analysisName, "\n")

#===============================================================================
# Example 5: Multiple Cost Concepts Analysis
#===============================================================================

cat("\n=== Example 5: Multiple Cost Concepts ===\n")

# Analyze multiple cost types using purrr
costTypes <- tibble(
  costType = c("Total Charge", "Total Cost", "Paid by Payer", "Paid by Patient"),
  conceptId = c(31973L, 31985L, 31980L, 31981L),
  analysisId = c(5001L, 5002L, 5003L, 5004L)
)

allCovariateData <- purrr::pmap(costTypes, function(costType, conceptId, analysisId) {
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0L,
    endOffsetDays = 365L,
    costConceptId = conceptId
  )
  
  results <- calculateCostOfCare(
    connection = con,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE
  )
  
  covariateData <- createCostCovariateData(
    costResults = results,
    costOfCareSettings = settings,
    cohortId = 1,
    databaseId = "EunomiaDemo",
    analysisId = analysisId
  )
  
  list(
    costType = costType,
    conceptId = conceptId,
    analysisId = analysisId,
    covariateData = covariateData
  )
})

cat("Multiple cost concept analyses:\n")
for (i in seq_along(allCovariateData)) {
  analysis <- allCovariateData[[i]]
  metaData <- attr(analysis$covariateData, "metaData")
  nCovariates <- analysis$covariateData$covariateRef %>% tally() %>% pull(n)
  
  cat(sprintf("  %s (ID: %d): %d covariates\n", 
              analysis$costType, analysis$conceptId, nCovariates))
}

#===============================================================================
# Example 6: Convenience Function Usage
#===============================================================================

cat("\n=== Example 6: Convenience Function ===\n")

# Use the convenience function for quick conversion
settings6 <- createCostOfCareSettings(
  costConceptId = 31973L,
  startOffsetDays = -365L,
  endOffsetDays = 0L
)

results6 <- calculateCostOfCare(
  connection = con,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = settings6,
  verbose = FALSE
)

# Quick conversion using convenience function
covariateData6 <- convertToFeatureExtractionFormat(
  costResults = results6,
  costOfCareSettings = settings6,
  cohortId = 1,
  databaseId = "EunomiaDemo"
)

cat("Convenience function result:\n")
print(covariateData6)

#===============================================================================
# Example 7: Integration with OHDSI Ecosystem
#===============================================================================

cat("\n=== Example 7: OHDSI Ecosystem Integration ===\n")

# Demonstrate how the CovariateData can be used in OHDSI workflows
cat("CovariateData objects are now compatible with:\n")
cat("  - CohortMethod: For comparative effectiveness research\n")
cat("  - PatientLevelPrediction: For outcome prediction models\n")
cat("  - FeatureExtraction: For covariate manipulation\n")
cat("  - Multi-database studies: Consistent format across sites\n")

# Show metadata that enables OHDSI integration
exampleMetaData <- attr(covariateData1, "metaData")
cat("\nKey metadata for OHDSI integration:\n")
cat("  Analysis ID:", exampleMetaData$analysisId, "\n")
cat("  Database ID:", exampleMetaData$databaseId, "\n")
cat("  Package Version:", as.character(exampleMetaData$packageVersion), "\n")
cat("  Creation Time:", format(exampleMetaData$creationTime), "\n")

# Example of how covariates would be used in downstream analysis
cat("\nExample covariate usage in downstream analysis:\n")
exampleCovariates <- covariateData1$covariates %>% 
  collect() %>%
  head(5)
print(exampleCovariates)

#===============================================================================
# Cleanup
#===============================================================================

cat("\n=== Cleanup ===\n")

# Close all CovariateData objects
purrr::walk(allCovariateData, ~ Andromeda::close(.x$covariateData))
Andromeda::close(covariateData1)
Andromeda::close(covariateData2) 
Andromeda::close(covariateData3)
Andromeda::close(covariateData4)
Andromeda::close(covariateData6)

# Close database connection
DBI::dbDisconnect(con, shutdown = TRUE)

# Clean up temporary files
unlink(databaseFile)
unlink(cpi_file)

cat("Demo completed successfully!\n")
cat("\nNext steps:\n")
cat("1. Use createCostCovariateData() to convert your cost analysis results\n")
cat("2. Integrate with CohortMethod, PatientLevelPrediction, or other OHDSI tools\n")
cat("3. Leverage the rich metadata for reproducible research\n")
cat("4. Scale to multi-database studies with consistent format\n")