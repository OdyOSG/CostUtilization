# ============================================================================
# FeatureExtraction Format Integration Demo
# ============================================================================
# 
# This script demonstrates how to use the CostUtilization package with 
# FeatureExtraction format support for both aggregated and person-level results.
#
# Author: CostUtilization Package Team
# Date: 2024
# ============================================================================

library(CostUtilization)
library(dplyr)
library(DBI)

# ============================================================================
# Setup: Prepare Eunomia Database with CDM v5.5 Cost Data
# ============================================================================

cat("Setting up Eunomia database with CDM v5.5 cost data...\n")

# Get Eunomia database
databaseFile <- getEunomiaDuckDb(pathToData = tempdir())
connection <- DBI::dbConnect(duckdb::duckdb(databaseFile))

# Transform to CDM v5.5 format (includes cost data injection)
connection <- transformCostToCdmV5dot5(connection)

# Load test cohort
cohort_data <- read.csv(system.file('testdata', 'cohort.csv', package = 'CostUtilization'))
DBI::dbWriteTable(connection, "cohort", cohort_data, overwrite = TRUE)

cat("Database setup complete!\n\n")

# ============================================================================
# Example 1: Basic Aggregated Results → FeatureExtraction Format
# ============================================================================

cat("=== Example 1: Basic Aggregated Analysis ===\n")

# Create basic settings
settings1 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -365L,
  endOffsetDays = 0L,
  costConceptId = 31973L  # Total charge
)

# Run analysis
results1 <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = settings1,
  verbose = FALSE
)

# Convert to FeatureExtraction format (auto-detects aggregated)
covariateData1 <- createCostCovariateData(
  costResults = results1,
  costOfCareSettings = settings1,
  cohortId = 1,
  databaseId = "EunomiaDemo"
)

# Display summary
cat("Covariate Data Summary:\n")
summary(covariateData1)

# Show first few covariates
cat("\nFirst few covariates:\n")
print(covariateData1$covariates |> slice_head(n = 5) |> collect())

cat("\n" %R% strrep("=", 60) %R% "\n\n")

# ============================================================================
# Example 2: Person-Level Results → FeatureExtraction Format
# ============================================================================

cat("=== Example 2: Person-Level Analysis ===\n")

# For person-level results, we would need to modify the SQL to return
# person_id, cost, adjusted_cost format instead of aggregated metrics.
# This is a conceptual example showing how it would work:

# Simulate person-level results (as would come from modified SQL)
person_level_results <- tibble(
  person_id = c(4929L, 4932L, 4933L, 4935L, 4942L),
  cost = c(1250.50, 2100.75, 0.0, 850.25, 3200.00),
  adjusted_cost = c(1375.55, 2310.83, 0.0, 935.28, 3520.00)
)

# Mock the results structure
mock_results <- list(
  results = person_level_results,
  diagnostics = results1$diagnostics  # Reuse diagnostics
)

# Convert to FeatureExtraction format (explicitly specify non-aggregated)
covariateData2 <- createCostCovariateData(
  costResults = mock_results,
  costOfCareSettings = settings1,
  cohortId = 1,
  databaseId = "EunomiaDemo",
  aggregated = FALSE
)

cat("Person-Level Covariate Data Summary:\n")
summary(covariateData2)

# Show covariates for specific people
cat("\nCovariates for person 4929:\n")
person_covariates <- getCovariateValues(covariateData2, c(4929L))
print(person_covariates)

cat("\n" %R% strrep("=", 60) %R% "\n\n")

# ============================================================================
# Example 3: CPI-Adjusted Analysis
# ============================================================================

cat("=== Example 3: CPI-Adjusted Analysis ===\n")

# Create CPI data file
cpi_file <- tempfile(fileext = ".csv")
cpi_data <- data.frame(
  year = 2000:2025,
  adj_factor = seq(1.0, 1.5, length.out = 26)  # 2% annual inflation
)
write.csv(cpi_data, cpi_file, row.names = FALSE)

# Settings with CPI adjustment
settings3 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -365L,
  endOffsetDays = 0L,
  costConceptId = 31973L,
  cpiAdjustment = TRUE,
  cpiFilePath = cpi_file
)

# Run analysis
results3 <- calculateCostOfCare(
  connection = connection,
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
  databaseId = "EunomiaDemo"
)

cat("CPI-Adjusted Analysis Summary:\n")
summary(covariateData3)

# Show covariates with CPI adjustment
covariates3 <- covariateData3$covariates |> collect()
cat("\nNumber of covariates with CPI adjustment:", nrow(covariates3), "\n")

# Cleanup
unlink(cpi_file)

cat("\n" %R% strrep("=", 60) %R% "\n\n")

# ============================================================================
# Example 4: Event-Filtered Analysis
# ============================================================================

cat("=== Example 4: Event-Filtered Analysis ===\n")

# Define event filters for specific clinical domains
event_filters <- list(
  list(
    name = "Cardiovascular Procedures",
    domain = "Procedure",
    conceptIds = c(4006969L, 4022492L, 4273391L)  # Example cardiovascular procedure concepts
  ),
  list(
    name = "Cardiovascular Medications", 
    domain = "Drug",
    conceptIds = c(1308216L, 1310756L, 1313200L)  # Example cardiovascular drug concepts
  )
)

# Settings with event filters
settings4 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -365L,
  endOffsetDays = 365L,
  costConceptId = 31973L,
  eventFilters = event_filters,
  nFilters = 1L  # Require at least 1 filter match
)

# Run analysis
results4 <- calculateCostOfCare(
  connection = connection,
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
  databaseId = "EunomiaDemo"
)

cat("Event-Filtered Analysis Summary:\n")
summary(covariateData4)

# Check metadata for event filters
metadata4 <- attr(covariateData4, "metaData")
cat("\nNumber of event filters applied:", length(metadata4$eventFilters), "\n")
cat("Filter names:", sapply(metadata4$eventFilters, function(x) x$name), "\n")

cat("\n" %R% strrep("=", 60) %R% "\n\n")

# ============================================================================
# Example 5: Multiple Cost Concepts Analysis
# ============================================================================

cat("=== Example 5: Multiple Cost Concepts ===\n")

# Analyze different cost concepts
cost_concepts <- list(
  list(id = 31973L, name = "Total Charge"),
  list(id = 31985L, name = "Total Cost"),
  list(id = 31980L, name = "Paid by Payer"),
  list(id = 31981L, name = "Paid by Patient")
)

# Run analysis for each cost concept
all_covariate_data <- list()

for (i in seq_along(cost_concepts)) {
  concept <- cost_concepts[[i]]
  cat("Analyzing", concept$name, "(", concept$id, ")...\n")
  
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = -365L,
    endOffsetDays = 0L,
    costConceptId = concept$id
  )
  
  results <- calculateCostOfCare(
    connection = connection,
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
    analysisId = 1000L + i  # Different analysis ID for each concept
  )
  
  all_covariate_data[[concept$name]] <- covariateData
}

cat("\nSummary of all cost concept analyses:\n")
for (name in names(all_covariate_data)) {
  covariates <- all_covariate_data[[name]]$covariates |> collect()
  cat("-", name, ":", nrow(covariates), "covariate values\n")
}

cat("\n" %R% strrep("=", 60) %R% "\n\n")

# ============================================================================
# Example 6: Wide Format Conversion
# ============================================================================

cat("=== Example 6: Wide Format Conversion ===\n")

# Convert one of the CovariateData objects to wide format
wide_data <- toWideFormat(covariateData1)

cat("Wide format data dimensions:", nrow(wide_data), "x", ncol(wide_data), "\n")
cat("Column names:\n")
print(names(wide_data))

cat("\nFirst few rows of wide format data:\n")
print(wide_data |> slice_head(n = 3))

cat("\n" %R% strrep("=", 60) %R% "\n\n")

# ============================================================================
# Example 7: Integration with OHDSI Ecosystem
# ============================================================================

cat("=== Example 7: OHDSI Ecosystem Integration ===\n")

cat("The CovariateData objects created above are now compatible with:\n")
cat("- FeatureExtraction: For covariate standardization and manipulation\n")
cat("- CohortMethod: For comparative effectiveness research\n")
cat("- PatientLevelPrediction: For outcome prediction models\n")
cat("- DatabaseConnector: For multi-database studies\n\n")

cat("Example usage patterns:\n")
cat("# For CohortMethod:\n")
cat("# runCmAnalyses(..., covariateSettings = covariateData1)\n\n")
cat("# For PatientLevelPrediction:\n") 
cat("# runPlpAnalyses(..., covariateSettings = covariateData1)\n\n")
cat("# For FeatureExtraction:\n")
cat("# aggregateCovariates(covariateData1)\n")
cat("# filterByRowId(covariateData1, rowIds = c(1, 2, 3))\n\n")

# ============================================================================
# Cleanup
# ============================================================================

cat("=== Cleanup ===\n")

# Close database connection
DBI::dbDisconnect(connection, shutdown = TRUE)

# Remove temporary database file
unlink(databaseFile)

cat("Demo completed successfully!\n")
cat("\nKey takeaways:\n")
cat("1. CostUtilization now supports FeatureExtraction format\n")
cat("2. Both aggregated and person-level results are supported\n")
cat("3. Auto-detection of result format works seamlessly\n")
cat("4. CPI adjustment and event filtering are fully supported\n")
cat("5. Integration with OHDSI ecosystem is now possible\n")
cat("6. Multiple cost concepts can be analyzed systematically\n")
cat("7. Wide format conversion enables traditional analysis workflows\n")