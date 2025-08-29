#' FeatureExtraction Format Demo
#' 
#' This script demonstrates how to use the new FeatureExtraction-compatible
#' format functions in the CostUtilization package.
#' 
#' @author CostUtilization Development Team
#' @date 2024

library(CostUtilization)
library(dplyr)
library(DBI)
library(duckdb)

# ==============================================================================
# Setup: Create Test Database and Data
# ==============================================================================

cat("Setting up test environment...\n")

# Create Eunomia test database
databaseFile <- getEunomiaDuckDb(pathToData = tempdir())
con <- DBI::dbConnect(duckdb::duckdb(databaseFile))

# Transform to CDM v5.5 format
con <- transformCostToCdmV5dot5(con)

# Create test cohort
cohort_data <- data.frame(
  cohort_definition_id = 1L,
  subject_id = 1:10,
  cohort_start_date = as.Date("2020-01-01"),
  cohort_end_date = as.Date("2020-12-31")
)
DBI::dbWriteTable(con, "cohort", cohort_data, overwrite = TRUE)

cat("✓ Test environment ready\n\n")

# ==============================================================================
# Example 1: Basic Cost Analysis with FeatureExtraction Format
# ==============================================================================

cat("Example 1: Basic Cost Analysis\n")
cat("==============================\n")

# Create basic settings
settings1 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L  # Total charge
)

# Run cost analysis
results1 <- calculateCostOfCare(
  connection = con,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = settings1,
  verbose = FALSE
)

cat("Original results structure:\n")
str(results1, max.level = 2)

# Convert to FeatureExtraction format
covariateData1 <- createCostCovariateData(
  costResults = results1,
  costOfCareSettings = settings1,
  cohortId = 1,
  databaseId = "EunomiaDemo",
  analysisId = 1L
)

cat("\nFeatureExtraction format structure:\n")
print(covariateData1)

cat("\nDetailed summary:\n")
summary(covariateData1)

cat("\n" %R% strrep("=", 80) %R% "\n\n")

# ==============================================================================
# Example 2: CPI-Adjusted Analysis
# ==============================================================================

cat("Example 2: CPI-Adjusted Analysis\n")
cat("=================================\n")

# Create CPI data file
cpi_file <- tempfile(fileext = ".csv")
cpi_data <- data.frame(
  year = 2015:2025,
  adj_factor = c(0.85, 0.87, 0.90, 0.92, 0.95, 1.00, 1.03, 1.06, 1.09, 1.12, 1.15)
)
write.csv(cpi_data, cpi_file, row.names = FALSE)

# Create settings with CPI adjustment
settings2 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31985L,  # Total cost
  cpiAdjustment = TRUE,
  cpiFilePath = cpi_file
)

# Run analysis
results2 <- calculateCostOfCare(
  connection = con,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = settings2,
  verbose = FALSE
)

# Convert to FeatureExtraction format
covariateData2 <- createCostCovariateData(
  costResults = results2,
  costOfCareSettings = settings2,
  cohortId = 1,
  databaseId = "EunomiaDemo",
  analysisId = 2L
)

cat("CPI-adjusted analysis results:\n")
summary(covariateData2)

# Show the difference between original and CPI-adjusted costs
covariates2 <- dplyr::collect(covariateData2$covariates)
covariateRef2 <- dplyr::collect(covariateData2$covariateRef)

cost_comparison <- covariates2 |>
  left_join(covariateRef2, by = "covariateId") |>
  select(covariateName, covariateValue) |>
  filter(grepl("Total", covariateName))

cat("\nCost comparison (original vs CPI-adjusted):\n")
print(cost_comparison)

# Clean up
unlink(cpi_file)

cat("\n" %R% strrep("=", 80) %R% "\n\n")

# ==============================================================================
# Example 3: Event-Filtered Analysis
# ==============================================================================

cat("Example 3: Event-Filtered Analysis\n")
cat("===================================\n")

# Define event filters for specific clinical domains
event_filters <- list(
  list(
    name = "Cardiovascular Conditions",
    domain = "Condition",
    conceptIds = c(313217L, 314666L, 320128L)  # Example cardiovascular concept IDs
  ),
  list(
    name = "Cardiovascular Procedures", 
    domain = "Procedure",
    conceptIds = c(4006969L, 4022492L, 4273391L)  # Example procedure concept IDs
  ),
  list(
    name = "Cardiac Medications",
    domain = "Drug", 
    conceptIds = c(1308216L, 1310149L, 1319880L)  # Example drug concept IDs
  )
)

# Create settings with event filters
settings3 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -180L,  # 6 months before
  endOffsetDays = 180L,     # 6 months after
  costConceptId = 31980L,   # Paid by payer
  eventFilters = event_filters
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
  analysisId = 3L
)

cat("Event-filtered analysis results:\n")
summary(covariateData3)

# Show analysis details
analysisRef3 <- dplyr::collect(covariateData3$analysisRef)
cat("\nAnalysis description:\n")
cat(analysisRef3$description, "\n")

# Show time reference (custom window)
if ("timeRef" %in% names(covariateData3)) {
  timeRef3 <- dplyr::collect(covariateData3$timeRef)
  cat("\nTime window:\n")
  print(timeRef3)
}

cat("\n" %R% strrep("=", 80) %R% "\n\n")

# ==============================================================================
# Example 4: Micro-Costing Analysis
# ==============================================================================

cat("Example 4: Micro-Costing (Line-Level) Analysis\n")
cat("==============================================\n")

# Create settings for micro-costing
settings4 <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L,
  eventFilters = event_filters,
  microCosting = TRUE,
  primaryEventFilterName = "Cardiovascular Procedures"
)

# Run analysis
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
  analysisId = 4L
)

cat("Micro-costing analysis results:\n")
summary(covariateData4)

# Compare visit-level vs line-level metrics
analysisRef4 <- dplyr::collect(covariateData4$analysisRef)
cat("\nAnalysis type:", analysisRef4$analysisName, "\n")

metaData4 <- attr(covariateData4, "metaData")
cat("Metric type:", metaData4$metricType, "\n")

cat("\n" %R% strrep("=", 80) %R% "\n\n")

# ==============================================================================
# Example 5: Multi-Analysis Comparison
# ==============================================================================

cat("Example 5: Multi-Analysis Comparison\n")
cat("====================================\n")

# Create a comparison table of all analyses
comparison_data <- list(
  "Basic Charge Analysis" = covariateData1,
  "CPI-Adjusted Cost" = covariateData2, 
  "Event-Filtered Payer" = covariateData3,
  "Micro-Costing" = covariateData4
)

# Extract key metrics from each analysis
comparison_summary <- purrr::map_dfr(names(comparison_data), function(name) {
  data <- comparison_data[[name]]
  metaData <- attr(data, "metaData")
  analysisRef <- dplyr::collect(data$analysisRef)
  covariates <- dplyr::collect(data$covariates)
  
  # Get total cost covariate (first one typically)
  total_cost <- if (nrow(covariates) > 0) covariates$covariateValue[1] else 0
  
  dplyr::tibble(
    Analysis = name,
    AnalysisId = metaData$analysisId,
    CostConcept = metaData$call$costConceptId,
    TimeWindow = paste(metaData$call$startOffsetDays, "to", metaData$call$endOffsetDays, "days"),
    MetricType = metaData$metricType,
    HasEventFilters = metaData$call$hasEventFilters,
    CPIAdjusted = metaData$call$cpiAdjustment,
    TotalCostValue = round(total_cost, 2),
    NCovariates = nrow(dplyr::collect(data$covariateRef))
  )
})

cat("Analysis comparison:\n")
print(comparison_summary)

cat("\n" %R% strrep("=", 80) %R% "\n\n")

# ==============================================================================
# Example 6: Using the Convenience Function
# ==============================================================================

cat("Example 6: Using Convenience Function\n")
cat("=====================================\n")

# Demonstrate the convertToFeatureExtractionFormat() convenience function
cat("Converting existing results using convenience function...\n")

# Use the convenience function (with messages)
converted_data <- convertToFeatureExtractionFormat(
  costResults = results1,
  costOfCareSettings = settings1,
  cohortId = 1,
  databaseId = "ConvenienceDemo"
)

cat("\nConverted data summary:\n")
summary(converted_data)

cat("\n" %R% strrep("=", 80) %R% "\n\n")

# ==============================================================================
# Example 7: Working with Covariates Data
# ==============================================================================

cat("Example 7: Working with Covariates Data\n")
cat("=======================================\n")

# Show how to extract and work with the covariate data
cat("Extracting covariate information:\n")

# Get all covariates from the basic analysis
covariates <- dplyr::collect(covariateData1$covariates)
covariateRef <- dplyr::collect(covariateData1$covariateRef)
analysisRef <- dplyr::collect(covariateData1$analysisRef)

# Join to get meaningful names
detailed_covariates <- covariates |>
  left_join(covariateRef, by = "covariateId") |>
  left_join(analysisRef, by = "analysisId") |>
  select(rowId, covariateId, covariateName, covariateValue, analysisName, description)

cat("\nDetailed covariate information:\n")
print(detailed_covariates)

# Show how to filter for specific types of covariates
cost_covariates <- detailed_covariates |>
  filter(grepl("Cost|Charge", covariateName))

utilization_covariates <- detailed_covariates |>
  filter(grepl("Visit|Event", covariateName))

cat("\nCost-related covariates:\n")
print(cost_covariates)

cat("\nUtilization-related covariates:\n") 
print(utilization_covariates)

cat("\n" %R% strrep("=", 80) %R% "\n\n")

# ==============================================================================
# Cleanup
# ==============================================================================

cat("Cleaning up...\n")

# Close Andromeda objects
purrr::walk(comparison_data, Andromeda::close)
Andromeda::close(converted_data)

# Close database connection
DBI::dbDisconnect(con, shutdown = TRUE)

# Remove temporary database file
unlink(databaseFile)

cat("✓ Cleanup complete\n\n")

# ==============================================================================
# Summary
# ==============================================================================

cat("Demo Summary\n")
cat("============\n")
cat("This demo showed how to:\n")
cat("1. Convert basic cost analysis results to FeatureExtraction format\n")
cat("2. Handle CPI-adjusted analyses\n") 
cat("3. Work with event-filtered analyses\n")
cat("4. Perform micro-costing (line-level) analyses\n")
cat("5. Compare multiple analyses\n")
cat("6. Use convenience functions for conversion\n")
cat("7. Extract and work with covariate data\n\n")

cat("The FeatureExtraction format provides:\n")
cat("- Standardized metadata and reference tables\n")
cat("- Compatibility with OHDSI study pipelines\n")
cat("- Rich descriptive information about analyses\n")
cat("- Andromeda backend for efficient data handling\n")
cat("- Integration with downstream analysis tools\n\n")

cat("For more information, see the package documentation and vignettes.\n")