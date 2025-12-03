# =============================================================================
# Modular Architecture Example
# =============================================================================
# This example demonstrates the new modular SQL architecture and 
# DatabaseConnector usage in the CostUtilization package.

library(CostUtilization)
library(DatabaseConnector)

# =============================================================================
# 1. Setup Connection (DatabaseConnector)
# =============================================================================

# Create connection details
connectionDetails <- createConnectionDetails(
  dbms = "duckdb",
  server = ":memory:"
)

# Connect using DatabaseConnector
connection <- connect(connectionDetails)

# Ensure cleanup on exit
on.exit({
  if (exists("connection") && !is.null(connection)) {
    disconnect(connection)
  }
})

# =============================================================================
# 2. Setup Test Data (using modular approach)
# =============================================================================

# Transform cost data to CDM v5.5 format
# This uses the modular transformation approach
transformCostToCdmV5dot5(
  connection = connection,
  cdmDatabaseSchema = "main",
  sourceCostTable = "cost"
)

# =============================================================================
# 3. Basic Cost Analysis (Modular SQL Execution)
# =============================================================================

# Create settings using the settings-based API
basicSettings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -30L,    # 30 days before index
  endOffsetDays = 365L,      # 1 year after index
  costConceptId = 31973L     # Total charge
)

# Execute analysis (uses modular SQL behind the scenes)
basicResults <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main", 
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = basicSettings,
  verbose = TRUE  # Shows modular execution progress
)

# View results
print("=== Basic Results ===")
print(basicResults$results)
print(basicResults$diagnostics)

# =============================================================================
# 4. Advanced Analysis with Event Filters (Modular Approach)
# =============================================================================

# Define event filters for diabetes-related costs
diabetesFilters <- list(
  list(
    name = "Diabetes_Diagnoses",
    domain = "Condition",
    conceptIds = c(201820L, 201826L, 443238L)
  ),
  list(
    name = "Diabetes_Medications", 
    domain = "Drug",
    conceptIds = c(1503297L, 1502826L, 1502855L)
  )
)

# Create advanced settings
advancedSettings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  eventFilters = diabetesFilters,
  costConceptId = 31973L,
  restrictVisitConceptIds = c(9201L, 9202L, 9203L)  # IP, OP, ER visits
)

# Execute advanced analysis
advancedResults <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort", 
  cohortId = 1,
  costOfCareSettings = advancedSettings,
  verbose = TRUE
)

print("=== Advanced Results ===")
print(advancedResults$results)

# =============================================================================
# 5. Multiple Cost Types Analysis (Base R Approach)
# =============================================================================

# Define cost types to analyze
costTypes <- data.frame(
  costType = c("total_charge", "total_cost", "paid_by_payer", "paid_by_patient"),
  conceptId = c(31973L, 31985L, 31980L, 31981L),
  stringsAsFactors = FALSE
)

# Analyze multiple cost types using base R
multiResults <- list()

for (i in seq_len(nrow(costTypes))) {
  costType <- costTypes$costType[i]
  conceptId <- costTypes$conceptId[i]
  
  cat(sprintf("Analyzing cost type: %s (concept %d)\n", costType, conceptId))
  
  # Create settings for this cost type
  settings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0L,
    endOffsetDays = 365L,
    costConceptId = conceptId
  )
  
  # Execute analysis
  result <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    costOfCareSettings = settings,
    verbose = FALSE  # Reduce output for batch processing
  )
  
  # Add cost type identifier
  result$results$costType <- costType
  multiResults[[costType]] <- result$results
}

# Combine results using base R
combinedResults <- do.call(rbind, multiResults)
rownames(combinedResults) <- NULL

print("=== Multi-Cost Type Results ===")
print(combinedResults[, c("costType", "totalCost", "costPppm")])

# =============================================================================
# 6. CPI Adjustment Example (Modular Data Handling)
# =============================================================================

# Create sample CPI data
cpiData <- data.frame(
  year = 2020:2023,
  adj_factor = c(1.0, 1.02, 1.05, 1.08),
  stringsAsFactors = FALSE
)

# Write to temporary file
cpiFile <- tempfile(fileext = ".csv")
write.csv(cpiData, cpiFile, row.names = FALSE)

# Create settings with CPI adjustment
cpiSettings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L,
  cpiAdjustment = TRUE,
  cpiFilePath = cpiFile
)

# Execute with CPI adjustment
cpiResults <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = cpiSettings,
  verbose = TRUE
)

print("=== CPI-Adjusted Results ===")
print(cpiResults$results[, c("totalCost", "totalAdjustedCost", "costPppm", "adjustedCostPppm")])

# Cleanup CPI file
unlink(cpiFile)

# =============================================================================
# 7. Micro-Costing Example (Advanced Modular Features)
# =============================================================================

# Define filters for micro-costing
microFilters <- list(
  list(
    name = "Primary_Procedures",
    domain = "Procedure", 
    conceptIds = c(4301351L, 4052536L)  # Example procedure concepts
  ),
  list(
    name = "Supporting_Drugs",
    domain = "Drug",
    conceptIds = c(1503297L, 1502826L)
  )
)

# Create micro-costing settings
microSettings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 90L,  # Shorter window for micro-costing
  eventFilters = microFilters,
  microCosting = TRUE,
  primaryEventFilterName = "Primary_Procedures",
  costConceptId = 31973L
)

# Execute micro-costing analysis
microResults <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = microSettings,
  verbose = TRUE
)

print("=== Micro-Costing Results ===")
print(microResults$results)
print("Micro-costing diagnostics:")
print(microResults$diagnostics)

# =============================================================================
# 8. Error Handling and Debugging (Modular Benefits)
# =============================================================================

# Example of error handling with modular approach
tryCatch({
  
  # This might fail due to invalid settings
  invalidSettings <- createCostOfCareSettings(
    anchorCol = "invalid_column",  # This will cause validation error
    startOffsetDays = 0L,
    endOffsetDays = 365L,
    costConceptId = 31973L
  )
  
}, error = function(e) {
  cat("Settings validation caught error (as expected):\n")
  cat(conditionMessage(e), "\n")
})

# Example of connection error handling
tryCatch({
  
  # Create invalid connection details
  badConnectionDetails <- createConnectionDetails(
    dbms = "nonexistent_db",
    server = "invalid_server"
  )
  
  # This would fail at connection time, not during SQL execution
  # badConnection <- connect(badConnectionDetails)
  
}, error = function(e) {
  cat("Connection error would be caught here:\n")
  cat(conditionMessage(e), "\n")
})

# =============================================================================
# 9. Performance Monitoring (Modular Advantages)
# =============================================================================

# Time the modular execution
cat("=== Performance Monitoring ===\n")

startTime <- Sys.time()

performanceResults <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = basicSettings,
  verbose = TRUE  # Shows timing for each module
)

endTime <- Sys.time()
totalTime <- as.numeric(difftime(endTime, startTime, units = "secs"))

cat(sprintf("Total execution time: %.2f seconds\n", totalTime))
cat("Modular execution provides detailed timing for each step.\n")

# =============================================================================
# 10. Summary and Best Practices
# =============================================================================

cat("\n=== Summary of Modular Architecture Benefits ===\n")
cat("1. Better error localization - know which module failed\n")
cat("2. Improved progress tracking - see execution step by step\n") 
cat("3. Enhanced maintainability - modify individual modules\n")
cat("4. DatabaseConnector integration - better OHDSI compatibility\n")
cat("5. Base R patterns - reduced dependencies, better performance\n")
cat("6. Consistent error handling - predictable failure modes\n")
cat("7. Modular testing - validate individual components\n")

cat("\n=== Best Practices ===\n")
cat("1. Always use verbose=TRUE during development\n")
cat("2. Use DatabaseConnector for all database operations\n")
cat("3. Validate settings before execution\n")
cat("4. Handle connections properly with on.exit()\n")
cat("5. Use base R patterns for better performance\n")
cat("6. Test individual modules when debugging\n")

cat("\nExample completed successfully!\n")