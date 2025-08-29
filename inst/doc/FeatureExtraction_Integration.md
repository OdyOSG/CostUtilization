# FeatureExtraction Format Integration Guide

## Overview

The CostUtilization package now provides seamless integration with the FeatureExtraction package and broader OHDSI ecosystem through standardized `CovariateData` objects. This integration enables cost and utilization metrics to be used directly in comparative effectiveness research, outcome prediction, and multi-database studies.

## Key Features

### 🔄 **Automatic Format Detection**
- Detects aggregated vs person-level results automatically
- Handles both `metric_type`/`metric_name`/`metric_value` and `person_id`/`cost`/`adjusted_cost` formats
- Works seamlessly with Andromeda objects from `calculateCostOfCare()`

### 🆔 **Systematic Covariate IDs**
- Generates unique, systematic covariate IDs: `analysisId * 1000 + typeOffset + metricOffset`
- Compatible with FeatureExtraction covariate numbering schemes
- Supports multiple analyses without ID conflicts

### 📊 **Rich Metadata**
- Comprehensive analysis provenance and settings
- Cost concept mappings and time windows
- Package version and creation timestamps
- OHDSI ecosystem compatibility markers

## Quick Start

### Basic Usage

```r
library(CostUtilization)

# 1. Run cost analysis (returns Andromeda object)
settings <- createCostOfCareSettings(costConceptId = 31973L)
results <- calculateCostOfCare(...)

# 2. Convert to FeatureExtraction format
covariateData <- createCostCovariateData(
  costResults = results,
  costOfCareSettings = settings,
  cohortId = 1,
  databaseId = "MyDatabase"
)

# 3. Use in OHDSI workflows
summary(covariateData)
```

### Convenience Function

```r
# Quick conversion with defaults
covariateData <- convertToFeatureExtractionFormat(
  costResults = results,
  costOfCareSettings = settings,
  cohortId = 1
)
```

## Supported Result Formats

### Aggregated Results Format

Input from `calculateCostOfCare()`:
```
metric_type     metric_name           metric_value
visit_level     total_person_days     985999
visit_level     total_cost            47367.5
visit_level     cost_pppm             1.46
visit_level     n_persons_with_cost   18
```

Output `CovariateData`:
- Population-level metrics as covariates
- Systematic covariate IDs (e.g., 1001101, 1001102, ...)
- Descriptive covariate names with time windows

### Person-Level Results Format

Input from `calculateCostOfCare()`:
```
person_id    cost      adjusted_cost
1001         1250.75   1375.83
1002         890.25    979.28
1003         2100.50   2310.55
```

Output `CovariateData`:
- Individual person-level cost covariates
- Separate covariates for raw and adjusted costs
- Preserved person IDs as `rowId`

## Covariate ID System

### ID Generation Formula
```
covariateId = analysisId * 1000 + typeOffset + metricOffset
```

### Type Offsets
- **100**: Visit-level analysis
- **200**: Line-level (micro-costing) analysis  
- **300**: Person-level analysis

### Metric Offsets
- **1**: Total cost/charge
- **2**: Adjusted cost (CPI)
- **3**: Cost PPPM
- **4**: Cost PPPY
- **5**: Person count with cost
- **6**: Visit/event counts
- **10+**: Additional utilization metrics

### Example IDs
```r
# Analysis ID 1000, visit-level
1001101  # Total cost
1001102  # Adjusted cost  
1001103  # Cost PPPM
1001105  # Persons with cost

# Analysis ID 2000, line-level
2002201  # Line-level total cost
2002202  # Line-level adjusted cost
```

## Advanced Features

### CPI Adjustment Support

```r
# Create CPI file
cpi_data <- data.frame(
  year = 2018:2022,
  adj_factor = c(1.0, 1.02, 1.05, 1.08, 1.12)
)
write.csv(cpi_data, "cpi_adjustment.csv", row.names = FALSE)

# Settings with CPI adjustment
settings <- createCostOfCareSettings(
  costConceptId = 31985L,
  cpiAdjustment = TRUE,
  cpiFilePath = "cpi_adjustment.csv"
)

# Metadata will include CPI adjustment flag
covariateData <- createCostCovariateData(...)
attr(covariateData, "metaData")$cpiAdjustment  # TRUE
```

### Event Filtering Integration

```r
# Define event filters
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

settings <- createCostOfCareSettings(
  eventFilters = diabetesFilters,
  costConceptId = 31980L
)

# Analysis name will reflect filtering
covariateData <- createCostCovariateData(...)
analysisRef <- covariateData$analysisRef %>% collect()
# analysisRef$analysisName: "Cost Analysis - Paid by Payer (Event-Filtered)"
```

### Micro-Costing Support

```r
# Line-level costing settings
settings <- createCostOfCareSettings(
  eventFilters = procedureFilters,
  microCosting = TRUE,
  primaryEventFilterName = "Surgical Procedures"
)

# Covariate IDs will use 200-series (line-level)
covariateData <- createCostCovariateData(...)
# Covariate IDs: 1002201, 1002202, etc.
```

## OHDSI Ecosystem Integration

### CohortMethod Integration

```r
# Use cost covariates in comparative effectiveness
library(CohortMethod)

# Create cost covariates
costCovariateData <- createCostCovariateData(...)

# Use in CohortMethod analysis
cmAnalysisList <- createCmAnalysisList(
  targetComparatorOutcomesList = tcoslist,
  analysisIdList = analysisList,
  covariateSettings = costCovariateData  # Direct integration
)

results <- runCmAnalyses(...)
```

### PatientLevelPrediction Integration

```r
# Use cost covariates for outcome prediction
library(PatientLevelPrediction)

# Person-level cost covariates
costCovariateData <- createCostCovariateData(...)

# Create prediction study
plpData <- createStudyPopulation(
  plpData = plpData,
  outcomeId = outcomeId,
  covariateSettings = costCovariateData
)

model <- runPlp(...)
```

### Multi-Database Studies

```r
# Consistent format across databases
databases <- c("Database1", "Database2", "Database3")

allResults <- map(databases, function(db) {
  # Run analysis on each database
  results <- calculateCostOfCare(
    connectionDetails = getConnectionDetails(db),
    ...
  )
  
  # Convert to standard format
  covariateData <- createCostCovariateData(
    costResults = results,
    costOfCareSettings = settings,
    cohortId = cohortId,
    databaseId = db,  # Database-specific ID
    analysisId = 1000L  # Consistent across sites
  )
  
  return(covariateData)
})

# Combine results with consistent metadata
```

## Data Structure Reference

### CovariateData Components

#### `covariates` Table
```
rowId          BIGINT     # Person ID (person-level) or cohort ID (aggregated)
covariateId    BIGINT     # Systematic covariate ID
covariateValue DECIMAL    # Cost or utilization value
```

#### `covariateRef` Table
```
covariateId   BIGINT     # Links to covariates table
covariateName VARCHAR    # Descriptive name with time window
analysisId    BIGINT     # Analysis identifier
conceptId     BIGINT     # OMOP cost concept ID
```

#### `analysisRef` Table
```
analysisId    BIGINT     # Analysis identifier
analysisName  VARCHAR    # Descriptive analysis name
domainId      VARCHAR    # Always "Cost"
startDay      BIGINT     # Time window start
endDay        BIGINT     # Time window end
isBinary      VARCHAR    # "N" for continuous cost values
missingMeansZero VARCHAR # "Y" for cost analyses
```

#### `metaData` Attributes
```r
list(
  analysisId = 1000L,
  cohortId = 1L,
  databaseId = "MyDatabase",
  resultFormat = "aggregated" | "person_level",
  costConceptId = 31973L,
  currencyConceptId = 44818668L,
  cpiAdjustment = FALSE,
  microCosting = FALSE,
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  hasVisitRestriction = FALSE,
  hasEventFilters = FALSE,
  nFilters = 0L,
  packageVersion = "0.0.0.9000",
  creationTime = "2024-01-15 10:30:00 UTC",
  populationSize = NA_integer_,
  outcomeId = NA_integer_
)
```

## Cost Concept Mapping

| Concept ID | Description | Covariate Name Prefix |
|------------|-------------|----------------------|
| 31973 | Total charge | "Total Charge" |
| 31985 | Total cost | "Total Cost" |
| 31980 | Paid by payer | "Paid by Payer" |
| 31981 | Paid by patient | "Paid by Patient" |
| 31974 | Patient copay | "Patient Copay" |
| 31975 | Patient coinsurance | "Patient Coinsurance" |
| 31976 | Patient deductible | "Patient Deductible" |
| 31979 | Amount allowed | "Amount Allowed" |

## S3 Methods

### Summary Method
```r
summary(covariateData)
# Displays:
# - Analysis information (ID, cohort, database)
# - Cost analysis settings
# - Data summary (covariates, people, values)
# - Creation information
```

### Print Method
```r
print(covariateData)
# Displays:
# - Object type and key identifiers
# - Table information with row counts
```

## Best Practices

### 1. **Systematic Analysis IDs**
```r
# Use consistent numbering scheme
baseAnalysisId <- 1000L
costConcepts <- c(31973L, 31985L, 31980L, 31981L)

analysisIds <- baseAnalysisId + seq_along(costConcepts)
# Results in: 1001, 1002, 1003, 1004
```

### 2. **Descriptive Database IDs**
```r
# Use meaningful database identifiers
databaseId <- "CCAE_2019_Q4"  # Clear, specific
# Not: "DB1" or "Database"
```

### 3. **Metadata Preservation**
```r
# Always preserve metadata for reproducibility
metaData <- attr(covariateData, "metaData")
saveRDS(metaData, "analysis_metadata.rds")
```

### 4. **Memory Management**
```r
# Properly close Andromeda objects
on.exit(Andromeda::close(covariateData), add = TRUE)
```

### 5. **Error Handling**
```r
# Validate inputs before conversion
if (!"results" %in% names(costResults)) {
  stop("Invalid cost results format")
}
```

## Troubleshooting

### Common Issues

#### 1. **Unrecognized Result Format**
```
Error: Unable to detect valid result format
```
**Solution**: Ensure your results have either:
- Aggregated: `metric_type`, `metric_name`, `metric_value` columns
- Person-level: `person_id`, `cost` columns

#### 2. **Missing Dependencies**
```
Error: could not find function "str_replace_all"
```
**Solution**: Install required packages:
```r
install.packages(c("stringr", "tidyr"))
```

#### 3. **Covariate ID Conflicts**
```
Warning: Duplicate covariate IDs detected
```
**Solution**: Use unique `analysisId` values for different analyses:
```r
# Good
analysisId1 <- 1000L
analysisId2 <- 2000L

# Bad - will cause conflicts
analysisId1 <- 1000L
analysisId2 <- 1000L
```

#### 4. **Memory Issues with Large Datasets**
```
Error: Cannot allocate memory
```
**Solution**: Use Andromeda's lazy evaluation:
```r
# Process in chunks
covariates <- covariateData$covariates %>%
  filter(covariateValue > 0) %>%
  collect()
```

### Performance Tips

1. **Filter Early**: Remove zero-value covariates during creation
2. **Use Appropriate Analysis IDs**: Keep them reasonable (< 10000)
3. **Batch Processing**: Process multiple cohorts separately
4. **Memory Monitoring**: Close unused Andromeda objects

## Examples Repository

See `inst/examples/featureExtraction_demo.R` for comprehensive examples covering:
- Basic aggregated and person-level conversion
- CPI adjustment integration
- Event filtering scenarios
- Micro-costing analysis
- Multiple cost concepts
- OHDSI ecosystem integration

## Support

- **Issues**: [GitHub Issues](https://github.com/OHDSI/CostUtilization/issues)
- **Discussions**: [OHDSI Forums](https://forums.ohdsi.org/)
- **Documentation**: Package vignettes and help files

---

*This integration enables cost analysis results to be seamlessly used across the OHDSI ecosystem, supporting reproducible research and multi-database studies.*