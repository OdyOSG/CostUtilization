# FeatureExtraction Format Integration

## Overview

The `CostUtilization` package now provides seamless integration with the OHDSI FeatureExtraction package format. This enables cost and utilization metrics to be used directly in downstream OHDSI analysis pipelines including CohortMethod, PatientLevelPrediction, and other OHDSI tools.

## Key Features

### 🔄 **Dual Format Support**
- **Aggregated Results**: Summary metrics (PPPM, PPPY, total costs, event rates)
- **Person-Level Results**: Individual patient cost and utilization values

### 🤖 **Auto-Detection**
- Automatically detects whether results are aggregated or person-level
- No manual specification required in most cases

### 🏥 **Clinical Integration**
- Supports all CDM v5.5 cost concepts (charges, payments, adjustments)
- Event filtering for domain-specific cost analysis
- CPI adjustment for inflation-corrected costs
- Micro-costing for line-item detail

### 🔗 **OHDSI Ecosystem Compatibility**
- Direct integration with CohortMethod for comparative effectiveness
- Compatible with PatientLevelPrediction for outcome modeling
- Standardized metadata for multi-database studies

## Result Format Detection

The package automatically detects the format of your results:

### Aggregated Format
Results from the database with columns:
```
metric_type VARCHAR(50)      -- e.g., "visit_level", "line_level"
metric_name VARCHAR(255)     -- e.g., "total_cost", "cost_pppm"
metric_value DECIMAL(19,4)   -- The actual metric value
```

### Person-Level Format  
Results from the database with columns:
```
person_id BIGINT NOT NULL PRIMARY KEY
cost DECIMAL(19,4) NOT NULL
adjusted_cost DECIMAL(19,4) NOT NULL  -- Optional, if CPI adjustment enabled
```

## Basic Usage

### Quick Start

```r
library(CostUtilization)

# Run your cost analysis
settings <- createCostOfCareSettings(costConceptId = 31973L)
results <- calculateCostOfCare(...)

# Convert to FeatureExtraction format (auto-detects format)
covariateData <- createCostCovariateData(
  costResults = results,
  costOfCareSettings = settings,
  cohortId = 1,
  databaseId = "MyDatabase"
)

# Use in OHDSI pipelines
summary(covariateData)
```

### Explicit Format Specification

```r
# Force aggregated format
covariateData <- createCostCovariateData(
  costResults = results,
  costOfCareSettings = settings,
  cohortId = 1,
  databaseId = "MyDatabase",
  aggregated = TRUE
)

# Force person-level format
covariateData <- createCostCovariateData(
  costResults = results,
  costOfCareSettings = settings,
  cohortId = 1,
  databaseId = "MyDatabase",
  aggregated = FALSE
)
```

## Advanced Features

### Multiple Cost Concepts

```r
cost_concepts <- c(31973L, 31985L, 31980L, 31981L)  # Different cost types
covariate_data_list <- list()

for (i in seq_along(cost_concepts)) {
  settings <- createCostOfCareSettings(costConceptId = cost_concepts[i])
  results <- calculateCostOfCare(...)
  
  covariate_data_list[[i]] <- createCostCovariateData(
    costResults = results,
    costOfCareSettings = settings,
    cohortId = 1,
    databaseId = "MyDB",
    analysisId = 1000L + i  # Unique analysis ID
  )
}
```

### Event-Filtered Analysis

```r
# Define clinical event filters
event_filters <- list(
  list(
    name = "Diabetes Diagnoses",
    domain = "Condition",
    conceptIds = c(201820L, 443238L)
  ),
  list(
    name = "Diabetes Medications", 
    domain = "Drug",
    conceptIds = c(1503297L, 1502826L)
  )
)

settings <- createCostOfCareSettings(
  costConceptId = 31973L,
  eventFilters = event_filters
)

results <- calculateCostOfCare(...)
covariateData <- createCostCovariateData(...)

# Event filters are preserved in metadata
metadata <- attr(covariateData, "metaData")
print(metadata$eventFilters)
```

### CPI Adjustment

```r
# Create CPI adjustment file
cpi_data <- data.frame(
  year = 2000:2025,
  adj_factor = seq(1.0, 1.5, length.out = 26)
)
write.csv(cpi_data, "cpi_data.csv", row.names = FALSE)

settings <- createCostOfCareSettings(
  costConceptId = 31973L,
  cpiAdjustment = TRUE,
  cpiFilePath = "cpi_data.csv"
)

# Results will include both raw and adjusted costs
results <- calculateCostOfCare(...)
covariateData <- createCostCovariateData(...)
```

## CovariateData Structure

The resulting `CovariateData` object contains:

### Core Tables
- **`covariates`**: Person-level covariate values
- **`covariateRef`**: Covariate definitions and names  
- **`analysisRef`**: Analysis-level metadata
- **`metaData`**: Comprehensive analysis settings

### Covariate ID Scheme

Systematic covariate ID generation:
```
Base ID = analysisId * 1000
+ Type Offset (visit_level: +100, line_level: +200)  
+ Metric Offset (total_cost: +1, pppm: +3, etc.)
```

Example covariate IDs:
- `1001101`: Analysis 1, visit-level, total cost
- `1001103`: Analysis 1, visit-level, cost PPPM
- `1001110`: Analysis 1, visit-level, person count
- `1002201`: Analysis 1, line-level, total cost

## Integration Examples

### CohortMethod Integration

```r
# Create cost covariates
costCovariateData <- createCostCovariateData(...)

# Use in comparative effectiveness study
cmAnalysisList <- createCmAnalysisList(
  targetComparatorOutcomesList = tcoslist,
  analysisSpecifications = analysisSpecifications
)

# Run CohortMethod with cost covariates
results <- runCmAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  exposureDatabaseSchema = exposureDatabaseSchema,
  exposureTable = exposureTable,
  outcomeDatabaseSchema = outcomeDatabaseSchema,
  outcomeTable = outcomeTable,
  cmAnalysisList = cmAnalysisList,
  covariateSettings = costCovariateData  # Use cost covariates
)
```

### PatientLevelPrediction Integration

```r
# Create person-level cost covariates
costCovariateData <- createCostCovariateData(
  aggregated = FALSE  # Person-level for prediction
)

# Use in prediction model
plpData <- getPlpData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortId = 1,
  covariateSettings = costCovariateData,
  outcomeDatabaseSchema = outcomeDatabaseSchema,
  outcomeTable = outcomeTable,
  outcomeIds = 2
)

# Train prediction model
model <- runPlp(
  plpData = plpData,
  modelSettings = setLassoLogisticRegression(),
  testSplit = "time",
  testFraction = 0.25
)
```

## Utility Functions

### Data Extraction

```r
# Get covariates for specific people
person_covariates <- getCovariateValues(
  covariateData, 
  rowIds = c(1001, 1002, 1003)
)

# Convert to wide format for traditional analysis
wide_data <- toWideFormat(covariateData)
```

### Data Inspection

```r
# Check if object is CovariateData
is.CovariateData(covariateData)

# Print summary
summary(covariateData)

# View structure
print(covariateData)
```

## Metadata and Reproducibility

The `CovariateData` object includes comprehensive metadata:

```r
metadata <- attr(covariateData, "metaData")

# Analysis settings
metadata$costConceptId      # Cost concept used
metadata$timeWindow         # Analysis time window
metadata$eventFilters       # Applied event filters
metadata$cpiAdjustment      # CPI adjustment status
metadata$microCosting       # Micro-costing status

# Provenance
metadata$packageVersion     # Package version used
metadata$createdOn          # Creation timestamp
metadata$databaseId         # Source database
```

## Performance Considerations

### Large Datasets
- Uses Andromeda backend for memory efficiency
- Supports datasets with millions of patients
- Lazy evaluation for large person-level results

### Database Optimization
- Leverages database-native aggregation
- Minimal data transfer from database
- Efficient covariate ID generation

## Error Handling

The package provides comprehensive input validation:

```r
# Validates input structure
createCostCovariateData(
  costResults = list(wrong = "structure")  # Error: missing 'results'
)

# Validates settings object
createCostCovariateData(
  costOfCareSettings = "not_settings"      # Error: wrong class
)

# Validates parameters
createCostCovariateData(
  cohortId = "not_numeric"                 # Error: must be integer
)
```

## Best Practices

### 1. **Use Descriptive Analysis IDs**
```r
# Good: Descriptive analysis IDs
analysisId = 1100L  # 1100s for total charge analyses
analysisId = 1200L  # 1200s for total cost analyses
analysisId = 1300L  # 1300s for patient payment analyses
```

### 2. **Preserve Metadata**
```r
# Always preserve metadata for reproducibility
metadata <- attr(covariateData, "metaData")
saveRDS(metadata, "analysis_metadata.rds")
```

### 3. **Handle Missing Values**
```r
# The package automatically handles missing values
# Zero costs are preserved as meaningful clinical information
# NA values are filtered out appropriately
```

### 4. **Multi-Database Studies**
```r
# Use consistent databaseId across sites
databases <- c("Site1", "Site2", "Site3")
all_results <- list()

for (db in databases) {
  results <- calculateCostOfCare(...)
  all_results[[db]] <- createCostCovariateData(
    costResults = results,
    databaseId = db,  # Consistent naming
    analysisId = 1000L  # Same analysis ID
  )
}
```

## Troubleshooting

### Common Issues

**Issue**: Auto-detection fails
```r
# Solution: Explicitly specify format
covariateData <- createCostCovariateData(
  aggregated = TRUE  # or FALSE
)
```

**Issue**: Missing covariate names
```r
# Check covariate reference
covariateRef <- covariateData$covariateRef |> collect()
print(covariateRef)
```

**Issue**: Memory issues with large datasets
```r
# Use person-level format for better memory efficiency
covariateData <- createCostCovariateData(
  aggregated = FALSE
)
```

## Future Enhancements

Planned features for future releases:

- **Temporal Covariates**: Time-varying cost patterns
- **Hierarchical Clustering**: Cost pattern identification  
- **Missing Data Imputation**: Advanced missing value handling
- **Cost Prediction Models**: Built-in cost forecasting
- **Real-World Evidence**: RWE-specific cost metrics

## Support and Resources

- **GitHub Issues**: [Report bugs and request features](https://github.com/OHDSI/CostUtilization/issues)
- **OHDSI Forums**: [Community discussions](https://forums.ohdsi.org/)
- **Documentation**: [Package documentation](https://ohdsi.github.io/CostUtilization/)
- **Examples**: See `inst/examples/featureExtraction_demo.R`

---

*This integration enables cost and utilization analysis to be a first-class citizen in the OHDSI ecosystem, supporting evidence generation across the full spectrum of observational health research.*