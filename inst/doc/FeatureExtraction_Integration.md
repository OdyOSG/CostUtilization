# FeatureExtraction Package Integration

## Overview

The CostUtilization package now provides seamless integration with the OHDSI FeatureExtraction package through standardized `CovariateData` objects. This integration enables cost and utilization analyses to be easily incorporated into larger OHDSI study pipelines and provides compatibility with downstream analysis tools.

## Key Benefits

- **Standardized Format**: Results conform to FeatureExtraction package standards
- **Rich Metadata**: Comprehensive analysis metadata and parameter tracking
- **Andromeda Backend**: Efficient handling of large datasets
- **OHDSI Compatibility**: Direct integration with study packages like CohortMethod, PatientLevelPrediction
- **Extensible Design**: Easy to extend for future analysis types

## Core Components

### CovariateData Structure

The `CovariateData` object contains the following components:

#### 1. `covariates` Table
Person-level cost and utilization metrics:
```r
# Structure
rowId          # Person/cohort identifier  
covariateId    # Unique covariate identifier
covariateValue # Numeric value of the covariate
```

#### 2. `covariateRef` Table  
Reference information for each covariate:
```r
# Structure
covariateId    # Links to covariates table
covariateName  # Human-readable covariate name
analysisId     # Links to analysisRef table
conceptId      # OMOP concept ID (if applicable)
```

#### 3. `analysisRef` Table
Analysis-level metadata:
```r
# Structure  
analysisId       # Unique analysis identifier
analysisName     # Human-readable analysis name
domainId         # Analysis domain (always "Cost")
startDay         # Analysis window start
endDay           # Analysis window end  
isBinary         # "N" for continuous cost metrics
missingMeansZero # "Y" - missing values treated as zero
description      # Detailed analysis description
```

#### 4. `timeRef` Table (Optional)
Time window reference for non-standard windows:
```r
# Structure
timeId      # Time window identifier
startDay    # Window start day
endDay      # Window end day  
description # Window description
```

#### 5. `metaData` Attributes
Comprehensive analysis metadata including:
- Database and analysis identifiers
- Package version information
- Analysis parameters and settings
- Population characteristics
- Execution metadata

## Usage Examples

### Basic Conversion

```r
library(CostUtilization)

# Run standard cost analysis
settings <- createCostOfCareSettings(costConceptId = 31973L)
results <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "cdm",
  cohortDatabaseSchema = "results", 
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = settings
)

# Convert to FeatureExtraction format
covariateData <- createCostCovariateData(
  costResults = results,
  costOfCareSettings = settings,
  cohortId = 1,
  databaseId = "MyDatabase"
)

# View summary
summary(covariateData)
```

### Advanced Analysis with Event Filtering

```r
# Define clinical event filters
eventFilters <- list(
  list(name = "Diabetes Conditions", domain = "Condition", conceptIds = c(201820L, 443238L)),
  list(name = "Diabetes Medications", domain = "Drug", conceptIds = c(1503297L, 1502826L))
)

# Create settings with filtering and CPI adjustment
settings <- createCostOfCareSettings(
  startOffsetDays = -365L,
  endOffsetDays = 365L,
  costConceptId = 31985L,
  eventFilters = eventFilters,
  cpiAdjustment = TRUE,
  cpiFilePath = "cpi_data.csv"
)

# Run analysis and convert
results <- calculateCostOfCare(...)
covariateData <- createCostCovariateData(
  costResults = results,
  costOfCareSettings = settings,
  cohortId = 1,
  databaseId = "DiabetesStudy",
  analysisId = 100L
)
```

### Micro-Costing Analysis

```r
# Line-level costing analysis
settings <- createCostOfCareSettings(
  eventFilters = procedureFilters,
  microCosting = TRUE,
  primaryEventFilterName = "Cardiac Procedures"
)

results <- calculateCostOfCare(...)
covariateData <- createCostCovariateData(
  costResults = results,
  costOfCareSettings = settings,
  cohortId = 1
)

# Check analysis type
metaData <- attr(covariateData, "metaData")
print(metaData$metricType)  # "line_level"
```

## Covariate Types Generated

### Cost Metrics
- **Total Cost/Charge**: Primary cost metric based on selected cost concept
- **CPI-Adjusted Cost**: Inflation-adjusted costs (when CPI adjustment enabled)
- **PPPM**: Per-person-per-month costs
- **PPPY**: Per-person-per-year costs

### Utilization Metrics  
- **Number of Visits**: Count of healthcare visits
- **Number of Events**: Count of clinical events (procedures, diagnoses, etc.)
- **Events per 1000 PY**: Event rate per 1000 person-years

### Covariate ID Scheme
- Base ID = `analysisId * 1000`
- Cost metrics: `baseId + 1-9`
- Utilization metrics: `baseId + 10-19`
- Rate metrics: `baseId + 20-29`

## Integration with OHDSI Packages

### CohortMethod Integration

```r
# Use cost covariates in comparative effectiveness studies
library(CohortMethod)

# Create covariate data for treatment and comparator cohorts
treatmentCovariates <- createCostCovariateData(...)
comparatorCovariates <- createCostCovariateData(...)

# Combine with other covariates
allCovariates <- FeatureExtraction::createCovariateData(
  connection = connection,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortTable = cohortTable,
  cohortId = c(treatmentCohortId, comparatorCohortId),
  covariateSettings = list(
    FeatureExtraction::createDefaultCovariateSettings(),
    # Cost covariates automatically included via createCostCovariateData
  )
)

# Run CohortMethod analysis
cmData <- createCohortMethodData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  exposureDatabaseSchema = exposureDatabaseSchema,
  exposureTable = exposureTable,
  outcomeDatabaseSchema = outcomeDatabaseSchema,
  outcomeTable = outcomeTable,
  targetId = treatmentCohortId,
  comparatorId = comparatorCohortId,
  outcomeIds = outcomeIds,
  covariateData = allCovariates
)
```

### PatientLevelPrediction Integration

```r
# Use cost features in prediction models
library(PatientLevelPrediction)

# Cost covariates as predictive features
plpData <- createPlpData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  targetId = targetCohortId,
  outcomeIds = outcomeIds,
  covariateData = costCovariateData
)

# Train prediction model
model <- runPlp(
  plpData = plpData,
  outcomeId = outcomeId,
  modelSettings = setLassoLogisticRegression(),
  testSplit = "time",
  testFraction = 0.25
)
```

## Best Practices

### Analysis Organization
- Use meaningful `analysisId` values for tracking multiple analyses
- Include descriptive `databaseId` for multi-database studies  
- Document analysis parameters in study protocols

### Performance Considerations
- Andromeda backend handles large datasets efficiently
- Close Andromeda objects when no longer needed: `Andromeda::close(covariateData)`
- Consider memory usage for very large cohorts

### Quality Assurance
- Always review analysis metadata: `attr(covariateData, "metaData")`
- Validate covariate values make clinical sense
- Check for missing or zero values in key metrics

### Documentation
- Include analysis descriptions in study documentation
- Save analysis settings objects for reproducibility
- Document any data transformations or filtering applied

## Troubleshooting

### Common Issues

**Empty Covariate Data**
```r
# Check if analysis returned results
nrow(dplyr::collect(covariateData$covariates))

# Review diagnostics
print(costResults$diagnostics)
```

**Missing Expected Covariates**
```r
# Check available covariates
covariateRef <- dplyr::collect(covariateData$covariateRef)
print(covariateRef$covariateName)

# Verify analysis settings
metaData <- attr(covariateData, "metaData")
print(metaData$call)
```

**Memory Issues**
```r
# Monitor Andromeda object size
Andromeda::andromedaTableSize(covariateData)

# Close when done
Andromeda::close(covariateData)
```

## Advanced Topics

### Custom Covariate Generation
For specialized analyses, you can extend the covariate generation:

```r
# Custom covariate function (advanced users)
createCustomCostCovariates <- function(costResults, baseId) {
  # Custom logic here
  # Return tibble with rowId, covariateId, covariateValue
}
```

### Multi-Database Studies
```r
# Generate covariates for multiple databases
databases <- c("Database1", "Database2", "Database3")

allCovariates <- purrr::map(databases, function(db) {
  # Run analysis for each database
  results <- calculateCostOfCare(...)
  createCostCovariateData(
    costResults = results,
    costOfCareSettings = settings,
    cohortId = cohortId,
    databaseId = db
  )
})

# Combine results for meta-analysis
```

### Temporal Analysis
```r
# Multiple time windows
timeWindows <- list(
  list(start = -365L, end = 0L),    # Pre-index
  list(start = 0L, end = 365L),     # Post-index  
  list(start = 365L, end = 730L)    # Follow-up
)

temporalCovariates <- purrr::imap(timeWindows, function(window, i) {
  settings <- createCostOfCareSettings(
    startOffsetDays = window$start,
    endOffsetDays = window$end
  )
  results <- calculateCostOfCare(...)
  createCostCovariateData(
    costResults = results,
    costOfCareSettings = settings,
    cohortId = cohortId,
    analysisId = i
  )
})
```

## Future Enhancements

Planned improvements include:
- Person-level covariate generation (currently cohort-level)
- Additional cost metric types
- Enhanced temporal analysis capabilities
- Direct FeatureExtraction package integration
- Automated covariate selection algorithms

## Support

For questions or issues:
- GitHub Issues: https://github.com/OHDSI/CostUtilization/issues
- OHDSI Forums: https://forums.ohdsi.org/
- Package Documentation: `?createCostCovariateData`