# CPI Adjustment Feature for CostUtilization Package

## Overview

The CostUtilization package now includes built-in functionality to adjust healthcare costs for inflation using the Consumer Price Index (CPI). This feature allows for meaningful cost comparisons across different time periods by standardizing all costs to a target year.

## Key Features

- **Built-in CPI Data**: Includes U.S. medical care CPI data from 1980-2023
- **Custom CPI Support**: Ability to use your own CPI data for specialized analyses
- **Automatic Interpolation**: Handles missing years through linear interpolation
- **Database-Optimized**: CPI adjustment performed efficiently within SQL queries
- **Flexible Target Years**: Adjust to any year within the CPI data range

## Quick Start

### Basic Usage

```r
# Run cost analysis with CPI adjustment to 2023 dollars
results <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "cdm",
  cohortDatabaseSchema = "results",
  cohortTable = "cohort",
  cohortId = 1,
  anchorCol = "cohort_start_date",
  startOffsetDays = -365,
  endOffsetDays = 0,
  cpiAdjustment = TRUE,      # Enable CPI adjustment
  cpiTargetYear = 2023       # Adjust all costs to 2023 dollars
)
```

### Using Custom CPI Data

```r
# Use your own CPI data
results <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "cdm",
  cohortDatabaseSchema = "results",
  cohortTable = "cohort",
  cohortId = 1,
  cpiAdjustment = TRUE,
  cpiTargetYear = 2023,
  cpiDataPath = "path/to/custom_cpi.csv"  # CSV with 'year' and 'cpi' columns
)
```

## CPI Functions

### Loading CPI Data

```r
# Load default medical CPI data
cpiData <- loadCpiData()

# Load custom CPI data
cpiData <- loadCpiData(cpiDataPath = "custom_cpi.csv")
```

### Calculating Adjustment Factors

```r
# Calculate adjustment factor between years
factor <- calculateCpiAdjustmentFactor(
  fromYear = 2015,
  toYear = 2023,
  cpiData = cpiData
)
# Example: factor = 1.34 means costs increased by 34% due to inflation
```

### Creating CPI Adjustment Tables

```r
# Create database table for CPI adjustments
tableName <- createCpiAdjustmentTable(
  connection = connection,
  cpiData = cpiData,
  targetYear = 2023
)
```

## Output Format

When CPI adjustment is enabled, the results include additional columns:

- `total_cost`: Original cost values
- `adjusted_cost`: CPI-adjusted cost values  
- `cost_pppm`: Original cost per patient per month
- `adjusted_cost_pppm`: Adjusted cost per patient per month
- `cpi_adjusted`: Boolean flag (TRUE when adjustment was applied)
- `cpi_target_year`: The target year used for adjustment

## Example: Multi-Year Analysis

```r
# Compare costs across multiple years, adjusted to 2023 dollars
years <- 2018:2023
results <- list()

for (year in years) {
  results[[year]] <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "results", 
    cohortTable = "cohort",
    cohortId = year - 2017,  # Cohorts 1-6
    cpiAdjustment = TRUE,
    cpiTargetYear = 2023
  )
}

# All costs are now comparable in 2023 dollars
```

## Performance Considerations

- CPI adjustment adds minimal overhead to queries
- The CPI adjustment table is indexed for efficient joins
- Adjustment is performed in SQL, not in R, for better performance

## Data Requirements

### Default CPI Data Format

The included CPI data (`inst/csv/cpi_data.csv`) contains:
- Years from 1980 to 2023
- Medical care CPI values (not seasonally adjusted)
- Base period varies by year (normalized for calculations)

### Custom CPI Data Requirements

Your custom CPI CSV file must have:
- Column `year` (integer): The year
- Column `cpi` (numeric): The CPI value for that year
- No duplicate years
- Positive CPI values

## Validation and Testing

The package includes comprehensive tests for CPI functionality:

```r
# Run CPI-specific tests
testthat::test_file("tests/testthat/test-CpiAdjustment.R")
```

## Troubleshooting

### Common Issues

1. **"Year outside range" error**: Ensure your analysis years are within the CPI data range
2. **Missing interpolation**: The package automatically interpolates between available years
3. **Performance with large cohorts**: CPI adjustment is optimized for large-scale analyses

### Debug Mode

```r
# Get detailed CPI adjustment information
cpiSettings <- getCpiAdjustmentSettings(
  enableCpiAdjustment = TRUE,
  targetYear = 2023
)
print(cpiSettings)
```

## References

- Default CPI data source: U.S. Bureau of Labor Statistics, Medical Care CPI
- CPI methodology: [BLS CPI Overview](https://www.bls.gov/cpi/)
- Healthcare-specific considerations: Medical care CPI vs. general CPI

## Future Enhancements

Planned improvements for the CPI adjustment feature:

1. Support for multiple currencies and international CPI data
2. Automatic CPI data updates from BLS API
3. Seasonal adjustment options
4. Regional CPI variations
5. Healthcare sector-specific indices (hospital, pharmaceutical, etc.)