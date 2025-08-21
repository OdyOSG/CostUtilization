# OMOP CDM 5.5 Cost Table Migration Guide

This branch contains updates to align the CostUtilization package with the OMOP CDM 5.5 cost table structure.

## Key Changes in CDM 5.5 Cost Table

The OMOP CDM 5.5 cost table has the following fields:
- `cost_id` (primary key)
- `person_id`
- `visit_occurrence_id`
- `visit_detail_id`
- **`event_cost_id`** (replaces `cost_event_id`)
- `effective_date`
- `cost_event_field_concept_id`
- `cost_type_concept_id`
- `cost_concept_id`
- `cost_source_value`
- `currency_concept_id`
- `cost_source_concept_id`
- `cost`
- `payer_plan_period_id`
- `incurred_date`
- `billed_date`
- `paid_date`

### Notable Changes:
1. **`cost_event_id` → `event_cost_id`**: The field name has changed
2. **Removed `cost_domain_id`**: This field is no longer part of CDM 5.5

## Files Updated

### 1. SQL Files
- **`inst/sql/MainCostUtilization_CDM55.sql`**: New SQL file compatible with CDM 5.5
  - Updated all references from `cost_event_id` to `event_cost_id`
  - Removed references to `cost_domain_id`
  - Modified cost table joins to use CDM 5.5 structure

### 2. R Scripts
- **`R/CalculateCostOfCare_CDM55.R`**: New R functions for CDM 5.5
  - `calculateCostOfCareCDM55()`: Main analysis function
  - `executeSqlPlanCDM55()`: SQL execution function
  - Uses the new SQL file for CDM 5.5 compatibility

### 3. Test Files
- **`tests/testthat/test-CDM55-cost-analysis.R`**: Test suite for CDM 5.5
  - Demonstrates proper usage with new table structure
  - Includes sample data generation
  - Tests event filtering functionality

## Usage Example

```r
library(CostUtilization)
library(DatabaseConnector)

# Connect to your CDM 5.5 database
connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = "localhost/cdm55",
  user = "username",
  password = "password"
)

connection <- connect(connectionDetails)

# Create cost analysis settings
costSettings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0,
  endOffsetDays = 365,
  costConceptId = 31980,  # Total cost
  currencyConceptId = 44818668  # USD
)

# Run CDM 5.5 compatible analysis
results <- calculateCostOfCareCDM55(
  connection = connection,
  cdmDatabaseSchema = "cdm_schema",
  cohortDatabaseSchema = "results_schema",
  cohortTable = "my_cohort",
  cohortId = 1,
  costOfCareSettings = costSettings,
  verbose = TRUE
)

# Results contain cost metrics
print(results$results)
print(results$diagnostics)
```

## Migration Steps

If you're migrating from an older CDM version:

1. **Update your cost table structure** to match CDM 5.5 specifications
2. **Rename fields**: 
   - `cost_event_id` → `event_cost_id`
   - Remove `cost_domain_id` if present
3. **Use the new functions**: Replace `calculateCostOfCare()` with `calculateCostOfCareCDM55()`
4. **Test thoroughly**: Run the provided test suite to ensure compatibility

## Backward Compatibility

The original functions remain unchanged for users still on older CDM versions. The new CDM 5.5 functions are provided separately to avoid breaking existing code.

## Testing

Run the test suite to verify CDM 5.5 compatibility:

```r
testthat::test_file("tests/testthat/test-CDM55-cost-analysis.R")
```

## Support

For questions or issues related to CDM 5.5 migration, please open an issue on the GitHub repository.