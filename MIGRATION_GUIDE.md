# Migration Guide: Modular Architecture

## Overview

This guide explains the migration from the monolithic SQL approach to the new modular architecture in the `dev_alfa_v3_modular` branch.

## Key Changes

### 1. **Modular SQL Structure**

**Before (Monolithic):**
```
inst/sql/MainCostUtilization.sql  # Single large file
```

**After (Modular):**
```
inst/sql/modules/
├── 00_initialize_diagnostics.sql
├── 01_cohort_analysis_window.sql
├── 02_qualifying_visits.sql
├── 03_cost_calculation.sql
├── 04_final_calculations.sql
└── 05_cleanup.sql
```

### 2. **DatabaseConnector vs DBI**

**Before:**
```r
# Used DBI functions
DBI::dbGetQuery(connection, sql)
DBI::dbExecute(connection, sql)
DBI::dbWriteTable(connection, name, data)
```

**After:**
```r
# Uses DatabaseConnector functions
DatabaseConnector::querySql(connection, sql)
DatabaseConnector::executeSql(connection, sql)
DatabaseConnector::insertTable(connection, tableName, data)
```

### 3. **Base R vs Tidyverse**

**Before:**
```r
# Heavy tidyverse usage
data |> 
  dplyr::mutate(...) |>
  dplyr::filter(...) |>
  purrr::map_dfr(...)
```

**After:**
```r
# Base R patterns
data <- transform(data, ...)
data <- subset(data, ...)
result <- do.call(rbind, lapply(...))
```

## Benefits of Modular Architecture

### 1. **Maintainability**
- Each SQL module has a single responsibility
- Easier to debug and modify specific functionality
- Clear separation of concerns

### 2. **Testability**
- Individual modules can be tested in isolation
- Better error localization
- Easier to validate specific steps

### 3. **Performance**
- Better error handling and recovery
- Improved progress tracking
- More efficient resource management

### 4. **Compatibility**
- Better integration with OHDSI ecosystem
- Consistent with HADES package standards
- Improved database dialect support

## Migration Steps

### For Package Users

**No changes required** - the public API remains the same:

```r
# This still works exactly the same
settings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L
)

results <- calculateCostOfCare(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = settings
)
```

### For Package Developers

If you were extending the package internally:

1. **Update SQL modifications:**
   - Instead of modifying `MainCostUtilization.sql`
   - Modify the appropriate module in `inst/sql/modules/`

2. **Update database operations:**
   - Replace DBI calls with DatabaseConnector equivalents
   - Use the new helper functions in `DatabaseConnectorHelpers.R`

3. **Update R code patterns:**
   - Use base R instead of tidyverse where possible
   - Follow OHDSI/HADES coding standards

## SQL Module Descriptions

### `00_initialize_diagnostics.sql`
- Creates the diagnostics tracking table
- Records initial cohort counts
- **Dependencies:** None

### `01_cohort_analysis_window.sql`
- Creates cohort person table with anchor dates
- Defines analysis windows based on observation periods
- Calculates person-time denominators
- **Dependencies:** Cohort table

### `02_qualifying_visits.sql`
- Identifies visits overlapping with analysis windows
- Applies visit restrictions if specified
- Processes event filters for visit qualification
- **Dependencies:** Analysis windows, optional filter tables

### `03_cost_calculation.sql`
- Extracts costs from CDM v5.5 cost table
- Applies CPI adjustments if enabled
- Aggregates costs at visit or visit-detail level
- **Dependencies:** Qualifying visits, optional CPI table

### `04_final_calculations.sql`
- Calculates final denominators and numerators
- Computes PPPM, PPPQ, PPPY metrics
- Creates final results table
- **Dependencies:** Cost aggregations, person-time

### `05_cleanup.sql`
- Drops all temporary tables
- Finalizes diagnostics
- **Dependencies:** All previous modules

## Error Handling Improvements

The modular approach provides better error handling:

```r
# Before: Single point of failure
executeSqlPlan(connection, params, ...)

# After: Module-level error handling
executeSqlModules(connection, params, ...)
# If module 03 fails, you know it's in cost calculation
# Previous modules (00-02) completed successfully
```

## Performance Considerations

### Memory Usage
- Modular approach uses similar memory
- Better cleanup between modules
- More predictable resource usage

### Execution Time
- Slight overhead from module switching
- Better progress tracking
- Improved error recovery

### Database Load
- Similar database load patterns
- Better connection management
- More efficient temp table handling

## Troubleshooting

### Common Issues

1. **Module not found error:**
   ```
   Error: SQL module not found: inst/sql/modules/XX_module.sql
   ```
   - Ensure all module files are installed
   - Check package installation

2. **DatabaseConnector connection error:**
   ```
   Error: Connection must be a DatabaseConnector connection object
   ```
   - Use `DatabaseConnector::connect()` instead of `DBI::dbConnect()`
   - Ensure connection is active

3. **Parameter rendering error:**
   ```
   Error in module XX: Invalid parameter YY
   ```
   - Check parameter names match module expectations
   - Verify parameter types (integer vs character)

### Debug Mode

Enable verbose logging for detailed execution tracking:

```r
results <- calculateCostOfCare(
  connectionDetails = connectionDetails,
  # ... other parameters ...
  verbose = TRUE  # Enable detailed logging
)
```

## Backward Compatibility

The modular architecture maintains full backward compatibility:

- All existing function signatures unchanged
- Same parameter names and types
- Identical output format
- Same error messages for user-facing errors

## Future Enhancements

The modular structure enables:

1. **Parallel execution** of independent modules
2. **Conditional module execution** based on settings
3. **Custom module injection** for specialized analyses
4. **Better testing coverage** at module level
5. **Performance profiling** per module

## Support

For questions about the migration:

1. Check this migration guide
2. Review the updated documentation
3. Examine the test files for examples
4. Open an issue on GitHub with specific questions