# Enhanced Test Suite Documentation - CDM v5.5 Cost Utilization Package

## Overview

This enhanced test suite provides comprehensive validation for the CostUtilization package's CDM v5.5 functionality. The tests are designed to ensure robust performance, data integrity, and compliance with OMOP CDM v5.5 standards.

## Test Structure

### 1. Core Transformation Tests (`test-transformCostToCdmV5dot5-enhanced.R`)

**Purpose**: Validate the wide-to-long format transformation for CDM v5.5 cost tables.

**Key Test Areas**:
- **Schema Validation**: Ensures CDM v5.5 compliant table structure
- **Cost Concept Mapping**: Validates proper mapping of cost concepts (31973, 31985, etc.)
- **Data Integrity**: Verifies referential integrity and data preservation
- **Temporal Fields**: Tests effective_date, incurred_date, billed_date, paid_date
- **Edge Cases**: Handles missing data, null values, boundary conditions
- **Performance**: Validates transformation performance
- **Rollback**: Tests backup and restore functionality

**CDM v5.5 Cost Concepts Tested**:
```r
COST_CONCEPTS <- list(
  total_charge = 31973L,
  total_cost = 31985L,
  paid_by_payer = 31980L,
  paid_by_patient = 31981L,
  paid_patient_copay = 31974L,
  paid_patient_coinsurance = 31975L,
  paid_patient_deductible = 31976L,
  amount_allowed = 31979L
)
```

### 2. Cost Calculation Tests (`test-calculateCostOfCare-enhanced.R`)

**Purpose**: Comprehensive testing of cost analysis functionality with CDM v5.5 integration.

**Key Test Areas**:
- **Cost Concept Integration**: Tests all standard CDM v5.5 cost concepts
- **Temporal Precision**: Leverages effective_date for precise analysis
- **Event Filtering**: Complex multi-domain event filtering scenarios
- **Micro-costing**: Visit_detail level analysis
- **CPI Adjustment**: Integration with temporal cost data
- **Multi-cohort Analysis**: Comparative analysis capabilities
- **Performance**: Complex query performance validation

**Example Test Pattern**:
```r
cost_concept_results <- CDM_V55_COST_CONCEPTS |>
  purrr::pmap_dfr(function(concept_name, concept_id, source_value) {
    settings <- createCostOfCareSettings(costConceptId = concept_id)
    result <- calculateCostOfCare(...)
    # Validation logic
  })
```

### 3. Settings Configuration Tests (`test-createCostOfCareSettings-enhanced.R`)

**Purpose**: Validate settings object creation and validation for CDM v5.5.

**Key Test Areas**:
- **Cost Concept Validation**: All standard CDM v5.5 concepts
- **Event Filter Validation**: Multi-domain filter configurations
- **Micro-costing Configuration**: Proper validation and setup
- **Temporal Windows**: Flexible time window configurations
- **CPI Adjustment**: File validation and configuration
- **Visit Restrictions**: Concept ID validation
- **Functional Programming**: purrr and rlang integration

### 4. Helper Functions Tests (`test-helpers-enhanced.R`)

**Purpose**: Comprehensive testing of utility functions and helpers.

**Key Test Areas**:
- **cleanupTempTables**: Temporary table management
- **logMessage**: Logging functionality with different levels
- **executeSqlStatements**: Batch SQL execution
- **insertTableDBI**: Data insertion with various options
- **Utility Functions**: .int_flag, .qualify, executeOne
- **CDM v5.5 Integration**: Support for cost table operations
- **Performance**: Large batch handling
- **Error Handling**: Robust error management

### 5. Eunomia Integration Tests (`test-eunomia-integration-enhanced.R`)

**Purpose**: Test data injection and Eunomia database integration.

**Key Test Areas**:
- **Database Creation**: Eunomia database setup and validation
- **Cost Data Injection**: Realistic synthetic cost data generation
- **Visit Details**: Visit_detail table creation and integrity
- **Full Pipeline**: Complete CDM v5.5 transformation pipeline
- **Data Quality**: Comprehensive quality metrics
- **Performance**: Efficient data injection
- **Reproducibility**: Seed-based reproducible data generation

## Modern R Practices

### Functional Programming Patterns

The test suite extensively uses `purrr` for functional programming:

```r
# Testing multiple scenarios functionally
temporal_scenarios |>
  purrr::map_lgl(~ {
    settings <- createCostOfCareSettings(
      startOffsetDays = .x$start,
      endOffsetDays = .x$end
    )
    # Validation logic
  })
```

### Tidy Evaluation with rlang

```r
create_settings_with_params <- function(concept_id, ...) {
  dots <- rlang::list2(...)
  final_args <- c(base_args, dots)
  do.call(createCostOfCareSettings, final_args)
}
```

### Comprehensive Error Testing

```r
expect_error(
  createCostOfCareSettings(eventFilters = duplicate_filters),
  regexp = "Duplicate filter names",
  class = "rlang_error"
)
```

## Test Data and Constants

### CDM v5.5 Reference Data

```r
CDM_V55_COST_CONCEPTS <- tibble::tribble(
  ~concept_name,           ~concept_id, ~source_value,
  "total_charge",          31973L,      "total_charge",
  "total_cost",            31985L,      "total_cost", 
  "paid_by_payer",         31980L,      "paid_by_payer",
  "paid_by_patient",       31981L,      "paid_by_patient",
  # ... additional concepts
)
```

### Expected Schema Validation

```r
EXPECTED_COST_SCHEMA <- c(
  "cost_id", "person_id", "visit_occurrence_id", "visit_detail_id",
  "effective_date", "cost_event_field_concept_id", "cost_type_concept_id",
  "cost_concept_id", "cost_source_value", "currency_concept_id",
  "cost_source_concept_id", "cost", "payer_plan_period_id",
  "incurred_date", "billed_date", "paid_date"
)
```

## Performance Benchmarks

### Transformation Performance
- **Target**: Complete within 60 seconds for test data
- **Measurement**: Full wide-to-long transformation
- **Validation**: Data integrity maintained

### Query Performance
- **Target**: Complex queries complete within 120 seconds
- **Scope**: Multi-concept, multi-filter, CPI-adjusted analyses
- **Validation**: Accurate results with reasonable performance

### Data Injection Performance
- **Target**: Complete within 120 seconds
- **Scope**: Full synthetic data generation (cost, payer plans, visit details)
- **Validation**: High-quality, realistic data

## Data Quality Standards

### Completeness Requirements
- **Person ID**: ≥95% completeness
- **Cost Concept ID**: ≥95% completeness
- **Effective Date**: ≥50% completeness (CDM v5.5 requirement)

### Value Quality Standards
- **Positive Costs**: ≥80% of cost values should be positive
- **Reasonable Ranges**: Cost values should be within realistic bounds
- **Temporal Consistency**: ≥80% of events should align with payer plan periods

### Referential Integrity
- **Person-Visit Consistency**: 100% consistency between cost and visit tables
- **Visit Detail Relationships**: Proper visit_occurrence_id alignment
- **Payer Plan Alignment**: Temporal alignment with plan periods

## Error Handling Patterns

### Graceful Degradation
```r
tryCatch({
  result <- calculateCostOfCare(...)
  tibble::tibble(success = TRUE, result = list(result))
}, error = function(e) {
  tibble::tibble(success = FALSE, error = as.character(e))
})
```

### Comprehensive Validation
```r
errorMessages <- checkmate::makeAssertCollection()
checkmate::assertClass(settings, "CostOfCareSettings", add = errorMessages)
checkmate::reportAssertions(errorMessages)
```

## Integration Testing Strategy

### End-to-End Workflows
1. **Database Setup** → **Data Injection** → **Transformation** → **Analysis**
2. **Settings Creation** → **Cost Calculation** → **Results Validation**
3. **Multi-cohort Setup** → **Comparative Analysis** → **Performance Validation**

### Cross-Function Integration
- Settings objects work seamlessly with calculation functions
- Helper functions support all main workflows
- Error handling is consistent across all components

## Continuous Integration Considerations

### Test Isolation
- Each test creates its own temporary database
- Proper cleanup ensures no test interference
- Reproducible results with seed-based randomization

### Resource Management
- Automatic cleanup of temporary files and connections
- Memory-efficient test data generation
- Reasonable performance expectations for CI environments

### Coverage Goals
- **Function Coverage**: 100% of exported functions
- **Branch Coverage**: ≥90% of conditional logic
- **Integration Coverage**: All major workflow combinations

## Usage Examples

### Running Enhanced Tests

```r
# Run all enhanced tests
testthat::test_dir("tests/testthat", filter = "enhanced")

# Run specific test suite
testthat::test_file("tests/testthat/test-transformCostToCdmV5dot5-enhanced.R")

# Run with verbose output
testthat::test_file("tests/testthat/test-calculateCostOfCare-enhanced.R", 
                   reporter = "progress")
```

### Custom Test Scenarios

```r
# Test specific cost concepts
test_cost_concepts <- c(31973L, 31985L, 31980L)
test_cost_concepts |>
  purrr::walk(~ {
    testthat::test_that(glue::glue("Cost concept {.x} works"), {
      settings <- createCostOfCareSettings(costConceptId = .x)
      expect_s3_class(settings, "CostOfCareSettings")
    })
  })
```

## Future Enhancements

### Planned Additions
1. **Benchmark Testing**: Automated performance regression detection
2. **Property-Based Testing**: Hypothesis-driven test generation
3. **Integration with Real Data**: Validation against actual CDM databases
4. **Visualization Testing**: Automated chart and report validation

### Extensibility
- Modular test structure allows easy addition of new test scenarios
- Parameterized tests support testing across different configurations
- Helper functions facilitate custom test development

---

This enhanced test suite represents a comprehensive approach to validating CDM v5.5 cost analysis functionality, ensuring robust, reliable, and performant operation across diverse use cases and environments.