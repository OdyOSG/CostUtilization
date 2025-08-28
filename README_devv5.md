# CostUtilization devv5 <img src="man/figures/logo.png" align="right" height="92" alt="" />

[![Build Status](https://github.com/OHDSI/CostUtilization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CostUtilization/actions?query=workflow%3AR-CMD-check+branch%3Adevv5)
[![codecov.io](https://codecov.io/github/OHDSI/CostUtilization/coverage.svg?branch=devv5)](https://app.codecov.io/github/OHDSI/CostUtilization?branch=devv5)

---

## 🚀 What's New in devv5

The devv5 branch introduces significant improvements and new features:

### ✨ Key Improvements

- **🏗️ Settings-Based Architecture** - New `createCostOfCareSettings()` for better parameter management
- **🔬 Enhanced visit_detail Support** - Proper micro-costing at the service line level
- **⚡ Modern R Practices** - Uses `rlang`, `purrr`, and native pipe operators (`|>`)
- **🛡️ Improved Error Handling** - Clear, actionable error messages with `cli`
- **🧪 Enhanced Testing** - Comprehensive test coverage with realistic synthetic data
- **📊 Better Eunomia Integration** - More realistic cost data generation

### 🔧 Technical Enhancements

- **CDM v5.5 Alignment** - Full support for modern OMOP CDM cost table structure
- **visit_detail Integration** - Proper handling of visit_detail_id for micro-costing
- **Performance Optimizations** - Faster SQL execution and better indexing
- **Robust Validation** - Input validation with `checkmate` and clear error messages

---

## Introduction

The `CostUtilization` R package provides a standardized framework for generating cost and healthcare resource utilization features for patient cohorts within OMOP Common Data Model (CDM) databases. It is designed to produce consistent, aggregated analytics that can be used for cohort characterization and descriptive studies.

### Key Features

- **🎯 Flexible Time Windows**: Analyze costs across multiple, user-defined time windows relative to a cohort index date
- **🔍 Granular Event Selection**: Calculate costs based on broad CDM domains or specific concept sets
- **💰 Advanced Cost Filtering**: Restrict analyses to specific cost types and currencies
- **📈 Multiple Aggregation Levels**: Generate per-patient metrics (PPPD, PPPM, PPPQ, PPPY)
- **🏥 Micro-Costing Support**: Detailed analysis at the visit_detail level
- **⚙️ Settings-Based Configuration**: Robust parameter management and validation

---

## Installation

To install the devv5 development version from GitHub:

```r
# install.packages("remotes")
remotes::install_github("OHDSI/CostUtilization", ref = "devv5")
```

---

## Quick Start

### Basic Cost Analysis

```r
library(CostUtilization)
library(dplyr)

# Setup Eunomia with enhanced cost data
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
connection <- DatabaseConnector::connect(connectionDetails)

# Transform to CDM v5.5 format with visit_detail support
transformCostToCdmV5dot5(connectionDetails)

# Create analysis settings
settings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -365L,
  endOffsetDays = 365L,
  costConceptId = 31980L,  # Total cost
  currencyConceptId = 44818668L  # USD
)

# Execute analysis
results <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main", 
  cohortTable = "cohort",
  cohortId = 1L,
  costOfCareSettings = settings
)

# View results
print(results$results)
print(results$diagnostics)
```

### Advanced: Micro-Costing Analysis

```r
# Define event filters for micro-costing
surgicalFilters <- list(
  list(
    name = "Surgical Procedures",
    domain = "Procedure",
    conceptIds = c(4301351L, 4142875L, 4143316L)
  ),
  list(
    name = "Anesthesia Services", 
    domain = "Procedure",
    conceptIds = c(4045049L, 4129389L)
  )
)

# Create micro-costing settings
microSettings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 30L,
  microCosting = TRUE,
  eventFilters = surgicalFilters,
  primaryEventFilterName = "Surgical Procedures"
)

# Run micro-costing analysis
microResults <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "surgical_cohort", 
  cohortId = 1L,
  costOfCareSettings = microSettings
)
```

### Modern R Patterns with purrr

```r
# Analyze multiple time windows using purrr
timeWindows <- list(
  preOp = c(-30L, -1L),
  periOp = c(0L, 7L),
  postOp = c(8L, 90L)
)

# Create settings for each window
windowSettings <- timeWindows |>
  purrr::imap(~ createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = .x[1],
    endOffsetDays = .x[2]
  ))

# Run analyses
results <- windowSettings |>
  purrr::imap_dfr(~ {
    calculateCostOfCare(
      connection = connection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortId = 1L, 
      costOfCareSettings = .x,
      verbose = FALSE
    )$results |>
      mutate(timeWindow = .y)
  })
```

---

## New Features Deep Dive

### 🏗️ Settings-Based Architecture

The new `createCostOfCareSettings()` function provides:

- **Robust validation** with clear error messages
- **Type safety** with proper parameter checking  
- **Reusable configurations** for consistent analyses
- **Better documentation** of analysis parameters

```r
# Example: Complex analysis settings
complexSettings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -90L,
  endOffsetDays = 365L,
  restrictVisitConceptIds = c(9201L, 9203L),  # Inpatient, Emergency
  eventFilters = list(
    list(name = "Diabetes Drugs", domain = "Drug", conceptIds = c(1503297L, 1502826L)),
    list(name = "Diabetes Labs", domain = "Measurement", conceptIds = c(3004501L, 3003309L))
  ),
  microCosting = FALSE,
  costConceptId = 31980L,
  currencyConceptId = 44818668L
)
```

### 🔬 Enhanced visit_detail Support

True micro-costing capabilities with proper visit_detail integration:

- **Service-level granularity** - Analyze individual services within visits
- **Cost attribution** - Understand which services drive costs
- **Temporal precision** - Link costs to specific service dates
- **Quality assurance** - Validate micro-costing against visit totals

```r
# Micro-costing reveals service-level cost drivers
serviceCosts <- DatabaseConnector::querySql(connection, "
  SELECT 
    vd.service_type,
    COUNT(*) as n_services,
    AVG(c.cost) as avg_cost,
    SUM(c.cost) as total_cost
  FROM main.cost c
  JOIN main.visit_detail vd ON c.visit_detail_id = vd.visit_detail_id
  WHERE c.cost_concept_id = 31980
  GROUP BY vd.service_type
  ORDER BY total_cost DESC
")
```

### ⚡ Modern R Practices

The codebase now uses modern R patterns:

- **Native pipe operator** (`|>`) for better readability
- **purrr functional programming** for robust iteration
- **rlang for tidy evaluation** in internal functions
- **cli for user communication** with styled messages
- **checkmate for validation** with clear error messages

### 🧪 Enhanced Testing & Synthetic Data

Improved Eunomia integration provides:

- **Realistic payer plan periods** with plan changes over time
- **Proper cost distributions** across domains and time
- **visit_detail structures** for micro-costing testing
- **Comprehensive test coverage** for edge cases

---

## Migration Guide

### From Earlier Versions

**Old approach (deprecated):**
```r
# Old parameter passing
calculateCostOfCare(
  connection = connection,
  anchorCol = "cohort_start_date",
  startOffsetDays = 0,
  endOffsetDays = 365,
  restrictVisitConceptIds = c(9201, 9203),
  # ... many parameters
)
```

**New approach (recommended):**
```r
# Settings-based approach
settings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  restrictVisitConceptIds = c(9201L, 9203L)
)

calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1L,
  costOfCareSettings = settings
)
```

### Key Changes

1. **Settings objects** - Use `createCostOfCareSettings()` for parameter management
2. **Integer types** - Use `L` suffix for integer parameters (e.g., `365L`)
3. **Schema parameters** - Explicitly specify database schemas
4. **Return format** - Results now include both `results` and `diagnostics`

---

## Documentation

### Vignettes

- **[New Features in devv5](vignettes/devv5-new-features.html)** - Overview of improvements
- **[Visit Detail Micro-Costing](vignettes/visit-detail-micro-costing.html)** - Detailed micro-costing guide
- **[Getting Started](vignettes/getting-started.html)** - Basic usage examples
- **[Working with Eunomia](vignettes/working-with-eunomia.html)** - Testing and development

### Function Documentation

- `createCostOfCareSettings()` - Create validated analysis settings
- `calculateCostOfCare()` - Execute cost analysis with settings
- `transformCostToCdmV5dot5()` - Transform cost tables to CDM v5.5
- `injectCostData()` - Generate synthetic cost data for testing

---

## System Requirements

- **R** (version 4.1.0 or higher)
- **Java** 8 or higher (for `DatabaseConnector`)
- **OMOP CDM** v5.4+ database with populated `cost` table in long format
- **visit_detail table** (recommended for micro-costing features)

### CDM v5.5 Cost Table

The package requires the modern long-format cost table structure:

```sql
CREATE TABLE cost (
  cost_id BIGINT PRIMARY KEY,
  person_id BIGINT NOT NULL,
  cost_event_id BIGINT,
  visit_occurrence_id BIGINT,
  visit_detail_id BIGINT,  -- New in v5.4+
  cost_domain_id VARCHAR(50),
  effective_date DATE NOT NULL,
  cost_concept_id INT NOT NULL,
  cost FLOAT,
  -- ... additional fields
);
```

---

## Performance Considerations

### Optimizations in devv5

- **Improved SQL generation** with conditional rendering
- **Better indexing strategies** for cost table queries
- **Efficient temporary table management** 
- **Reduced memory footprint** for large analyses

### Best Practices

1. **Index the cost table** on key columns (person_id, visit_occurrence_id, visit_detail_id)
2. **Use appropriate time windows** to limit data volume
3. **Consider micro-costing carefully** for very large cohorts
4. **Monitor temp table usage** in your database system

---

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Development Setup

```r
# Install development dependencies
remotes::install_github("OHDSI/CostUtilization", ref = "devv5", dependencies = TRUE)

# Run tests
devtools::test()

# Check package
devtools::check()
```

---

## Getting Help

- **Bug Reports**: [GitHub Issues](https://github.com/OHDSI/CostUtilization/issues)
- **Questions**: [OHDSI Forums](https://forums.ohdsi.org/)
- **Community**: [OHDSI MS Teams](https://www.ohdsi.org/web/wiki/doku.php?id=documentation:collaboration:ms_teams)

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- **OHDSI Community** for feedback and testing
- **Contributors** who helped improve the package
- **Eunomia Team** for providing excellent test data infrastructure

---

*The devv5 branch represents a significant evolution of CostUtilization, bringing modern R practices, enhanced functionality, and improved user experience to healthcare cost analysis.*