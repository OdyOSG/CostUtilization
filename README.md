# CostUtilization <img src="man/figures/logo.png" align="right" height="92" alt="" />

[![Build Status](https://github.com/OHDSI/CostUtilization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CostUtilization/actions?query=workflow%3AR-CMD-check+branch%3Adevv5)
[![codecov.io](https://codecov.io/github/OHDSI/CostUtilization/coverage.svg?branch=devv5)](https://app.codecov.io/github/OHDSI/CostUtilization?branch=devv5)

---

## Introduction

The `CostUtilization` R package provides a standardized framework for generating cost and healthcare resource utilization features for patient cohorts within OMOP Common Data Model (CDM) databases. **This version is specifically designed for CDM v5.5** with full support for the new long-format COST table structure.

### Key Features

- **CDM v5.5 Compatibility**: Full support for the new long-format COST table with enhanced temporal precision
- **Flexible Time Windows**: Analyze costs across multiple, user-defined time windows relative to cohort index dates
- **Granular Event Selection**: Calculate costs based on broad CDM domains or specific concept sets
- **Advanced Cost Filtering**: Restrict analyses to specific cost types (charges, payments, etc.) and currencies
- **Multiple Aggregation Levels**: Generate per-patient metrics aggregated to daily, monthly, quarterly, or yearly rates
- **Modern R Implementation**: Settings-based API with tidyverse integration (`dplyr`, `purrr`, `rlang`)

---

## What's New in CDM v5.5

### Enhanced Cost Table Structure

CDM v5.5 introduces a **long-format COST table** that provides better traceability and analytical flexibility:

**Previous (Wide Format)**:
```
cost_id | person_id | total_charge | paid_by_payer | paid_by_patient
   1    |    123    |    1000.00   |    800.00     |     200.00
```

**CDM v5.5 (Long Format)**:
```
cost_id | person_id | cost_concept_id | cost_source_value | cost | effective_date
   1    |    123    |      31973      |  "total_charge"   | 1000 | 2023-01-15
   2    |    123    |      31980      |  "paid_by_payer"  | 800  | 2023-01-15  
   3    |    123    |      31981      | "paid_by_patient" | 200  | 2023-01-15
```

### New Temporal Fields

- `effective_date`: When the cost was incurred (required)
- `billed_date`: When the cost was billed
- `paid_date`: When the cost was actually paid
- `cost_event_field_concept_id`: Links cost to specific clinical event fields

---

## Installation

To install the latest development version from GitHub:

```r
# install.packages("remotes")
remotes::install_github("OHDSI/CostUtilization", ref = "devv5")
```

---

## Quick Start

### Basic Example with Eunomia

```r
library(CostUtilization)
library(dplyr)

# Get connection to Eunomia test database
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

# Transform cost data to CDM v5.5 format (includes synthetic data injection)
transformCostToCdmV5dot5(connectionDetails)

# Create analysis settings using the new API
costSettings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -365L,  # 1 year before
  endOffsetDays = 0L,       # up to index date
  costConceptId = 31973L    # Total charge
)

# Execute the analysis
results <- calculateCostOfCare(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = costSettings
)

# View results
print(results$results)
print(results$diagnostics)
```

### Advanced Event Filtering

```r
# Define event filters for diabetes-related costs
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
  ),
  list(
    name = "Diabetes Labs",
    domain = "Measurement",
    conceptIds = c(3004501L, 3003309L)
  )
)

# Create settings with event filters
eventSettings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date",
  startOffsetDays = -365L,
  endOffsetDays = 365L,
  eventFilters = diabetesFilters,
  costConceptId = 31973L
)

# Run filtered analysis
filteredResults <- calculateCostOfCare(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main", 
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = eventSettings
)
```

### Multi-Cohort Analysis with Modern R

```r
# Analyze multiple cost types using purrr
costTypes <- tibble(
  costType = c("total_charge", "total_cost", "paid_by_payer", "paid_by_patient"),
  conceptId = c(31973L, 31985L, 31980L, 31981L)
)

results <- costTypes |>
  pmap_dfr(function(costType, conceptId) {
    settings <- createCostOfCareSettings(
      anchorCol = "cohort_start_date",
      startOffsetDays = 0L,
      endOffsetDays = 365L,
      costConceptId = conceptId
    )
    
    calculateCostOfCare(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main", 
      cohortTable = "cohort",
      cohortId = 1,
      costOfCareSettings = settings
    )$results |>
    mutate(costType = costType)
  })

# Visualize results
library(ggplot2)
results |>
  ggplot(aes(x = costType, y = total_cost, fill = costType)) +
  geom_bar(stat = "identity") +
  labs(title = "Healthcare Costs by Payment Type (CDM v5.5)")
```

---

## Documentation

- **[Package Manual](https://ohdsi.github.io/CostUtilization/)**
- **Vignettes:**
  - [Getting Started with CDM v5.5](vignettes/getting-started-v55.html)
  - [Setting Up Eunomia with CDM v5.5](vignettes/eunomia-setup-v55.html)
  - [Advanced Cost Analysis](vignettes/advanced-cost-analysis-v55.html)
  - [CPI Adjustment](vignettes/cpi-adjustment.html)

---

## Key Functions

### Data Setup
- `injectCostData()`: Create synthetic cost data for testing
- `transformCostToCdmV5dot5()`: Transform wide-format tables to CDM v5.5 long format

### Analysis Configuration  
- `createCostOfCareSettings()`: Create validated settings objects with comprehensive options

### Cost Analysis
- `calculateCostOfCare()`: Perform cost and utilization analysis with detailed diagnostics

---

## CDM v5.5 Cost Concepts

The package supports all standard OMOP cost concepts:

| Concept ID | Description | Source Value |
|------------|-------------|--------------|
| 31973 | Total charge | "total_charge" |
| 31985 | Total cost | "total_cost" |
| 31980 | Paid by payer | "paid_by_payer" |
| 31981 | Paid by patient | "paid_by_patient" |
| 31974 | Patient copay | "paid_patient_copay" |
| 31975 | Patient coinsurance | "paid_patient_coinsurance" |
| 31976 | Patient deductible | "paid_patient_deductible" |
| 31979 | Amount allowed | "amount_allowed" |

---

## Migration from Earlier Versions

### Updating Existing Code

**Old approach (direct parameters):**
```r
# ❌ Old way - direct parameters
results <- calculateCostOfCare(
  connection = connection,
  anchorCol = "cohort_start_date",
  startOffsetDays = 0,
  endOffsetDays = 365
)
```

**New approach (settings object):**
```r
# ✅ New way - settings object
settings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date", 
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L
)

results <- calculateCostOfCare(
  connection = connection,
  costOfCareSettings = settings
)
```

### Transforming Existing Data

If you have wide-format cost data, use the transformation function:

```r
# Transform existing wide-format cost table to CDM v5.5
transformCostToCdmV5dot5(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "your_schema",
  sourceCostTable = "cost",
  createIndexes = TRUE
)
```

---

## Technology

- **R** (version 4.1.0 or higher)
- **DatabaseConnector** for database connectivity
- **Tidyverse** packages (`dplyr`, `purrr`, `rlang`) for modern R workflows
- **SqlRender** for database-agnostic SQL generation
- **Java 8+** (for DatabaseConnector)

---

## System Requirements

- R (version 4.1.0 or higher)
- Java 8 or higher (for DatabaseConnector)
- Access to an OMOP CDM v5.5+ database
- COST table in long format (use `transformCostToCdmV5dot5()` if needed)

---

## Getting Help

- **Bug Reports**: [GitHub Issues](https://github.com/OHDSI/CostUtilization/issues)
- **Questions**: [OHDSI Forums](https://forums.ohdsi.org/)
- **Community**: [OHDSI MS Teams](https://www.ohdsi.org/web/wiki/doku.php?id=documentation:collaboration:ms_teams)

---

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- **OHDSI Collaborative** for the OMOP Common Data Model
- **Eunomia** for providing synthetic test data
- **Contributors** who have helped develop and test this package

---

*This package is part of the [OHDSI](https://ohdsi.org/) ecosystem of tools for observational health data analysis.*