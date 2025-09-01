# CostUtilization <img src="man/figures/logo.png" align="right" height="92" alt="CostUtilization Logo" />

[![Build Status](https://github.com/OHDSI/CostUtilization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CostUtilization/actions?query=workflow%3AR-CMD-check+branch%3Amain)
[![codecov.io](https://codecov.io/github/OHDSI/CostUtilization/coverage.svg?branch=main)](https://app.codecov.io/github/OHDSI/CostUtilization?branch=main)

---

## Introduction

The `CostUtilization` R package provides a standardized framework for generating cost and healthcare resource utilization features for patient cohorts within OMOP Common Data Model (CDM) databases. **This package is specifically designed for CDM v5.5 and later**, fully supporting the new normalized, long-format `COST` table structure.

### Key Features

* **CDM v5.5+ Compatibility**: Leverages the long-format `COST` table for enhanced temporal precision and analytical flexibility.
* **Flexible Analysis Windows**: Defines analysis periods relative to cohort start or end dates with simple offsets (e.g., 365 days before to 365 days after).
* **Granular Costing**: Calculates costs based on broad CDM domains (e.g., 'Drug', 'Procedure') or specific, user-defined concept sets.
* **Advanced Filtering**: Restricts analyses to specific visit types, cost concepts (e.g., 'total charge', 'paid by payer'), and currencies.
* **Multiple Costing Levels**: Supports both standard visit-level (`visit_occurrence`) costing and detailed line-level (`visit_detail`) micro-costing.
* **Seamless OHDSI Integration**: The primary output is a `CovariateData` object, which is fully compatible with other OHDSI tools like `FeatureExtraction` and `PatientLevelPrediction`.

---

## Installation

To install the latest stable version from GitHub:

```r
# install.packages("remotes")
remotes::install_github("OHDSI/CostUtilization")
```

To install the development version:

```r
remotes::install_github("OHDSI/CostUtilization")
```

---

## Quick Start: A Complete Workflow

This example demonstrates a full analysis workflow using the included Eunomia test dataset.

```r
library(CostUtilization)
library(dplyr)
library(DBI)

# 1. Set up a local test database
# This helper function downloads and creates a local DuckDB with the Eunomia dataset.
dbFile <- getEunomiaDuckDb(pathToData = tempdir())
con <- DBI::dbConnect(duckdb::duckdb(dbFile))

# 2. Prepare the data
# This function injects synthetic cost data and transforms it to the CDM v5.5 long format.
transformCostToCdmV5dot5(con)

# 3. Create a cohort for analysis
DBI::dbExecute(con, "
  CREATE TABLE main.cohort AS
  SELECT
    1 AS cohort_definition_id,
    p.person_id AS subject_id,
    op.observation_period_start_date AS cohort_start_date,
    op.observation_period_end_date AS cohort_end_date
  FROM main.person p
  JOIN main.observation_period op ON p.person_id = op.person_id
  LIMIT 200;
")

# 4. Define analysis settings
# We will analyze total charges in the 365 days following cohort entry.
costSettings <- createCostOfCareSettings(
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L # 31973 = Total Charge
)

# 5. Execute the analysis
analysisResults <- calculateCostOfCare(
  connection = con,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = costSettings
)

# 6. Review the results
# The output is a list containing results and diagnostics.
print(analysisResults$results)
print(analysisResults$diagnostics)

# 7. Clean up
DBI::dbDisconnect(con, shutdown = TRUE)
unlink(dbFile)
```

---

## Advanced Usage

### Event-Filtered Micro-Costing

Calculate costs for specific line-level events (`visit_detail`) that meet certain criteria. Here, we calculate costs for visits that include a diabetes diagnosis and focus on the costs of specific diabetes medications within those visits.

```r
# Define event filters
diabetesFilters <- list(
  list(
    name = "Diabetes Diagnoses",
    domain = "Condition", 
    conceptIds = c(201820L, 443238L) # Type 1 and Type 2 Diabetes Mellitus
  ),
  list(
    name = "Diabetes Medications",
    domain = "Drug",
    conceptIds = c(1503297L, 1502826L) # Metformin, Insulin
  )
)

# Create settings with event filters and micro-costing enabled
microSettings <- createCostOfCareSettings(
  startOffsetDays = -365L,
  endOffsetDays = 365L,
  eventFilters = diabetesFilters,
  microCosting = TRUE,
  primaryEventFilterName = "Diabetes Medications", # Cost only these line items
  costConceptId = 31985L # 31985 = Total Cost
)

# Rerun analysis with these settings...
```

### Integration with the OHDSI Ecosystem

Convert your analysis results into a standard `CovariateData` object for use in other OHDSI packages.

```r
# Use the results from our first analysis
covariateData <- createCostCovariateData(
  costResults = analysisResults,
  costOfCareSettings = costSettings,
  cohortId = 1L,
  databaseId = "Eunomia"
)

# The object is ready for use with FeatureExtraction
print(covariateData)
summary(covariateData)
```

---

## Core Functions

* `getEunomiaDuckDb()`: Creates a local DuckDB copy of the Eunomia dataset for testing and examples.
* `transformCostToCdmV5dot5()`: Injects synthetic data and transforms a wide `cost` table to the required long format.
* `createCostOfCareSettings()`: Creates a validated settings object to define all analysis parameters.
* `calculateCostOfCare()`: Executes the main cost and utilization analysis.
* `createCostCovariateData()`: Converts analysis results into a `FeatureExtraction` compatible `CovariateData` object.
* `calculateLos()`: A utility function to calculate the length of stay for visits in a cohort.

---

## Migration from Earlier Versions

To update code from previous versions of this package, adopt the settings-based approach.

**New approach (settings object):**

```r
# ✅ Recommended
settings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date", 
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L # Specify cost concept in settings
)

results <- calculateCostOfCare(
  connection = connection,
  costOfCareSettings = settings,
  # Other parameters remain the same...
)
```

---

## Technology

* **R** (version 4.1.0 or higher)
* **DatabaseConnector** & **DBI** for database connectivity
* **Tidyverse** packages (`dplyr`, `purrr`, `rlang`, `tidyr`) for modern R workflows
* **SqlRender** for generating database-agnostic SQL
* **Andromeda** for handling large data objects efficiently
* **checkmate** for robust input validation

---

## Getting Help

* **Bug Reports**: [GitHub Issues](https://github.com/OHDSI/CostUtilization/issues)
* **Questions & Community**: [OHDSI Forums](https://forums.ohdsi.org/) and [OHDSI Teams](https://www.ohdsi.org/web/wiki/doku.php?id=documentation:collaboration:ms_teams)

---

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.