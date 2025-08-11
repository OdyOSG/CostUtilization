# CostUtilization

[![Build Status](https://github.com/OHDSI/CostUtilization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CostUtilization/actions?query=workflow%3AR-CMD-check+branch%3Amain)
[![codecov.io](https://codecov.io/github/OHDSI/CostUtilization/coverage.svg?branch=main)](https://app.codecov.io/github/OHDSI/CostUtilization?branch=main)

---

## Introduction

The `CostUtilization` R package provides a standardized framework for generating cost and utilization features for patient cohorts within OMOP Common Data Model (CDM) databases. It is designed to produce consistent, aggregated analytics that can be used for cohort characterization and descriptive studies.

The package returns results as a `CovariateData` object, ensuring seamless integration with other OHDSI tools like `FeatureExtraction` and study packages.

### Key Features

-   **Temporal Analysis**: Analyze costs and utilization across user-defined time windows relative to a cohort index date (e.g., 365 days prior, 365 days after).
-   **Domain-Specific Metrics**: Calculate costs and event counts for specific CDM domains (Drug, Procedure, Visit, etc.).
-   **Cost Standardization**: Standardize costs to a specific year using Consumer Price Index (CPI) data to account for inflation. A default CPI table for U.S. Medical Care is included.
-   **Cost by Concept Set**: Calculate costs for specific clinical events by providing a custom concept set.
-   **Utilization Counts**: Generate counts of unique events (e.g., number of visits, number of prescriptions).
-   **Length of Stay**: Calculate the length of stay for inpatient hospitalizations.
-   **Flexible Filtering**: Restrict analyses to specific cost types (e.g., paid by payer) and currencies.
-   **OHDSI Integration**: Returns a `CovariateData` object, making it fully compatible with the OHDSI ecosystem.

---

## Installation

To install the package, use the `remotes` package:

```r
# install.packages("remotes")
remotes::install_github("OHDSI/CostUtilization")
```

---

## Examples

### Example 1: Basic Cost and Utilization Analysis

Calculate total cost, cost by domain, and length of stay for two cohorts in the year before and after their index date.

```r
library(CostUtilization)
library(dplyr)

# Set up connection to the database
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "...",
  server = "...",
  user = "...",
  password = "..."
)

cdmDatabaseSchema <- "your_cdm_schema"
cohortDatabaseSchema <- "your_results_schema"
cohortTable <- "your_cohort_table"

# Define the analysis settings
costSettings <- createCostUtilSettings(
  analysisName = "General Cost and Utilization",
  timeWindows = list(c(-365, -1), c(0, 365)),
  costDomains = c("Drug", "Visit", "Procedure"),
  calculateTotalCost = TRUE,
  calculateLengthOfStay = TRUE
)

# Execute the analysis
costData <- getCostUtilData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortIds = c(101, 102),
  costUtilSettings = costSettings,
  aggregated = TRUE
)

# View the aggregated results
costData$covariatesContinuous %>%
  collect() %>%
  print()
```

### Example 2: Standardizing Costs to a Specific Year

Calculate the total cost for a cohort and standardize all values to the year 2015.

```r
# Define settings with cost standardization
costSettingsStd <- createCostUtilSettings(
  analysisName = "Standardized Cost Analysis",
  timeWindows = list(c(0, 365)),
  calculateTotalCost = TRUE,
  costStandardizationYear = 2015
  # The package will use the default CPI table.
  # A custom table can be provided via the `cpiData` argument.
)

# Execute the analysis
costDataStd <- getCostUtilData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortIds = 101,
  costUtilSettings = costSettingsStd,
  aggregated = TRUE
)

# View the standardized cost results
costDataStd$covariatesContinuous %>%
  collect() %>%
  print()
```

---

## Documentation

-   [Package Manual](https://ohdsi.github.io/CostUtilization/)
-   Vignettes are under development.

---

## Technology

`CostUtilization` is an R package that uses `SqlRender` for generating database-agnostic SQL and `DatabaseConnector` for connecting to OMOP CDMs. Results are handled using the `Andromeda` package to ensure efficiency with large datasets.

---

## System Requirements

-   R (version 4.1.0 or higher)
-   Java 8 or higher (for `DatabaseConnector`)
-   Access to an OMOP CDM v5.4+ database.
-   The `COST` table must be populated in the **long format**, as introduced in CDM v5.4.

---

## Getting Help

-   **Bug Reports and Feature Requests**: Use the [GitHub Issue Tracker](https://github.com/OHDSI/CostUtilization/issues).
-   **General Questions**: Post questions on the [OHDSI Forums](https://forums.ohdsi.org/).
-   **Community and Collaboration**: Join the OHDSI community on [MS Teams](https://www.ohdsi.org/web/wiki/doku.php?id=documentation:collaboration:ms_teams) to engage with developers and researchers.

---

## Development Status

This package is under active development by the OHDSI community. Contributions are welcome.