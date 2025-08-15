# CostUtilization <img src="man/figures/logo.png" align="right" height="92" alt="" />

[![Build Status](https://github.com/OHDSI/CostUtilization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CostUtilization/actions?query=workflow%3AR-CMD-check+branch%3Amain)
[![codecov.io](https://codecov.io/github/OHDSI/CostUtilization/coverage.svg?branch=main)](https://app.codecov.io/github/OHDSI/CostUtilization?branch=main)

---

## Introduction

The `CostUtilization` R package provides a standardized framework for generating cost and healthcare resource utilization features for patient cohorts within OMOP Common Data Model (CDM) databases. It is designed to produce consistent, aggregated analytics that can be used for cohort characterization and descriptive studies.

### Key Features

  - **Flexible Time Windows**: Analyze costs across multiple, user-defined time windows relative to a cohort index date (e.g., 365 days prior, during the cohort, 90 days after).
  - **Granular Event Selection**: Calculate costs and utilization based on broad CDM domains (e.g., 'Drug', 'Visit') or provide a specific concept set for fine-grained analysis of particular clinical events.
  - **Advanced Cost Filtering**: Restrict analyses to specific cost types (e.g., paid by payer vs. paid by patient) and currencies.
  - **Multiple Aggregation Levels**: Generate per-patient metrics aggregated to daily, monthly, quarterly, or yearly rates (PPPD, PPPM, PPPQ, PPPY).
  - **Cost Standardization**: Includes functionality to standardize costs to a specific year using Consumer Price Index (CPI) data to account for inflation. A default CPI table for U.S. Medical Care is included.

-----

## Installation

To install the latest development version from GitHub, use the `remotes` package:

```r
# install.packages("remotes")
remotes::install_github("OHDSI/CostUtilization")
```

-----

## Example Usage

Calculate total cost and cost by domain for a cohort in the year before their index date. This example uses the built-in Eunomia test dataset.

```r
library(CostUtilization)
library(dplyr)

# Get a connection to the Eunomia test database
# This function also injects synthetic cost data for the example
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
connection <- DatabaseConnector::connect(connectionDetails)

# The Eunomia test helper transforms the cost table to the required long format
transformCostToCdmV5dot5(connectionDetails)

# Define the analysis settings
costSettings <- createCostUtilizationSettings(
  timeWindows = list(c(-365, 0)),
  costDomains = c("Drug", "Visit", "Procedure"),
  # Specify aggregation levels for the output
  aggregate = c("pppm", "pppy")
)

# Execute the analysis for cohort ID 1
costData <- getDbCostData(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortIds = 1,
  costUtilizationSettings = costSettings
)


# Disconnect from the database
DatabaseConnector::disconnect(connection)
```

-----

## Documentation

  - [Package Manual](https://ohdsi.github.io/CostUtilization/)
  - Vignettes are under development.

-----

## Technology

`CostUtilization` is an R package that uses `SqlRender` for generating database-agnostic SQL and `DatabaseConnector` for connecting to OMOP CDMs. Results are handled using the `Andromeda` package to ensure efficiency with large datasets.

-----

## System Requirements

  - R (version 4.1.0 or higher)
  - Java 8 or higher (for `DatabaseConnector`)
  - Access to an OMOP CDM v5.4+ database.
  - The `COST` table must be populated in the **long format**, as introduced in CDM v5.4. The package includes a helper function, `transformCostToCdmV5dot5`, to assist with this conversion for testing purposes.

-----

## Getting Help

  - **Bug Reports and Feature Requests**: Use the [GitHub Issue Tracker](https://github.com/OHDSI/CostUtilization/issues).
  - **General Questions**: Post questions on the [OHDSI Forums](https://forums.ohdsi.org/).
  - **Community and Collaboration**: Join the OHDSI community on [MS Teams](https://www.ohdsi.org/web/wiki/doku.php?id=documentation:collaboration:ms_teams) to engage with developers and researchers.