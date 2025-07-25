# CostUtilization

[![Build Status](https://github.com/OHDSI/CostUtilization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CostUtilization/actions?query=workflow%3AR-CMD-check+branch%3Amain)
[![codecov.io](https://codecov.io/github/OHDSI/CostUtilization/coverage.svg?branch=main)](https://app.codecov.io/github/OHDSI/CostUtilization?branch=main)

---

## Introduction

The CostUtilization R package provides a standardized framework for analyzing healthcare costs and utilization patterns for patient cohorts within OMOP Common Data Model (CDM) databases. It enables researchers to extract, aggregate, and analyze cost data across different domains, time windows, and cost types.

### Key Features

- **Temporal Analysis**: Analyze costs across multiple time windows relative to an index date
- **Domain-Specific Costs**: Extract costs for medical, pharmacy, procedures, visits, devices, and more
- **Cost Type Stratification**: Differentiate between charged, allowed, paid, and other cost components
- **Flexible Aggregation**: Support for sum, mean, median, min, and max aggregations
- **Demographic Stratification**: Analyze costs by age groups and gender
- **Integration with OHDSI Tools**: Seamless integration with FeatureExtraction and other OHDSI packages

The package follows OHDSI best practices for cost data standardization, leveraging standard concepts for cost classification and provenance tracking.

---

## Installation

```r
# Install from GitHub
# install.packages("remotes")
remotes::install_github("OHDSI/CostUtilization")
```

---

## Examples

### Basic Cost Analysis

Calculate total healthcare costs for a patient cohort:

```r
library(CostUtilization)

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
  analysisName = "My First Cost Analysis",
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

# The result is a standard CovariateData object, which can be explored
# using FeatureExtraction functions
summary <- FeatureExtraction::getCovariateDataSummary(costData)
print(summary)
```

## Documentation

- [Package Manual](https://ohdsi.github.io/CostUtilization/)
- [Vignettes](https://github.com/OHDSI/CostUtilization/tree/main/vignettes)
  - Getting Started with CostUtilization
  - Advanced Cost Analysis Techniques
  - Integration with OHDSI Studies

---

## Technology

CostUtilization is an R package built using:
- SqlRender for database-agnostic SQL generation
- DatabaseConnector for database connectivity
- Standard OHDSI design patterns for consistency

---

## System Requirements

- R (version 4.1.0 or higher)
- Java 8 or higher (for DatabaseConnector)
- Access to an OMOP CDM v5.3+ database with cost data

---

## Getting Help

- [Issue Tracker](https://github.com/OHDSI/CostUtilization/issues) for bug reports and feature requests
- [OHDSI Forums](https://forums.ohdsi.org/) for questions and discussions
- [OHDSI MS Teams](https://teams.microsoft.com/l/team/19%3a133f2b94b86a41a884d4a4ca3a7fc7e1%40thread.tacv2/conversations?groupId=a5f83e43-d065-4d6e-b20c-4adb3