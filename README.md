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
library(DatabaseConnector)

# Create connection to your CDM database
connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = "localhost/ohdsi",
  user = "cdm_user",
  password = "cdm_password"
)

# Define cost analysis settings
costSettings <- costCovariateSettings(
  useCosts = TRUE,
  temporalStartDays = c(-365, -180, -30, 0),
  temporalEndDays = c(-1, -1, -1, 0),
  includeMedicalCosts = TRUE,
  includePharmacyCosts = TRUE,
  aggregateMethod = "sum"
)

# Extract cost data
costData <- getDbCostData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "cdm_schema",
  cohortTable = "cohort",
  cohortId = 1234,
  covariateSettings = costSettings,
  aggregated = TRUE
)

# View summary
summary(costData)
```

### Advanced Analysis with Stratification

Analyze costs stratified by demographics and cost domains:

```r
# Create detailed settings with stratification
advancedSettings <- costCovariateSettings(
  useCosts = TRUE,
  useCostDemographics = TRUE,
  temporalStartDays = c(-365, -180, -30),
  temporalEndDays = c(-1, -1, -1),
  includeMedicalCosts = TRUE,
  includePharmacyCosts = TRUE,
  includeProcedureCosts = TRUE,
  stratifyByAgeGroup = TRUE,
  stratifyByGender = TRUE,
  stratifyByCostDomain = TRUE,
  aggregateMethod = "sum"
)

# Extract stratified cost data
stratifiedCostData <- getDbCostData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "cdm_schema",
  cohortTable = "cohort",
  cohortId = 1234,
  covariateSettings = advancedSettings,
  aggregated = TRUE
)

# Filter by specific analysis
procedureCosts <- filterByAnalysisId(stratifiedCostData, analysisIds = c(1003))
```

### Cost and Utilization Analysis

Combine cost analysis with healthcare utilization metrics:

```r
# Settings for cost and utilization
utilizationSettings <- costCovariateSettings(
  useCosts = TRUE,
  useCostUtilization = TRUE,
  useCostVisitCounts = TRUE,
  temporalStartDays = c(-365, -30, 0),
  temporalEndDays = c(-1, -1, 30),
  includeTimeDistribution = TRUE
)

# Extract combined data
utilizationData <- getDbCostData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "cdm_schema",
  cohortTable = "cohort",
  cohortId = 1234,
  covariateSettings = utilizationSettings,
  aggregated = TRUE
)
```

---

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