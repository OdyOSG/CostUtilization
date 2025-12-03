# CostUtilization <img src="man/figures/logo.png" align="right" height="92" alt="CostUtilization Logo" />

[![Build Status](https://github.com/OHDSI/CostUtilization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CostUtilization/actions?query=workflow%3AR-CMD-check+branch%3Amain)
[![codecov.io](https://codecov.io/github/OHDSI/CostUtilization/coverage.svg?branch=main)](https://app.codecov.io/github/OHDSI/CostUtilization?branch=main)

---

## Introduction

The `CostUtilization` R package provides a standardized framework for generating cost and healthcare resource utilization features for patient cohorts within OMOP Common Data Model (CDM) databases. **This package is specifically designed for CDM v5.5 and later**, fully supporting the new normalized, long-format `COST` table structure.

### Key Features

* **CDM v5.5+ Compatibility**: Leverages the long-format `COST` table for enhanced temporal precision and analytical flexibility.
* **Modular SQL Architecture**: Features a modular, maintainable SQL codebase with database-agnostic queries powered by SqlRender.
* **DatabaseConnector Integration**: Built on OHDSI's DatabaseConnector framework for robust, enterprise-grade database connectivity across multiple platforms.
* **Base R Implementation**: Utilizes base R for core functionality, ensuring broad compatibility and minimal dependencies.
* **Flexible Analysis Windows**: Defines analysis periods relative to cohort start or end dates with simple offsets (e.g., 365 days before to 365 days after).
* **Granular Costing**: Calculates costs based on broad CDM domains (e.g., 'Drug', 'Procedure') or specific, user-defined concept sets.
* **Advanced Filtering**: Restricts analyses to specific visit types, cost concepts (e.g., 'total charge', 'paid by payer'), and currencies.
* **Multiple Costing Levels**: Supports both standard visit-level (`visit_occurrence`) costing and detailed line-level (`visit_detail`) micro-costing.
* **Seamless OHDSI Integration**: The primary output is a `CovariateData` object, which is fully compatible with other OHDSI tools like `FeatureExtraction` and `PatientLevelPrediction`.

---

## Architecture & Design

### Modular SQL Structure

The package employs a modular SQL architecture that enhances maintainability, testability, and readability:

```
inst/sql/sql_server/
├── base/
│   ├── cohort_filtering.sql       # Core cohort operations
│   ├── cost_aggregation.sql       # Cost calculation logic
│   └── temporal_windows.sql       # Time window definitions
├── components/
│   ├── event_filters.sql          # Event-based filtering
│   ├── visit_filtering.sql        # Visit type restrictions
│   └── micro_costing.sql          # Visit detail level costing
└── main/
    ├── calculate_cost_of_care.sql # Main analysis query
    └── cost_summary.sql           # Results aggregation
```

**Benefits of the Modular Approach:**

* **Maintainability**: Each SQL module has a single responsibility, making updates and debugging easier.
* **Reusability**: Common operations are abstracted into reusable components.
* **Testability**: Individual modules can be tested in isolation.
* **Database Agnostic**: All SQL is written using SqlRender templates for cross-platform compatibility.
* **Version Control**: Changes to specific functionality are isolated and trackable.

### DatabaseConnector vs. DBI

While DBI provides basic database connectivity, DatabaseConnector offers enterprise-grade features essential for OHDSI applications:

**DatabaseConnector Advantages:**

* **Multi-Platform Support**: Native support for all major healthcare databases (SQL Server, PostgreSQL, Oracle, BigQuery, Redshift, etc.)
* **Connection Pooling**: Efficient connection management for large-scale analyses
* **Batch Operations**: Optimized for healthcare data workloads with large result sets
* **Error Handling**: Robust error handling and logging specifically designed for OHDSI workflows
* **Security**: Enhanced security features including connection string encryption
* **Performance**: Optimized drivers and query execution for healthcare analytics
* **OHDSI Integration**: Seamless integration with other OHDSI tools and workflows

**Base R Implementation Benefits:**

* **Minimal Dependencies**: Reduces package complexity and potential conflicts
* **Stability**: Base R functions provide long-term stability and compatibility
* **Performance**: Optimized core R functions for data manipulation tasks
* **Portability**: Works across different R environments and platforms without additional requirements

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

This example demonstrates a full analysis workflow using the included Eunomia test dataset with the new modular architecture.

```r
library(CostUtilization)
library(DatabaseConnector)

# 1. Set up database connection using DatabaseConnector
connectionDetails <- createConnectionDetails(
  dbms = "duckdb",
  server = getEunomiaDuckDb(pathToData = tempdir())
)

connection <- connect(connectionDetails)

# 2. Prepare the data
# This function injects synthetic cost data and transforms it to the CDM v5.5 long format.
transformCostToCdmV5dot5(connection)

# 3. Create a cohort for analysis
executeSql(connection, "
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

# 5. Execute the analysis using the new modular approach
analysisResults <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = costSettings
)

# 6. Review the results
# The output leverages the modular SQL components for enhanced performance
print(analysisResults$results)

# 7. Clean up
disconnect(connection)
```

### Backward Compatibility with DBI

The package maintains backward compatibility with existing DBI-based workflows:

```r
# Existing DBI workflows continue to work
library(DBI)
library(duckdb)

dbFile <- getEunomiaDuckDb(pathToData = tempdir())
con <- dbConnect(duckdb(dbFile))

# Your existing code works unchanged
costSettings <- createCostOfCareSettings(
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L
)

results <- calculateCostOfCare(
  connection = con,  # DBI connection still supported
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = costSettings
)

dbDisconnect(con, shutdown = TRUE)
```

---

## Advanced Usage

### Event-Filtered Micro-Costing with Modular SQL

The modular architecture enables sophisticated analyses with optimized query performance:

```r
# Define event filters - processed by modular event_filters.sql component
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

# Create settings that leverage micro_costing.sql module
microSettings <- createCostOfCareSettings(
  startOffsetDays = -365L,
  endOffsetDays = 365L,
  eventFilters = diabetesFilters,
  microCosting = TRUE,  # Activates visit_detail level analysis
  costConceptId = 31985L # 31985 = Total Cost
)

# Analysis automatically selects optimal SQL modules based on settings
results <- calculateCostOfCare(
  connection = connection,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1,
  costOfCareSettings = microSettings
)
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

## SQL Module Documentation

### Base Modules

* **cohort_filtering.sql**: Core cohort identification and filtering logic
* **cost_aggregation.sql**: Cost calculation and aggregation algorithms
* **temporal_windows.sql**: Time window calculations relative to cohort dates

### Component Modules

* **event_filters.sql**: Event-based filtering by domain and concept sets
* **visit_filtering.sql**: Visit type and context restrictions
* **micro_costing.sql**: Visit detail level micro-costing operations

### Main Modules

* **calculate_cost_of_care.sql**: Orchestrates the complete analysis workflow
* **cost_summary.sql**: Final results aggregation and formatting

Each module is database-agnostic through SqlRender templating and can be individually tested and maintained.

---

## Core Functions

* `getEunomiaDuckDb()`: Creates a local DuckDB copy of the Eunomia dataset for testing and examples.
* `transformCostToCdmV5dot5()`: Injects synthetic data and transforms a wide `cost` table to the required long format.
* `createCostOfCareSettings()`: Creates a validated settings object to define all analysis parameters.
* `calculateCostOfCare()`: Executes the main cost and utilization analysis using modular SQL architecture.
* `createCostCovariateData()`: Converts analysis results into a `FeatureExtraction` compatible `CovariateData` object.
* `calculateLos()`: A utility function to calculate the length of stay for visits in a cohort.

---

## Migration from Earlier Versions

To update code from previous versions of this package, adopt the settings-based approach and optionally migrate to DatabaseConnector.

**New approach (settings object with DatabaseConnector):**

```r
# ✅ Recommended - DatabaseConnector approach
connectionDetails <- createConnectionDetails(
  dbms = "postgresql",  # or your database type
  server = "your-server",
  database = "your-database"
)
connection <- connect(connectionDetails)

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

disconnect(connection)
```

**Backward compatible (DBI still supported):**

```r
# ✅ Still supported - DBI approach
con <- dbConnect(RPostgres::Postgres(), ...)

settings <- createCostOfCareSettings(
  anchorCol = "cohort_start_date", 
  startOffsetDays = 0L,
  endOffsetDays = 365L,
  costConceptId = 31973L
)

results <- calculateCostOfCare(
  connection = con,  # DBI connection
  costOfCareSettings = settings,
  # Other parameters remain the same...
)
```

---

## Technology

* **R** (version 4.1.0 or higher)
* **DatabaseConnector** (primary) - Enterprise-grade database connectivity with multi-platform support, connection pooling, and OHDSI-optimized drivers
* **SqlRender** - Database-agnostic SQL generation and templating for the modular SQL architecture
* **Base R** - Core functionality built on stable base R functions for maximum compatibility and minimal dependencies
* **DBI** (secondary) - Maintained for backward compatibility with existing workflows
* **Andromeda** - Efficient handling of large healthcare datasets
* **checkmate** - Robust input validation and parameter checking

### Key Architectural Components

* **Modular SQL Framework**: Database-agnostic SQL modules for maintainable, testable query logic
* **Multi-Database Support**: Native support for SQL Server, PostgreSQL, Oracle, BigQuery, Redshift, and more
* **Connection Management**: Robust connection handling with automatic cleanup and error recovery
* **Performance Optimization**: Optimized for large-scale healthcare analytics workloads

---

## Getting Help

* **Bug Reports**: [GitHub Issues](https://github.com/OHDSI/CostUtilization/issues)
* **Questions & Community**: [OHDSI Forums](https://forums.ohdsi.org/) and [OHDSI Teams](https://www.ohdsi.org/web/wiki/doku.php?id=documentation:collaboration:ms_teams)

---

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.