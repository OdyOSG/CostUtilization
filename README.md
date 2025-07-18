# CostUtilization

[![Build Status](https://github.com/OHDSI/CostUtilization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/CostUtilization/actions?query=workflow%3AR-CMD-check+branch%3Amain)
[![codecov.io](https://codecov.io/github/OHDSI/CostUtilization/coverage.svg?branch=main)](https://app.codecov.io/github/OHDSI/CostUtilization?branch=main)

---

## Introduction

This R package provides a standardized framework for calculating costs for specified patient cohorts within an OMOP Common Data Model database.

[cite_start]The package design is based on the principle that cost data requires two key pieces of information for standardization: classification (the "what") and provenance (the "where")[cite: 7, 8, 11]. [cite_start]It provides functions to generate cost covariates by leveraging standard concepts for different cost components (e.g., 'Charged', 'Allowed', 'Paid') [cite: 24] [cite_start]and cost provenance types (e.g., 'Adjudicated Claim')[cite: 78, 81]. [cite_start]The final output conforms to the `FeatureExtraction` package's object model, allowing for seamless integration into existing OHDSI study pipelines[cite: 200].

---

## Examples

You can install and run the package from RStudio. The following example demonstrates how to calculate the total "Allowed" cost for a cohort of patients with Type 2 Diabetes using the Eunomia test dataset.

## Examples

You can install and run the package from RStudio. The following example demonstrates how to calculate the total "Allowed" cost for a cohort of patients with Type 2 Diabetes using the Eunomia test dataset.

```r
# 1. Install CostUtilization from GitHub
# install.packages('remotes')
remotes::install_github('OHDSI/CostUtilization')

# 2. Load the library and its dependencies
library(CostUtilization)
library(dplyr)
library(DatabaseConnector)

# 3. Get connection details for the Eunomia test database
connectionDetails <- Eunomia::getEunomiaConnectionDetails()

# 4. Create a cohort and sample cost data for the example
# A helper function to populate the database (implementation not shown for brevity)
source("[https://raw.githubusercontent.com/OHDSI/CostUtilization/main/extras/TestSetup.R](https://raw.githubusercontent.com/OHDSI/CostUtilization/main/extras/TestSetup.R)")
setupCostTests(connectionDetails)


# 5. Define and execute the cost analysis
# Define settings to calculate the total "Allowed" cost from "Claims"
costSettings <- createCostCovariateSettings(
  covariateId = 1001,
  costConceptId = c(31978), # Allowed
  costTypeConceptId = c(32817) # Adjudicated Claim
)

# Execute the analysis using connectionDetails
# The function will handle connecting and disconnecting automatically
costCovariates <- getCostCovariateData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohort",
  cohortId = 1, # ID for the T2DM cohort created in setupCostTests()
  costCovariateSettings = costSettings
)

# 6. View the results
print(head(costCovariates))
```

-----

## Technology

`CostUtilization` is an R package.

-----

## System Requirements

Running the package requires R (version 4.1.0 or higher).

-----

## License

`CostUtilization` is licensed under Apache License 2.0.

-----

## Development

`CostUtilization` is being developed in RStudio. [cite\_start]The package is maintained by the OHDSI community[cite: 199].


