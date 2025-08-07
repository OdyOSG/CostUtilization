# Copyright 2025 OHDSI
#
# This file is part of CostUtilization
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Get Cost and Utilization Data
#'
#' @description
#' Executes the cost and utilization analysis against a cohort and returns the results
#' as a `CovariateData` object. This function uses a `costUtilSettings` object to define
#' the analysis parameters.
#'
#' @details
#' This function connects to the database, prepares the analysis settings, and runs the
#' main SQL query (`GetCostAndUtilization.sql`) to extract person-level data. If aggregation
#' is requested, it runs a second SQL query (`AggregateCovariates.sql`) to summarize the results.
#' The final output is structured as a standard OHDSI `CovariateData` object.
#'
#' @param connection              An object of type `connection` as created using the
#'                                `DatabaseConnector::connect()` function.
#' @param cdmDatabaseSchema       The name of the database schema that contains the OMOP CDM instance.
#'                                Requires read permissions to this schema.
#' @param cohortDatabaseSchema    The name of the database schema that is the location where the
#'                                cohort data used for the analysis resides.
#' @param cohortTable             The name of the table that contains the cohort data.
#' @param cohortIds               A numeric vector of cohort definition IDs to be included in the analysis.
#' @param costUtilSettings        An S3 object of type `costUtilSettings` created using the
#'                                `createCostUtilSettings()` function.
#' @param aggregated              A boolean flag indicating whether to return aggregated summary
#'                                statistics (`TRUE`) or person-level data (`FALSE`).
#'
#' @return
#' An R object of class `CovariateData`, which is an `Andromeda` object containing the
#' analysis results.
#'
#' @export
getCostUtilData <- function(connection,
                            cdmDatabaseSchema,
                            cohortDatabaseSchema,
                            cohortTable,
                            cohortIds,
                            costUtilSettings,
                            aggregated = FALSE) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connection, "DatabaseConnection", add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(cohortDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(cohortTable, len = 1, add = errorMessages)
  checkmate::assertIntegerish(cohortIds, min.len = 1, add = errorMessages)
  checkmate::assertClass(costUtilSettings, "costUtilSettings", add = errorMessages)
  checkmate::assertFlag(aggregated, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  start <- Sys.time()
  
  # Set up temp tables
  personLevelTable <- paste0("#pl_", paste(sample(letters, 12), collapse = ""))
  temporalWindowsTable <- "#temporal_windows"
  cpiDataTable <- "#cpi_data"
  conceptSetTable <- "#concept_set_codes"
  
  # Create and upload temporal windows table
  useFixedWindows <- !is.null(costUtilSettings$timeWindows) && length(costUtilSettings$timeWindows) > 0
  if (useFixedWindows) {
    temporalWindows <- do.call(rbind, lapply(seq_along(costUtilSettings$timeWindows), function(i) {
      data.frame(
        window_id = i,
        start_day = costUtilSettings$timeWindows[[i]][1],
        end_day = costUtilSettings$timeWindows[[i]][2]
      )
    }))
    DatabaseConnector::insertTable(connection,
                                   tableName = temporalWindowsTable,
                                   data = temporalWindows,
                                   tempTable = TRUE,
                                   camelCaseToSnakeCase = TRUE)
  }
  
  # Prepare and upload CPI data if standardization is requested
  useCostStandardization <- !is.null(costUtilSettings$costStandardizationYear)
  if (useCostStandardization) {
    cpiData <- costUtilSettings$cpiData
    if (is.null(cpiData)) {
      cpiData <- getDefaultCpiTable()
    }
    targetCpi <- cpiData$cpi[cpiData$year == costUtilSettings$costStandardizationYear]
    if (length(targetCpi) == 0) {
      stop("Target year for cost standardization not found in CPI table.")
    }
    cpiData$inflation_factor <- targetCpi / cpiData$cpi
    DatabaseConnector::insertTable(connection,
                                   tableName = cpiDataTable,
                                   data = cpiData[, c("year", "inflation_factor")],
                                   tempTable = TRUE,
                                   camelCaseToSnakeCase = TRUE)
  }
  
  # Prepare and upload concept IDs if provided
  useConceptSet <- !is.null(costUtilSettings$conceptIds) && length(costUtilSettings$conceptIds) > 0
  if (useConceptSet) {
    conceptSet <- data.frame(
      concept_id = costUtilSettings$conceptIds,
      concept_rank = seq_along(costUtilSettings$conceptIds)
    )
    DatabaseConnector::insertTable(connection,
                                   tableName = conceptSetTable,
                                   data = conceptSet,
                                   tempTable = TRUE,
                                   camelCaseToSnakeCase = TRUE)
  }
  
  # Render and execute main SQL
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "GetCostAndUtilization.sql",
    packageName = "CostUtilization",
    dbms = connection@dbms,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds,
    person_level_table = personLevelTable,
    currency_concept_id = costUtilSettings$currencyConceptId,
    cost_type_concept_ids = costUtilSettings$costTypeConceptIds,
    use_total_cost = costUtilSettings$calculateTotalCost,
    use_domain_cost = !is.null(costUtilSettings$costDomains),
    cost_domains = costUtilSettings$costDomains,
    use_utilization = !is.null(costUtilSettings$utilizationDomains),
    utilization_domains = costUtilSettings$utilizationDomains,
    use_length_of_stay = costUtilSettings$calculateLengthOfStay,
    use_cost_standardization = useCostStandardization,
    use_fixed_windows = useFixedWindows,
    use_in_cohort_window = costUtilSettings$useInCohortWindow,
    use_concept_set = useConceptSet
  )
  DatabaseConnector::executeSql(connection, sql)
  
  # Create CovariateData object in Andromeda
  covariateData <- Andromeda::andromeda()
  
  # Construct reference tables
  analysisRef <- .createAnalysisRef(costUtilSettings)
  Andromeda::appendToTable(covariateData$analysisRef, analysisRef)
  covariateRef <- .createCovariateRef(costUtilSettings, analysisRef)
  Andromeda::appendToTable(covariateData$covariateRef, covariateRef)
  
  if (aggregated) {
    # Aggregate results on the server and pull into Andromeda
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "AggregateCovariates.sql",
      packageName = "CostUtilization",
      dbms = connection@dbms,
      person_level_table = personLevelTable
    )
    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      andromeda = covariateData,
      andromedaTableName = "covariatesContinuous",
      snakeCaseToCamelCase = TRUE
    )
    covariateData$covariatesContinuous <- covariateData$covariatesContinuous %>%
      mutate(cohortDefinitionId = !!cohortIds[1])
    
  } else {
    # Pull person-level results into Andromeda
    sql <- "SELECT row_id, covariate_id, covariate_value FROM @person_level_table;"
    sql <- SqlRender::render(sql, person_level_table = personLevelTable)
    sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      andromeda = covariateData,
      andromedaTableName = "covariates",
      snakeCaseToCamelCase = TRUE
    )
  }
  
  # Clean up server-side temp tables
  if (useFixedWindows) {
    sql <- "TRUNCATE TABLE @temporal_windows_table; DROP TABLE @temporal_windows_table;"
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 temporal_windows_table = temporalWindowsTable)
  }
  if (useCostStandardization) {
    sql <- "TRUNCATE TABLE @cpi_data_table; DROP TABLE @cpi_data_table;"
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 cpi_data_table = cpiDataTable)
  }
  if (useConceptSet) {
    sql <- "TRUNCATE TABLE @concept_set_table; DROP TABLE @concept_set_table;"
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 concept_set_table = conceptSetTable)
  }
  
  
  # Set metadata
  attr(covariateData, "metaData") <- list(
    call = match.call(),
    cohortIds = cohortIds,
    costUtilSettings = costUtilSettings,
    aggregated = aggregated,
    executionTime = Sys.time() - start
  )
  class(covariateData) <- "CovariateData"
  return(covariateData)
}

# Helper functions to create reference tables
.createAnalysisRef <- function(settings) {
  analysisId <- numeric(0)
  analysisName <- character(0)
  domainId <- character(0)
  
  if (settings$calculateTotalCost) {
    analysisId <- c(analysisId, 1)
    analysisName <- c(analysisName, "Total Cost")
    domainId <- c(domainId, "Cost")
  }
  if (!is.null(settings$costDomains)) {
    analysisId <- c(analysisId, 2)
    analysisName <- c(analysisName, "Cost By Domain")
    domainId <- c(domainId, "Cost")
  }
  if (!is.null(settings$utilizationDomains)) {
    analysisId <- c(analysisId, 3)
    analysisName <- c(analysisName, "Utilization")
    domainId <- c(domainId, "Count")
  }
  if (settings$calculateLengthOfStay) {
    analysisId <- c(analysisId, 4)
    analysisName <- c(analysisName, "Length of Stay")
    domainId <- c(domainId, "Observation")
  }
  if (!is.null(settings$conceptIds) && length(settings$conceptIds) > 0) {
    analysisId <- c(analysisId, 5)
    analysisName <- c(analysisName, "Cost by Concept")
    domainId <- c(domainId, "Cost")
  }
  
  data.frame(
    analysisId = analysisId,
    analysisName = analysisName,
    domainId = domainId,
    startDay = NA,
    endDay = NA,
    isBinary = "N",
    missingMeansZero = "Y"
  )
}

.createCovariateRef <- function(settings, analysisRef) {
  covariateRef <- list()
  
  window_df <- data.frame(windowId = integer(), windowName = character())
  if (!is.null(settings$timeWindows) && length(settings$timeWindows) > 0) {
    fixed_windows_df <- do.call(rbind, lapply(seq_along(settings$timeWindows), function(i) {
      data.frame(
        windowId = i,
        windowName = paste0(" (", settings$timeWindows[[i]][1], "d to ", settings$timeWindows[[i]][2], "d)")
      )
    }))
    window_df <- rbind(window_df, fixed_windows_df)
  }
  if (settings$useInCohortWindow) {
    in_cohort_window_df <- data.frame(windowId = 999, windowName = " (In Cohort)")
    window_df <- rbind(window_df, in_cohort_window_df)
  }
  
  if (settings$calculateTotalCost) {
    df <- window_df
    df$covariateId <- 10000 + df$windowId
    df$covariateName <- paste0("Total Cost", df$windowName)
    df$analysisId <- 1
    df$conceptId <- 0
    covariateRef[[length(covariateRef) + 1]] <- df
  }
  if (!is.null(settings$costDomains)) {
    domain_df <- data.frame(
      domainId = 1:6,
      domainName = c("Drug", "Procedure", "Visit", "Device", "Measurement", "Observation")
    )
    domain_df <- domain_df[domain_df$domainName %in% settings$costDomains, ]
    
    df <- tidyr::crossing(window_df, domain_df)
    df$covariateId <- 20000 + (df$windowId * 100) + df$domainId
    df$covariateName <- paste0("Cost - ", df$domainName, df$windowName)
    df$analysisId <- 2
    df$conceptId <- 0
    covariateRef[[length(covariateRef) + 1]] <- df
  }
  if (!is.null(settings$utilizationDomains)) {
    util_df <- data.frame(
      domainId = 1:4,
      domainName = c("Drug", "Procedure", "Visit", "Device")
    )
    util_df <- util_df[util_df$domainName %in% settings$utilizationDomains, ]
    
    df <- tidyr::crossing(window_df, util_df)
    df$covariateId <- 30000 + (df$windowId * 100) + df$domainId
    df$covariateName <- paste0("Count of ", df$domainName, " Events", df$windowName)
    df$analysisId <- 3
    df$conceptId <- 0
    covariateRef[[length(covariateRef) + 1]] <- df
  }
  if (settings$calculateLengthOfStay) {
    df <- window_df
    df$covariateId <- 40000 + df$windowId
    df$covariateName <- paste0("Length of Stay for Inpatient Visits", df$windowName)
    df$analysisId <- 4
    df$conceptId <- 0
    covariateRef[[length(covariateRef) + 1]] <- df
  }
  if (!is.null(settings$conceptIds) && length(settings$conceptIds) > 0) {
    concept_df <- data.frame(
      conceptId = settings$conceptIds,
      concept_rank = seq_along(settings$conceptIds)
    )
    
    df <- tidyr::crossing(window_df, concept_df)
    df$covariateId <- 50000 + (df$windowId * 10000) + df$concept_rank
    df$covariateName <- paste0("Cost - Concept ", df$conceptId, df$windowName)
    df$analysisId <- 5
    # conceptId is already in the data frame
    covariateRef[[length(covariateRef) + 1]] <- df
  }
  
  dplyr::bind_rows(covariateRef) %>%
    dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId)
}