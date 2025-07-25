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

#' Get Cost and Utilization Data From the Database
#'
#' @description
#' Constructs covariates for cost and healthcare utilization for one or more cohorts. This is the main
#' workhorse function of the `CostUtilization` package.
#'
#' @details
#' This function constructs a set of covariates based on the provided settings. It returns results
#' as a standard `CovariateData` object compatible with the OHDSI ecosystem.
#'
#' @param connection              A connection to the server containing the OMOP CDM instance.
#' @param cdmDatabaseSchema       The name of the database schema that contains the OMOP CDM instance.
#' @param cohortDatabaseSchema    The name of the database schema where the cohort table is located.
#' @param cohortTable             The name of the table holding the cohort(s).
#' @param cohortIds               A vector of `cohort_definition_id`s to retrieve covariates for.
#' @param costUtilSettings        An S3 object of type `costUtilSettings` as created by `createCostUtilSettings()`.
#' @param aggregated              A logical flag indicating whether to compute population-level aggregate statistics
#'                                (`TRUE`) or person-level data (`FALSE`).
#' @param tempEmulationSchema     A schema with write privileges for emulating temp tables on platforms like Oracle.
#'
#' @return
#' A `CovariateData` object, which is an `Andromeda` object containing the results.
#'
#' @export
getCostUtilData <- function(connection,
                            cdmDatabaseSchema,
                            cohortDatabaseSchema,
                            cohortTable,
                            cohortIds,
                            costUtilSettings,
                            aggregated = FALSE,
                            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  checkmate::assertClass(connection, "DBIConnection")
  checkmate::assertClass(costUtilSettings, "costUtilSettings")
  
  # Generate reference tables from settings
  references <- .generateReferenceTables(costUtilSettings)
  
  if (nrow(references$analysisRef) == 0) {
    ParallelLogger::logWarn("No analyses specified in settings. Returning an empty CovariateData object.")
    return(.createEmptyCovariateData(costUtilSettings, cohortIds, aggregated))
  }
  
  # Upload temporal windows to temp table
  temporalWindows <- references$temporalWindows
  DatabaseConnector::insertTable(connection,
                                 tableName = "#temporal_windows",
                                 data = temporalWindows,
                                 tempTable = TRUE,
                                 dropTableIfExists = TRUE
  )
  
  # Prepare for cost standardization if requested
  standardizationInfo <- .prepareStandardization(costUtilSettings, connection)
  
  # Construct and execute SQL
  ParallelLogger::logInfo("Constructing cost & utilization covariates on the server.")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "GetCostAndUtilization.sql",
    packageName = "CostUtilization",
    dbms = DatabaseConnector::dbms(connection),
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds,
    cost_domains = .toSingleQuotes(costUtilSettings$costDomains),
    utilization_domains = .toSingleQuotes(costUtilSettings$utilizationDomains),
    cost_type_concept_ids = costUtilSettings$costTypeConceptIds,
    currency_concept_id = costUtilSettings$currencyConceptId,
    use_total_cost = costUtilSettings$calculateTotalCost,
    use_domain_cost = !is.null(costUtilSettings$costDomains) && length(costUtilSettings$costDomains) > 0,
    use_utilization = !is.null(costUtilSettings$utilizationDomains) && length(costUtilSettings$utilizationDomains) > 0,
    use_length_of_stay = costUtilSettings$calculateLengthOfStay,
    use_cost_standardization = standardizationInfo$useStandardization
  )
  
  # Create a temp table with person-level results
  personLevelTable <- paste0("#person_covs_", paste(sample(letters, 10), collapse = ""))
  sql <- SqlRender::render(sql, person_level_table = personLevelTable)
  DatabaseConnector::executeSql(connection, sql)
  
  covariateData <- Andromeda::andromeda()
  
  if (aggregated) {
    ParallelLogger::logInfo("Aggregating results.")
    aggSql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "AggregateCovariates.sql",
      packageName = "CostUtilization",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      person_level_table = personLevelTable
    )
    DatabaseConnector::querySqlToAndromeda(connection,
                                           aggSql,
                                           andromeda = covariateData,
                                           andromedaTableName = "covariatesContinuous",
                                           snakeCaseToCamelCase = TRUE
    )
  } else {
    ParallelLogger::logInfo("Fetching person-level results.")
    fetchSql <- "SELECT row_id, covariate_id, covariate_value FROM @person_level_table;"
    fetchSql <- SqlRender::render(fetchSql, person_level_table = personLevelTable)
    fetchSql <- SqlRender::translate(fetchSql, targetDialect = connection@dbms, tempEmulationSchema = tempEmulationSchema)
    DatabaseConnector::querySqlToAndromeda(connection,
                                           fetchSql,
                                           andromeda = covariateData,
                                           andromedaTableName = "covariates",
                                           snakeCaseToCamelCase = TRUE
    )
  }
  
  # Clean up the temp tables
  .cleanupTempTables(connection, personLevelTable, standardizationInfo$useStandardization)
  
  # Finalize the CovariateData object
  covariateData$analysisRef <- references$analysisRef
  covariateData$covariateRef <- references$covariateRef
  
  populationSize <- .getPopulationSize(connection, cohortDatabaseSchema, cohortTable, cohortIds)
  metaData <- list(
    call = match.call(),
    cohortIds = cohortIds,
    aggregated = aggregated,
    populationSize = populationSize
  )
  attr(covariateData, "metaData") <- metaData
  class(covariateData) <- "CovariateData"
  
  return(covariateData)
}

#' @noRd
.createEmptyCovariateData <- function(costUtilSettings, cohortIds, aggregated) {
  covariateData <- Andromeda::andromeda()
  metaData <- list(
    call = match.call(definition = getCostUtilData, call = sys.call(-1)),
    cohortIds = cohortIds,
    aggregated = aggregated,
    populationSize = 0
  )
  attr(covariateData, "metaData") <- metaData
  class(covariateData) <- "CovariateData"
  return(covariateData)
}

#' @noRd
.getPopulationSize <- function(connection, cohortDatabaseSchema, cohortTable, cohortIds) {
  sql <- "SELECT COUNT_BIG(DISTINCT subject_id) FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id IN (@cohort_ids);"
  sql <- SqlRender::render(sql, cohort_database_schema = cohortDatabaseSchema, cohort_table = cohortTable, cohort_ids = cohortIds)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms)
  return(DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)$count)
}

#' @noRd
.prepareStandardization <- function(settings, connection) {
  useStandardization <- !is.null(settings$costStandardizationYear)
  if (useStandardization) {
    cpiData <- provideDefault(settings$cpiData, getDefaultCpiTable())
    targetCpi <- cpiData$cpi[cpiData$year == settings$costStandardizationYear]
    if (length(targetCpi) == 0) {
      stop("costStandardizationYear '", settings$costStandardizationYear, "' not found in CPI data.")
    }
    cpiTableToUpload <- cpiData %>%
      dplyr::mutate(inflation_factor = targetCpi / .data$cpi) %>%
      dplyr::select("year", "inflation_factor")
    DatabaseConnector::insertTable(connection, "#cpi_data", cpiTableToUpload, tempTable = TRUE, dropTableIfExists = TRUE)
  }
  return(list(useStandardization = useStandardization))
}

#' @noRd
.cleanupTempTables <- function(connection, personLevelTable, usedCpi) {
  cleanupSql <- "TRUNCATE TABLE @person_level_table; DROP TABLE @person_level_table;"
  if (usedCpi) {
    cleanupSql <- paste(cleanupSql, "TRUNCATE TABLE #cpi_data; DROP TABLE #cpi_data;")
  }
  cleanupSql <- SqlRender::render(cleanupSql, person_level_table = personLevelTable)
  cleanupSql <- SqlRender::translate(cleanupSql, targetDialect = connection@dbms)
  DatabaseConnector::executeSql(connection, cleanupSql, reportOverallTime = FALSE, progressBar = FALSE)
}

#' @noRd
.toSingleQuotes <- function(vec) {
  if (is.null(vec) || length(vec) == 0) {
    return("''")
  }
  paste0("'", paste(vec, collapse = "', '"), "'")
}

#' Provides a default value if the primary value is NULL
#' @noRd
provideDefault <- function(a, b) {
  if (is.null(a)) b else a
}