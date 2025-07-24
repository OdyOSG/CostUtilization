# Copyright 2025 Observational Health Data Sciences and Informatics
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

#' Get Cost and Utilization Covariates From the Database
#'
#' @description
#' Constructs covariates for cost and healthcare utilization for one or more cohorts.
#'
#' @details
#' This function constructs a set of covariates based on the provided settings. It can return the results
#' as a `CovariateData` object in memory, or write them to tables in the database.
#'
#' @param connection              A connection to the server containing the OMOP CDM instance.
#' @param oracleTempSchema        DEPRECATED. Use `tempEmulationSchema` instead.
#' @param cdmDatabaseSchema       The name of the database schema that contains the OMOP CDM instance.
#' @param cohortTable             The name of the table holding the cohort(s).
#' @param cohortIds               A vector of `cohort_definition_id`s to retrieve covariates for.
#' @param cdmVersion              The version of the OMOP CDM. Currently, only "5" is supported.
#' @param rowIdField              The name of the field in the cohort table that is to be used as the
#'                                `row_id` field. Crucial for person-level data.
#' @param costCovariateSettings       An S3 object of type `costcostCovariateSettings` as created by `createCostcostCovariateSettings()`.
#' @param aggregated              A logical flag indicating whether to compute population-level aggregate statistics
#'                                (`TRUE`) or person-level data (`FALSE`).
#' @param tempEmulationSchema     A schema with write privileges for emulating temp tables.
#' @param targetDatabaseSchema    (Optional) The name of the database schema where the resulting covariate tables
#'                                should be stored.
#' @param targetCovariateTable    (Optional) The name of the table where the resulting covariates will
#'                                be stored. If not provided, results will be fetched to R.
#' @param targetCovariateRefTable (Optional) The name of the table where the covariate reference will be stored.
#' @param targetAnalysisRefTable  (Optional) The name of the table where the analysis reference will be stored.
#'
#' @return
#' If `targetCovariateTable` is not specified, a `CovariateData` object is returned.
#' Otherwise, nothing is returned.
#'
#' @export
getDbCostCovariateData <- function(connection,
                                   oracleTempSchema = NULL,
                                   cdmDatabaseSchema,
                                   cohortTable,
                                   cohortIds,
                                   cdmVersion = "5",
                                   rowIdField = "subject_id",
                                   costCovariateSettings,
                                   aggregated = FALSE,
                                   tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                   targetDatabaseSchema = NULL,
                                   targetCovariateTable = NULL,
                                   targetCovariateRefTable = NULL,
                                   targetAnalysisRefTable = NULL) {
  
  # --- Input Validation ---
  checkmate::assertClass(connection, "DBIConnection")
  checkmate::assertClass(costCovariateSettings, "costcostCovariateSettings")
  if (!is.null(targetCovariateTable) && aggregated) {
    stop("Writing aggregated results to a database table is not supported. Please use aggregated = FALSE or omit the targetCovariateTable argument.")
  }
  
  # --- Reference Table Generation ---
  references <- generateReferenceTablesFromUnifiedSettings(costCovariateSettings)
  if (nrow(references$temporalWindows) == 0 || nrow(references$analysisRef) == 0) {
    ParallelLogger::logWarn("No covariates to generate based on the provided settings.")
    if (is.null(targetCovariateTable)) {
      # Return empty CovariateData object
      covariateData <- Andromeda::andromeda()
      metaData <- list(call = match.call(), cohortIds = cohortIds, aggregated = aggregated)
      attr(covariateData, "metaData") <- metaData
      class(covariateData) <- "CovariateData"
      return(covariateData)
    } else {
      return(invisible(NULL))
    }
  }
  
  # --- Temp Table Setup ---
  # Upload temporal windows and concept sets to temp tables
  DatabaseConnector::insertTable(connection, "temporal_windows", references$temporalWindows, tempTable = TRUE, dropTableIfExists = TRUE, createTable = TRUE, camelCaseToSnakeCase = TRUE)
  f <- costCovariateSettings$filters
  hasIncludedConcepts <- length(f$includedCovariateConceptIds) > 0
  if (hasIncludedConcepts) {
    # (Logic for resolving and inserting included concepts as before)
  }
  hasExcludedConcepts <- length(f$excludedCovariateConceptIds) > 0
  if (hasExcludedConcepts) {
    # (Logic for resolving and inserting excluded concepts as before)
  }
  
  
  # --- SQL Generation ---
  sqlFilename <- if (aggregated) "GetAggregatedCostCovariates.sql" else "GetCostCovariates.sql"
  isAnalysisActive <- function(baseName) { any(grepl(baseName, names(which(unlist(costCovariateSettings$analyses))))) }
  
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = sqlFilename,
    packageName = "CostUtilization",
    dbms = DatabaseConnector::dbms(connection),
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cdmDatabaseSchema,
    cohort_table = cohortTable,
    cohort_id = cohortIds, # SQL script uses @cohort_id for filtering
    row_id_field = rowIdField,
    use_total_cost = isAnalysisActive("CostTotal"),
    use_cost_by_domain = isAnalysisActive("CostByDomain"),
    use_cost_by_type = isAnalysisActive("CostByType"),
    use_utilization = isAnalysisActive("Utilization"),
    cost_domains = f$costDomains,
    cost_type_concept_ids = f$costTypeConceptIds,
    currency_concept_ids = f$currencyConceptIds,
    has_included_concepts = hasIncludedConcepts,
    has_excluded_concepts = hasExcludedConcepts
  )
  
  ParallelLogger::logInfo("Constructing cost & utilization covariates on the server.")
  
  # --- Execution: Fetch to R or Write to Table ---
  if (is.null(targetCovariateTable)) {
    # Fetch to R using Andromeda
    covariateData <- Andromeda::andromeda()
    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      andromeda = covariateData,
      andromedaTableName = if (aggregated) "covariatesContinuous" else "covariates",
      snakeCaseToCamelCase = TRUE
    )
    covariateData$analysisRef <- references$analysisRef
    covariateData$covariateRef <- references$covariateRef
    
    metaData <- list(
      call = match.call(),
      sql = sql,
      cohortIds = cohortIds,
      aggregated = aggregated,
      rowIdField = rowIdField
    )
    attr(covariateData, "metaData") <- metaData
    class(covariateData) <- "CovariateData"
    return(covariateData)
    
  } else {
    # Write to database tables
    destinationTable <- if (is.null(targetDatabaseSchema)) targetCovariateTable else paste(targetDatabaseSchema, targetCovariateTable, sep = ".")
    insertSql <- paste("INSERT INTO @destination_table (row_id, covariate_id, covariate_value)", sql)
    insertSql <- SqlRender::render(insertSql, destination_table = destinationTable)
    DatabaseConnector::executeSql(connection, insertSql)
    
    # Write reference tables
    if (!is.null(targetCovariateRefTable)) {
      DatabaseConnector::insertTable(connection, targetCovariateRefTable, references$covariateRef, dropTableIfExists = FALSE, createTable = FALSE)
    }
    if (!is.null(targetAnalysisRefTable)) {
      DatabaseConnector::insertTable(connection, targetAnalysisRefTable, references$analysisRef, dropTableIfExists = FALSE, createTable = FALSE)
    }
    return(invisible(NULL))
  }
}