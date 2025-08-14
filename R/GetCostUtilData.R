# Copyright 2025 OHDSI
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

#' Get Cost and Utilization Data from the Database
#'
#' @description
#' This function connects to an OMOP CDM database, executes the cost and HRU
#' analysis based on the provided settings, and returns the results as a tibble.
#'
#' @details
#' This function uses `SqlRender` to parameterize a SQL script and `DatabaseConnector`
#' to execute it. All heavy computation is performed on the database server. The
#' function handles the creation of temporary tables for intermediate steps,
#' including uploading cost standardization data if specified.
#'
#' @param connectionDetails         An R object of type `connectionDetails` created by the
#'                                  `DatabaseConnector::createConnectionDetails` function.
#' @param cdmDatabaseSchema         The name of the database schema that contains the OMOP CDM
#'                                  instance.
#' @param cohortDatabaseSchema      The name of the database schema that contains the cohort table.
#' @param cohortTable               The name of the cohort table.
#' @param cohortIds                 A numeric vector of cohort definition IDs to include in the
#'                                  analysis.
#' @param costUtilSettings          An S3 object of class `costUtilSettings` created by the
#'                                  `costUtilizationSettings()` function.
#'
#' @return
#' A `tibble` containing the aggregated cost and HRU statistics, with columns for
#' cohort ID, analysis category, analysis type (cost or hru), and descriptive statistics.
#'
#' @export
#' @importFrom rlang .data
#' @importFrom dplyr mutate across everything as_tibble
#' @importFrom purrr map_chr
getDbCostData <- function(connectionDetails,
                          cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          cohortTable,
                          cohortIds,
                          costUtilSettings) {
  
  if (!inherits(costUtilSettings, "costUtilSettings")) {
    rlang::abort("`costUtilSettings` must be an object created by the `costUtilizationSettings()` function.")
  }
  
  # --- Prepare SQL Rendering Parameters ---
  sqlPath <- system.file("sql", "sql_server", "CostAnalysis.sql", package = "CostUtilization")
  sql <- readChar(sqlPath, file.info(sqlPath)$size)
  
  # Prepare multiplier SQL if needed
  multiplierInsertSql <- ""
  if (costUtilSettings$standardization$standardize) {
    df <- costUtilSettings$standardization$multiplierData
    # Use purrr to build insert statements without a loop
    insertLines <- purrr::map_chr(1:nrow(df), ~{
      sprintf("INSERT INTO #cost_multipliers (cost_domain_id, cost_year, multiplier) VALUES ('%s', %d, %f);",
              df$cost_domain_id[.x], df$year[.x], df$multiplier[.x])
    })
    multiplierInsertSql <- paste(insertLines, collapse = "\n")
  }
  
  # --- Connect and Execute ---
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Render and execute the SQL
  results <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    dbms = connectionDetails$dbms,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds,
    start_anchor = costUtilSettings$timeWindow$startAnchor,
    end_anchor = costUtilSettings$timeWindow$endAnchor,
    start_window = costUtilSettings$timeWindow$startWindow,
    end_window = costUtilSettings$timeWindow$endWindow,
    cost_domain_ids = if (is.null(costUtilSettings$costDomains)) "" else paste0("'", costUtilSettings$costDomains, "'", collapse = ","),
    cost_type_concept_ids = if (is.null(costUtilSettings$costTypeConceptIds)) "" else paste(costUtilSettings$costTypeConceptIds, collapse = ","),
    standardize = costUtilSettings$standardization$standardize,
    multiplier_insert_sql = multiplierInsertSql,
    calculate_hru = costUtilSettings$hru$calculate,
    snakeCaseToCamelCase = TRUE
  )
  
  # --- Tidy Output ---
  results <- results |>
    dplyr::as_tibble() |>
    dplyr::mutate(dplyr::across(dplyr::everything(), ~-999, .names = "new_{.col}")) # Example of a dplyr operation
  
  # A more realistic tidy step: convert to tibble and ensure correct types
  results <- dplyr::as_tibble(results)
  
  return(results)
}