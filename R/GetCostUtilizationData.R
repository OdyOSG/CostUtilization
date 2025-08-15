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
#' to execute it. All heavy computation is performed on the database server.
#'
#' When a `conceptSetDefinition` is used, this function first executes a
#' preliminary query to identify the relevant domains, ensuring the main analysis
#' query is as efficient as possible by only scanning necessary event tables.
#'
#' @param connectionDetails         An R object of type `connectionDetails` created by the
#'                                  `DatabaseConnector::createConnectionDetails` function.
#' @param cdmDatabaseSchema         The name of the database schema that contains the OMOP CDM
#'                                  instance.
#' @param cohortDatabaseSchema      The name of the database schema that contains the cohort table.
#' @param cohortTable               The name of the cohort table.
#' @param cohortIds                 A numeric vector of cohort definition IDs to include in the
#'                                  analysis. If -1 - all cohorts in table will be used.
#' @param costUtilizationSettings          An S3 object of class `costUtilizationSettings` created by the
#'                                  `createCostUtilizationSettings()` function.
#'
#' @return
#' A `tibble` containing the aggregated cost and HRU statistics.
#'
#' @export
getDbCostData <- function(
    connectionDetails = NULL,
    connection = NULL,
    cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable,
    cohortIds,
    costUtilizationSettings,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(costUtilizationSettings, "costUtilizationSettings")
  if (is.null(connectionDetails) && is.null(connection)) {
    stop("Need to provide either connectionDetails or connection")
  }
  if (!is.null(connectionDetails) && !is.null(connection)) {
    stop("Need to provide either connectionDetails or connection, not both")
  }

  if (!is.null(connectionDetails)) {
    checkmate::assertClass(connectionDetails, "ConnectionDetails")
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  } else {
    checkmate::assertClass(connection, "DatabaseConnectorJdbcConnection")
  }

  checkmate::assertCharacter(cohortTable, max.len = 1)
  checkmate::assertNumeric(cohortIds, lower = -1, any.missing = FALSE, min.len = 1)
  .route <- NULL
  
  # useInflationAdjustment <- costUtilizationSettings$adjustForInflation
  # inflationTable <- NULL
  # if (useInflationAdjustment) {
  #   inflationTable <- paste0("#inflation_data_", paste(sample(letters, 10), collapse = ""))
  #   cli::cli_alert_info("Uploading inflation data to `{inflationTable}`.")
  #   DatabaseConnector::insertTable(
  #     connection = connection,
  #     tableName = inflationTable,
  #     data = costUtilizationSettings$inflationDataTable,
  #     dropTableIfExists = TRUE,
  #     createTable = TRUE
  #   )
  # }
  
  if (!is.null(costUtilizationSettings$conceptSetDefinition)) {
    .route <- conceptSetRoute(costUtilizationSettings, connection, cdmDatabaseSchema)
  } else if (!is.null(costUtilizationSettings$costDomains)) {
    .route <- buildEventUnionSpec(costUtilizationSettings)
  } 
  analysisSql <- generateCostAnalysisSql(costUtilizationSettings, cohortIds, .route)
  
  
}
