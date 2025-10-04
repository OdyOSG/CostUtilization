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
#' Fetch Results from Executed SQL Plan
#' #' @description
#' Fetches the results of the cost-of-care analysis after executing the SQL
#' plan. Returns the results as an Andromeda object.
#' #' @param params Named list of parameters for SQL rendering (can be camelCase or snake_case).
#' @param connection A live `DatabaseConnector` or `DBI` connection object.
#' @param tempEmulationSchema Optional schema for temp table emulation (Oracle/Redshift/...).
#' @param verbose Logical; print progress messages.
#' @return An Andromeda object containing the results.
#' @noRd
.fetchResults <- function(params, connection, tempEmulationSchema, verbose = TRUE) {
  fetchSql <- executeSqlPlan(
    connection          = connection,
    params              = params,
    targetDialect       = DatabaseConnector::dbms(connection),
    tempEmulationSchema = tempEmulationSchema,
    verbose             = verbose
  )
  andr <- Andromeda::andromeda(
    results = DBI::dbGetQuery(
      connection,
      fetchSql
    )
  )
  return(andr)
}
