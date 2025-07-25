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

#' Construct Cost and Utilization Covariates
#'
#' @description
#' This is an S3 method for the `FeatureExtraction::getDbCovariateData` generic.
#' It is called when the `covariateSettings` object is of type `costUtilSettings`.
#'
#' @details
#' This function is not intended to be called directly but is automatically dispatched
#' by `FeatureExtraction::getDbCovariateData`. It serves as a wrapper around the main
#' `getCostUtilData` function to ensure compatibility with the OHDSI framework.
#'
#' @param connection              A connection to the server containing the schema.
#' @param cdmDatabaseSchema       The name of the database schema that contains the OMOP CDM instance.
#' @param cohortDatabaseSchema    The name of the database schema where the cohort table is located.
#' @param cohortTable             Name of the table holding the cohort.
#' @param cohortId                DEPRECATED. Use `cohortIds` instead.
#' @param cohortIds               A vector of cohort definition IDs.
#' @param cdmVersion              The version of the OMOP CDM.
#' @param rowIdField              The name of the field in the cohort table that is to be used as the `row_id` field.
#' @param covariateSettings       An S3 object of type `costUtilSettings`.
#' @param aggregated              A logical flag indicating whether to compute aggregate statistics (`TRUE`) or person-level data (`FALSE`).
#' @param tempEmulationSchema     A schema with write privileges for emulating temp tables.
#'
#' @return
#' A `CovariateData` object.
#'
#' @method getDbCovariateData costUtilSettings
#' @export
getDbCovariateData.costUtilSettings <- function(connection,
                                                cdmDatabaseSchema,
                                                cohortDatabaseSchema,
                                                cohortTable,
                                                cohortId = NULL,
                                                cohortIds,
                                                cdmVersion = "5",
                                                rowIdField = "subject_id",
                                                covariateSettings,
                                                aggregated = FALSE,
                                                tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  if (!is.null(cohortId)) {
    warning("The 'cohortId' argument is deprecated. Please use 'cohortIds'.")
    cohortIds <- cohortId
  }
  
  results <- getCostUtilData(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohortIds,
    costUtilSettings = covariateSettings,
    aggregated = aggregated,
    tempEmulationSchema = tempEmulationSchema
  )
  # Ensure the rowIdField is correctly stored in metadata for FeatureExtraction compatibility
  attr(results, "metaData")$rowIdField <- rowIdField
  return(results)
}

#' Internal helper to generate reference tables from the settings object
#' @noRd
.generateReferenceTables <- function(settings) {
  analysisRef <- dplyr::tibble()
  covariateRef <- dplyr::tibble()
  
  windows <- settings$timeWindows
  if (length(windows) == 0) {
    return(list(analysisRef = analysisRef, covariateRef = covariateRef, temporalWindows = dplyr::tibble()))
  }
  
  temporalWindows <- do.call(rbind, lapply(windows, function(x) data.frame(startDay = x[1], endDay = x[2]))) %>%
    dplyr::distinct() %>%
    dplyr::mutate(windowId = dplyr::row_number())
  
  # Analysis 1: Total Cost
  if (settings$calculateTotalCost) {
    analysisId <- 1001
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Total Cost", domainId = "Cost", isBinary = "N", missingMeansZero = "Y")
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      temporalWindows %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 10000 + .data$windowId,
          covariateName = paste0("Total cost during day ", .data$startDay, " to ", .data$endDay),
          conceptId = 0
        )
    )
  }
  
  # Analysis 2: Cost By Domain
  if (!is.null(settings$costDomains) && length(settings$costDomains) > 0) {
    analysisId <- 1002
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Cost By Domain", domainId = "Cost", isBinary = "N", missingMeansZero = "Y")
    domains <- dplyr::tibble(domainName = settings$costDomains) %>% dplyr::mutate(domainIdValue = dplyr::row_number())
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      tidyr::crossing(temporalWindows, domains) %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 20000 + (.data$windowId * 100) + .data$domainIdValue,
          covariateName = paste0(.data$domainName, " cost during day ", .data$startDay, " to ", .data$endDay),
          conceptId = 0
        )
    )
  }
  
  # Analysis 3: Utilization
  if (!is.null(settings$utilizationDomains) && length(settings$utilizationDomains) > 0) {
    analysisId <- 1003
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Utilization", domainId = "Observation", isBinary = "N", missingMeansZero = "Y")
    domains <- dplyr::tibble(domainName = settings$utilizationDomains) %>% dplyr::mutate(domainIdValue = dplyr::row_number())
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      tidyr::crossing(temporalWindows, domains) %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 30000 + (.data$windowId * 100) + .data$domainIdValue,
          covariateName = paste0(.data$domainName, " utilization count during day ", .data$startDay, " to ", .data$endDay),
          conceptId = 0
        )
    )
  }
  
  # Analysis 4: Length of Stay
  if (settings$calculateLengthOfStay) {
    analysisId <- 1004
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Length of Stay", domainId = "Visit", isBinary = "N", missingMeansZero = "Y")
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      temporalWindows %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 40000 + .data$windowId,
          covariateName = paste0("Length of stay for inpatient visits during day ", .data$startDay, " to ", .data$endDay),
          conceptId = 0
        )
    )
  }
  
  return(list(
    analysisRef = analysisRef,
    covariateRef = covariateRef %>% dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId),
    temporalWindows = temporalWindows
  ))
}