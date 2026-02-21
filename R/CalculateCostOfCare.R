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
#' Calculate Cost of Care Analysis
#'
#' @description
#' Runs the cost-of-care analysis for a specified cohort using a validated
#' `CostOfCareSettings` object. Creates temp helper tables as needed
#' (visit restrictions, event filters, CPI) and executes the SQL plan.
#'
#' @param connection A live `DatabaseConnector` or `DBI` connection object.
#' @param connectionDetails Optional `ConnectionDetails` to create a connection
#'   if `connection` is not supplied (provide exactly one of the two).
#' @param cdmDatabaseSchema Schema (or database) that contains CDM tables.
#' @param cohortDatabaseSchema Schema (or database) that contains the cohort table.
#' @param cohortTable Name of the cohort table.
#' @param cohortId Numeric cohort definition id to analyze.
#' @param costOfCareSettings A settings object from `createCostOfCareSettings()`.
#' @param aggregated	 Should aggregate statistics be computed instead of covariates per cohort entry? If aggregated is set to FALSE, the results returned will be based on each subject_id and cohort_start_date in your cohort table. If your cohort contains multiple entries for the same subject_id (due to different cohort_start_date values), you must carefully set the rowIdField so you can identify the patients properly.
#' @param tempEmulationSchema Optional schema for temp table emulation (Oracle/Redshift/...).
#' @param verbose Logical; print progress messages.
#'
#' @return results andromeda object
#' @export
calculateCostOfCare <- function(
    connection = NULL,
    connectionDetails = NULL,
    cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable,
    cohortId,
    costOfCareSettings,
    aggregated = TRUE,
    tempEmulationSchema = NULL,
    verbose = TRUE) {
  
  # --- Validation / connection management ---
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(costOfCareSettings, "CostOfCareSettings", add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(cohortDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(cohortTable, len = 1, add = errorMessages)
  checkmate::assertIntegerish(cohortId, len = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertFlag(aggregated, add = errorMessages)
  checkmate::assertFlag(verbose, add = errorMessages)
  checkmate::reportAssertions(errorMessages)
  
  if (is.null(connectionDetails) && is.null(connection)) {
    stop("Provide either `connectionDetails` or an open `connection`.", call. = FALSE)
  }
  if (!is.null(connectionDetails) && !is.null(connection)) {
    stop("Provide exactly one of `connectionDetails` or `connection`, not both.", call. = FALSE)
  }
  
  if (!is.null(connectionDetails)) {
    checkmate::assertClass(connectionDetails, "ConnectionDetails")
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  } else {
    checkmate::assertClass(connection, "DBIConnection")
  }
  
  # --- Setup ---
  startTime <- Sys.time()
  # SqlRender returns a vector; taking first element
  sessionPrefix <- SqlRender::translate("#", "oracle")[[1]]
  restrictVisitTableName <- NULL
  eventConceptsTableName <- NULL
  cpiAdjTableName <- NULL
  
  # --- CPI Adjustment ---
  if (costOfCareSettings$cpiAdjustment) {
    logMessage("Setting up CPI adjustment...", verbose, "INFO")
    
    cpiPath <- costOfCareSettings$cpiFilePath
    cpiData <- utils::read.csv(cpiPath, stringsAsFactors = FALSE)
    
    if (!all(c("year", "adj_factor") %in% names(cpiData))) {
      if (all(c("year", "cpi") %in% names(cpiData))) {
        cpiData$adj_factor <- cpiData$cpi
      } else {
        stop("CPI data must contain columns 'year' and 'adj_factor' (or 'year' and 'cpi').", call. = FALSE)
      }
    }
    
    cpiData <- cpiData[, c("year", "adj_factor")]
    checkmate::assertIntegerish(cpiData$year, lower = 1900, any.missing = FALSE)
    checkmate::assertNumeric(cpiData$adj_factor, any.missing = FALSE)
    
    cpiAdjTableName <- paste0(sessionPrefix, "_cpi_adj")
    cpiAdjTableName <- insertTableDBI(
      connection = connection,
      tableName = cpiAdjTableName,
      data = cpiData,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE
    )
    logMessage(sprintf("Uploaded %d CPI rows to #%s", nrow(cpiData), cpiAdjTableName), verbose, "DEBUG")
  }
  
  # --- Upload helper tables ---
  if (costOfCareSettings$hasVisitRestriction) {
    restrictVisitTableName <- paste0(sessionPrefix, "_visit_restr")
    # Base R alternative to tibble
    visitConcepts <- data.frame(
      visit_concept_id = costOfCareSettings$restrictVisitConceptIds,
      stringsAsFactors = FALSE
    )
    restrictVisitTableName <- insertTableDBI(
      connection = connection,
      tableName = restrictVisitTableName,
      data = visitConcepts,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE
    )
    logMessage(sprintf("Uploaded %d visit concepts to #%s", nrow(visitConcepts), restrictVisitTableName), verbose, "DEBUG")
  }
  
  if (costOfCareSettings$hasEventFilters) {
    eventConceptsTableName <- paste0(sessionPrefix, "_evt_concepts")
    
    # Base R alternative to purrr::map_dfr
    eventList <- lapply(seq_along(costOfCareSettings$eventFilters), function(i) {
      filter <- costOfCareSettings$eventFilters[[i]]
      data.frame(
        filter_id    = i,
        filter_name  = filter$name,
        domain_scope = filter$domain,
        concept_id   = as.integer(filter$conceptIds),
        stringsAsFactors = FALSE
      )
    })
    eventConcepts <- do.call(rbind, eventList)
    
    eventConceptsTableName <- insertTableDBI(
      connection = connection,
      tableName = eventConceptsTableName,
      data = eventConcepts,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE
    )
    logMessage(sprintf("Uploaded %d event concept rows to #%s", nrow(eventConcepts), eventConceptsTableName), verbose, "DEBUG")
  }
  
  # --- Prepare Params ---
  # Replaced %||% with logic check
  nFiltersVal <- if (is.null(costOfCareSettings$nFilters)) 0L else as.integer(costOfCareSettings$nFilters)
  
  params <- list(
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = as.integer(cohortId),
    restrictVisitTable = restrictVisitTableName,
    eventConceptsTable = eventConceptsTableName,
    cpiAdjTable = cpiAdjTableName,
    anchorOnEnd = identical(costOfCareSettings$anchorCol, "cohort_end_date"),
    timeA = as.integer(costOfCareSettings$startOffsetDays),
    timeB = as.integer(costOfCareSettings$endOffsetDays),
    hasVisitRestriction = costOfCareSettings$hasVisitRestriction,
    hasEventFilters = costOfCareSettings$hasEventFilters,
    nFilters = nFiltersVal,
    microCosting = costOfCareSettings$microCosting,
    cpiAdjustment = costOfCareSettings$cpiAdjustment,
    costConceptId = as.integer(costOfCareSettings$costConceptId),
    currencyConceptId = as.integer(costOfCareSettings$currencyConceptId),
    aggregated = aggregated,
    primaryFilterId = .findPrimaryFilterId(costOfCareSettings)
  )
  
  # --- Fetch & return results ---
  logMessage("Fetching results from database...", verbose, "INFO")
  .res <- .fetchResults(params, connection, tempEmulationSchema, verbose)
  
  logMessage(
    sprintf("Analysis complete in %0.1fs.", as.numeric(difftime(Sys.time(), startTime, units = "secs"))),
    verbose = verbose,
    level = "SUCCESS"
  )
  return(.res)
}