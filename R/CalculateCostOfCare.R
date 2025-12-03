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
#' @param connection A live `DatabaseConnector` connection object.
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
    rlang::abort("Provide either `connectionDetails` or an open `connection`.")
  }
  if (!is.null(connectionDetails) && !is.null(connection)) {
    rlang::abort("Provide exactly one of `connectionDetails` or `connection`, not both.")
  }

  connectionOwner <- FALSE
  if (!is.null(connectionDetails)) {
    checkmate::assertClass(connectionDetails, "ConnectionDetails")
    connection <- DatabaseConnector::connect(connectionDetails)
    connectionOwner <- TRUE
    on.exit({
      if (connectionOwner && !is.null(connection)) {
        tryCatch({
          DatabaseConnector::disconnect(connection)
        }, error = function(e) {
          logMessage(paste("Warning: Error disconnecting:", e$message), verbose, "WARN")
        })
      }
    }, add = TRUE)
  } else {
    # Validate that connection is a DatabaseConnector connection
    if (!inherits(connection, "DatabaseConnectorConnection")) {
      rlang::abort("Connection must be a DatabaseConnector connection object.")
    }
  }
  
  # --- Setup ---
  startTime <- Sys.time()
  dbms <- DatabaseConnector::dbms(connection)
  sessionPrefix <- getSessionTempTablePrefix(dbms)
  restrictVisitTableName <- NULL
  eventConceptsTableName <- NULL
  cpiAdjTableName <- NULL
  
  # Cleanup function for temporary tables
  cleanupTables <- function() {
    if (!is.null(restrictVisitTableName)) {
      tryCatch({
        dropTempTable(connection, restrictVisitTableName, tempEmulationSchema)
      }, error = function(e) {
        logMessage(paste("Warning: Could not drop temp table", restrictVisitTableName, ":", e$message), verbose, "WARN")
      })
    }
    if (!is.null(eventConceptsTableName)) {
      tryCatch({
        dropTempTable(connection, eventConceptsTableName, tempEmulationSchema)
      }, error = function(e) {
        logMessage(paste("Warning: Could not drop temp table", eventConceptsTableName, ":", e$message), verbose, "WARN")
      })
    }
    if (!is.null(cpiAdjTableName)) {
      tryCatch({
        dropTempTable(connection, cpiAdjTableName, tempEmulationSchema)
      }, error = function(e) {
        logMessage(paste("Warning: Could not drop temp table", cpiAdjTableName, ":", e$message), verbose, "WARN")
      })
    }
  }
  
  on.exit(cleanupTables(), add = TRUE)

  # --- CPI Adjustment: prepare adjustment factors table (if enabled) ---
  if (costOfCareSettings$cpiAdjustment) {
    logMessage("Setting up CPI adjustment...", verbose, "INFO")
    cpiAdjTableName <- paste0(sessionPrefix, "_cpi_adj")

    # Load CPI data from explicit path; caller validated existence already
    cpiPath <- costOfCareSettings$cpiFilePath
    cpiData <- utils::read.csv(cpiPath, stringsAsFactors = FALSE)

    if (!all(c("year", "adj_factor") %in% names(cpiData))) {
      # Backward compatibility: allow a file with `year` and `cpi` by renaming
      if (all(c("year", "cpi") %in% names(cpiData))) {
        cpiData$adj_factor <- cpiData$cpi
      } else {
        rlang::abort("CPI data must contain columns 'year' and 'adj_factor' (or 'year' and 'cpi').")
      }
    }

    cpiData <- cpiData[, c("year", "adj_factor"), drop = FALSE]
    checkmate::assertIntegerish(cpiData$year, lower = 1900, any.missing = FALSE)
    checkmate::assertNumeric(cpiData$adj_factor, any.missing = FALSE)

    tryCatch({
      cpiAdjTableName <- insertTempTable(
        connection = connection,
        tableName = cpiAdjTableName,
        data = cpiData,
        tempEmulationSchema = tempEmulationSchema
      )
      logMessage(sprintf("Uploaded %d CPI rows to %s", nrow(cpiData), cpiAdjTableName), verbose, "DEBUG")
    }, error = function(e) {
      rlang::abort(sprintf("Failed to create CPI adjustment table: %s", e$message))
    })
  }

  # --- Upload helper tables ---
  if (costOfCareSettings$hasVisitRestriction) {
    restrictVisitTableName <- paste0(sessionPrefix, "_visit_restr")
    visitConcepts <- data.frame(visit_concept_id = costOfCareSettings$restrictVisitConceptIds)
    
    tryCatch({
      restrictVisitTableName <- insertTempTable(
        connection = connection,
        tableName = restrictVisitTableName,
        data = visitConcepts,
        tempEmulationSchema = tempEmulationSchema
      )
      logMessage(sprintf("Uploaded %d visit concepts to %s", nrow(visitConcepts), restrictVisitTableName), verbose, "DEBUG")
    }, error = function(e) {
      rlang::abort(sprintf("Failed to create visit restriction table: %s", e$message))
    })
  }

  if (costOfCareSettings$hasEventFilters) {
    eventConceptsTableName <- paste0(sessionPrefix, "_evt_concepts")
    
    # Build event concepts table using base R
    eventConcepts <- NULL
    for (i in seq_along(costOfCareSettings$eventFilters)) {
      filter <- costOfCareSettings$eventFilters[[i]]
      filterData <- data.frame(
        filter_id = i,
        filter_name = filter$name,
        domain_scope = filter$domain,
        concept_id = as.integer(filter$conceptIds),
        stringsAsFactors = FALSE
      )
      if (is.null(eventConcepts)) {
        eventConcepts <- filterData
      } else {
        eventConcepts <- rbind(eventConcepts, filterData)
      }
    }

    tryCatch({
      eventConceptsTableName <- insertTempTable(
        connection = connection,
        tableName = eventConceptsTableName,
        data = eventConcepts,
        tempEmulationSchema = tempEmulationSchema
      )
      logMessage(sprintf("Uploaded %d event concept rows to %s", nrow(eventConcepts), eventConceptsTableName), verbose, "DEBUG")
    }, error = function(e) {
      rlang::abort(sprintf("Failed to create event concepts table: %s", e$message))
    })
  }

  params <- list(
    # core
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = as.integer(cohortId),

    # output / helper tables (unqualified here)
    restrictVisitTable = restrictVisitTableName,
    eventConceptsTable = eventConceptsTableName,
    cpiAdjTable = cpiAdjTableName,

    # window & anchor
    anchorOnEnd = identical(costOfCareSettings$anchorCol, "cohort_end_date"),
    timeA = as.integer(costOfCareSettings$startOffsetDays),
    timeB = as.integer(costOfCareSettings$endOffsetDays),

    # flags & knobs (logical where appropriate; numbers as integers)
    hasVisitRestriction = costOfCareSettings$hasVisitRestriction,
    hasEventFilters = costOfCareSettings$hasEventFilters,
    nFilters = as.integer(if (is.null(costOfCareSettings$nFilters)) 0L else costOfCareSettings$nFilters),
    microCosting = costOfCareSettings$microCosting, # pass-through (string/int as your SQL expects)
    cpiAdjustment = costOfCareSettings$cpiAdjustment,

    # costing
    costConceptId = as.integer(costOfCareSettings$costConceptId),
    currencyConceptId = as.integer(costOfCareSettings$currencyConceptId),
    aggregated = aggregated,
    # primary filter id (index in eventFilters) if named
    primaryFilterId = .findPrimaryFilterId(costOfCareSettings)
  )

  # --- Fetch & return results ---
  logMessage("Executing cost of care analysis...", verbose, "INFO")

  tryCatch({
    results <- .fetchResults(params, connection, tempEmulationSchema, verbose)
  }, error = function(e) {
    rlang::abort(sprintf("Failed to execute cost of care analysis: %s", e$message))
  })

  logMessage(
    sprintf("Analysis complete in %0.1fs.", as.numeric(difftime(Sys.time(), startTime, units = "secs"))),
    verbose = verbose,
    level = "SUCCESS"
  )
  
  return(results)
}