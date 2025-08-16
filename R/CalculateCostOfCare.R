#' @title Calculate Cost of Care on OMOP CDM
#' @description
#' Computes cost of care for a cohort within a censored window, with optional
#' encounter and event filters, and support for micro-costing.
#'
#' @param conn A DatabaseConnector connection object.
#' @param cdmSchema Schema containing OMOP CDM tables.
#' @param cohortSchema Schema containing the cohort table.
#' @param cohortTable The cohort table name.
#' @param cohortId The cohort definition ID to use.
#' @param anchorCol Anchor date column in cohort table: "cohort_start_date" or "cohort_end_date".
#' @param startOffsetDays Integer offset from anchor date to start window.
#' @param endOffsetDays Integer offset from anchor date to end window.
#' @param restrictVisitConceptIds Optional integer vector of visit_concept_ids to restrict analysis to.
#' @param eventFilters Optional list of event filters. See details.
#' @param microCosting If TRUE, computes costs at the visit_detail level.
#' @param costConceptId Concept ID for the cost (e.g., 31978 for 'Allowed Amount').
#' @param currencyConceptId Concept ID for the currency (e.g., 44818668 for 'USD').
#' @param primaryEventFilterName If micro-costing with multiple event filters, specifies which filter's events are primary. Defaults to the first.
#' @param targetDialect The SQL dialect of the target database. Autodetected if NULL.
#' @param tempEmulationSchema A schema for temporary tables if needed.
#' @param asPermanent If TRUE, helper tables are created as permanent tables in tempEmulationSchema.
#' @param returnFormat Return a "tibble" or a "list" with results and diagnostics.
#' @param verbose If TRUE, print informative messages.
#' @param logger A logger object (e.g., from ParallelLogger).
#'
#' @return A tibble or list with cost metrics and diagnostics.
#'
#' @export
calculateCostOfCare <- function(
    conn,
    cdmSchema,
    cohortSchema,
    cohortTable,
    cohortId,
    anchorCol = c("cohort_start_date", "cohort_end_date"),
    startOffsetDays = 0L,
    endOffsetDays   = 90L,
    restrictVisitConceptIds = NULL,
    eventFilters = NULL,
    microCosting = FALSE,
    costConceptId = 31978L,
    currencyConceptId = 44818668L,
    primaryEventFilterName = NULL,
    targetDialect = NULL,
    tempEmulationSchema = NULL,
    asPermanent = FALSE,
    returnFormat = c("tibble","list"),
    verbose = TRUE,
    logger = NULL
) {
  # --- 1. Setup & Validation ---
  returnFormat <- rlang::arg_match(returnFormat)
  anchorCol <- rlang::arg_match(anchorCol)
  if (is.null(targetDialect)) {
    targetDialect <- DatabaseConnector::dbms(conn)
  }
  validateInputs(as.list(environment()))
  if (microCosting) {
    checkMicrocostingPrerequisites(conn, cdmSchema)
  }
  
  # --- 2. Materialize Helper Tables ---
  tableNameRoot <- paste0("cu_", paste(sample(letters, 10), collapse = ""))
  restrictVisitTable <- NULL
  eventConceptsTable <- NULL
  
  withr::defer({
    if (!asPermanent) {
      cleanupTempTables(conn, tempEmulationSchema, restrictVisitTable, eventConceptsTable)
    }
  })
  
  if (!is.null(restrictVisitConceptIds)) {
    restrictVisitTable <- materializeVisitConcepts(
      conn, restrictVisitConceptIds, tableNameRoot, tempEmulationSchema, asPermanent
    )
  }
  
  if (!is.null(eventFilters)) {
    eventConceptsTable <- materializeEventFilters(
      conn, eventFilters, tableNameRoot, tempEmulationSchema, asPermanent
    )
  }
  
  # --- 3. Prepare SQL Parameters ---
  primaryFilterId <- 1L # Default
  if (microCosting && !is.null(eventFilters)) {
    if (!is.null(primaryEventFilterName)) {
      filterNames <- purrr::map_chr(eventFilters, "name")
      id <- which(filterNames == primaryEventFilterName)
      if (length(id) == 0) {
        rlang::abort(paste("primaryEventFilterName", primaryEventFilterName, "not found in eventFilters."))
      }
      primaryFilterId <- as.integer(id)
    }
  }
  
  resultsTable <- paste0(tableNameRoot, "_res")
  diagTable <- paste0(tableNameRoot, "_diag")
  
  params <- list(
    cdmSchema = cdmSchema,
    cohortSchema = cohortSchema,
    cohortTable = cohortTable,
    cohortId = cohortId,
    anchorCol = anchorCol,
    startOffsetDays = startOffsetDays,
    endOffsetDays = endOffsetDays,
    costConceptId = costConceptId,
    currencyConceptId = currencyConceptId,
    hasVisitRestriction = !is.null(restrictVisitConceptIds),
    restrictVisitTable = restrictVisitTable,
    hasEventFilters = !is.null(eventFilters),
    eventConceptsTable = eventConceptsTable,
    nFilters = if (is.null(eventFilters)) 0L else length(eventFilters),
    microCosting = microCosting,
    primaryFilterId = primaryFilterId,
    resultsTable = resultsTable,
    diagTable = diagTable
  )
  
  # --- 4. Render, Translate, Execute ---
  executeSqlPlan(
    conn = conn,
    params = params,
    targetDialect = targetDialect,
    tempEmulationSchema = tempEmulationSchema
  )
  
  withr::defer({
    if (!asPermanent) {
      cleanupTempTables(conn, tempEmulationSchema, resultsTable, diagTable)
    }
  })
  
  # --- 5. Shape Output ---
  results <- DatabaseConnector::querySql(conn, glue::glue("SELECT * FROM {resultsTable}")) |> tibble::as_tibble()
  diagnostics <- DatabaseConnector::querySql(conn, glue::glue("SELECT * FROM {diagTable}")) |> tibble::as_tibble()
  
  if (returnFormat == "tibble") {
    return(results)
  } else {
    return(list(
      results = results,
      diagnostics = diagnostics
    ))
  }
}