#' Execute SQL Plan for Cost Analysis
#'
#' @description
#' Reads the SQL template, renders it with parameters, translates to the target
#' dialect, and executes the statements.
#'
#' @param connection A DatabaseConnector connection object.
#' @param params Named list of parameters for SQL rendering (can be camelCase or snake_case).
#' @param targetDialect The SQL dialect to translate to.
#' @param tempEmulationSchema Schema for temp table emulation (if needed).
#' @param verbose Whether to output progress messages.
#'
#' @return NULL (invisibly)
#' @noRd
# Execute SQL Plan for Cost Analysis (refactored render call; unchanged signature)
executeSqlPlan <- function(
    connection,
    params,
    targetDialect,
    tempEmulationSchema,
    verbose = TRUE) {
  logMessage("Starting SQL plan execution", verbose, "INFO")

  sql <- system.file("sql", "MainCostUtilization.sql",
    package = "CostUtilization", mustWork = TRUE
  ) |>
    SqlRender::readSql()

  renderParams <- prepareSqlRenderParams(params, tempEmulationSchema)

  logMessage(sprintf("Translating SQL to %s dialect", targetDialect), verbose, "DEBUG")

  sqlStatements <- do.call(SqlRender::render, c(list(sql = sql), renderParams)) |> 
    SqlRender::translate(
      targetDialect = targetDialect,
      tempEmulationSchema = tempEmulationSchema
  ) |> 
    SqlRender::splitSql()
  
  logMessage(sprintf("Executing %d SQL statements", length(sqlStatements)), verbose, "INFO")

  executeSqlStatements(
    connection = connection,
    sqlStatements = sqlStatements,
    verbose = verbose
  )

  logMessage("SQL plan execution completed", verbose, "INFO")
  invisible(NULL)
}

#' Prepare SQL Render Parameters
#'
#' @description
#' Accepts either camelCase (legacy) or snake_case (new) inputs and returns a
#' single normalized list for SqlRender::render(). All boolean flags are 0/1.
#'
#' @param params List of parameters from the main function.
#' @param tempEmulationSchema Schema for temporary tables.
#'
#' @return Named list ready for SQL rendering.
#' @noRd
#'
prepareSqlRenderParams <- function(
    params, 
    tempEmulationSchema) {
  # derive hasEventFilters if not explicitly set (keeps backward compatibility)
  has_event_filters <- params$hasEventFilters %||% (as.integer(params$nFilters %||% 0L) > 0L)

  list(
    # schema/table/id
    cdm_database_schema    = params$cdmDatabaseSchema,
    cohort_database_schema = params$cohortDatabaseSchema,
    cohort_table           = params$cohortTable,
    cohort_id              = as.integer(params$cohortId),

    # window & anchor
    anchor_on_end          = .int_flag(params$anchorOnEnd),
    time_a                 = as.integer(params$timeA %||% 0L),
    time_b                 = as.integer(params$timeB %||% 365L),
    
    aggregated             = params$aggregated,

    # costing & filters
    cost_concept_id        = as.integer(params$costConceptId),
    currency_concept_id    = as.integer(params$currencyConceptId),
    n_filters              = as.integer(params$nFilters %||% 0L),
    has_visit_restriction  = .int_flag(params$hasVisitRestriction),
    has_event_filters      = .int_flag(has_event_filters),
    micro_costing          = params$microCosting,
    primary_filter_id      = as.integer(params$primaryFilterId %||% 0L),

    # helper tables (qualified only here)
    results_table          = .qualify(params$resultsTable, tempEmulationSchema),
    diag_table             = .qualify(params$diagTable, tempEmulationSchema),
    restrict_visit_table   = .qualify(params$restrictVisitTable, tempEmulationSchema),
    event_concepts_table   = .qualify(params$eventConceptsTable, tempEmulationSchema),

    # CPI
    cpi_adjustment         = .int_flag(params$cpiAdjustment),
    cpi_adj_table          = .qualify(params$cpiAdjTable, tempEmulationSchema)
  )
}


