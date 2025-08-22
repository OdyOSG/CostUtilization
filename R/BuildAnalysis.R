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
executeSqlPlan <- function(connection,
                           params,
                           targetDialect,
                           tempEmulationSchema,
                           verbose = TRUE) {
  logMessage("Starting SQL plan execution", verbose, "INFO")
  
  # Read SQL template
  sql <- system.file("sql", "MainCostUtilization.sql",
                     package = "CostUtilization", mustWork = TRUE) |>
    SqlRender::readSql()
  
  # Prepare parameters for SQL rendering (robust to naming styles)
  renderParams <- prepareSqlRenderParams(params, tempEmulationSchema)
  
  logMessage(sprintf("Translating SQL to %s dialect", targetDialect), verbose, "DEBUG")
  
  # Render SQL with parameters (all flags are 0/1 ints)
  sql <- SqlRender::render(
    sql = sql,
    cdm_database_schema    = renderParams$cdm_database_schema,
    cohort_database_schema = renderParams$cohort_database_schema,
    cohort_table           = renderParams$cohort_table,
    cohort_id              = renderParams$cohort_id,
    anchor_on_end          = renderParams$anchor_on_end,
    time_a                 = renderParams$time_a,
    time_b                 = renderParams$time_b,
    cost_concept_id        = renderParams$cost_concept_id,
    currency_concept_id    = renderParams$currency_concept_id,
    n_filters              = renderParams$n_filters,
    has_visit_restriction  = renderParams$has_visit_restriction,
    has_event_filters      = renderParams$has_event_filters,
    micro_costing          = renderParams$micro_costing,
    primary_filter_id      = renderParams$primary_filter_id,
    results_table          = renderParams$results_table,
    diag_table             = renderParams$diag_table,
    restrict_visit_table   = renderParams$restrict_visit_table,
    event_concepts_table   = renderParams$event_concepts_table,
    cpi_adjustment         = renderParams$cpi_adjustment,
    cpi_adj_table          = renderParams$cpi_adj_table
  )
  
  # Translate to target dialect
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = targetDialect,
    tempEmulationSchema = tempEmulationSchema
  )
  
  # Split and execute
  sqlStatements <- SqlRender::splitSql(sql)
  logMessage(sprintf("Executing %d SQL statements", length(sqlStatements)), verbose, "INFO")
  
  executeSqlStatements(connection = connection,
                       sqlStatements = sqlStatements,
                       verbose = verbose)
  
  logMessage("SQL plan execution completed", verbose, "INFO")
  invisible(NULL)
}

# small internal helper
.to_int_flag <- function(x) as.integer(isTRUE(x))

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
prepareSqlRenderParams <- function(params, tempEmulationSchema) {
  # Helper to get by snake_case first, then camelCase fallback
  g <- function(snake, camel = NULL, default = NULL) {
    if (!is.null(params[[snake]])) return(params[[snake]])
    if (!is.null(camel) && !is.null(params[[camel]])) return(params[[camel]])
    default
  }
  
  # Core schema/table/id
  cdm_database_schema    <- g("cdm_database_schema",    "cdmDatabaseSchema")
  cohort_database_schema <- g("cohort_database_schema", "cohortDatabaseSchema")
  cohort_table           <- g("cohort_table",           "cohortTable")
  cohort_id              <- as.integer(g("cohort_id",   "cohortId"))
  
  # Window & anchor (prefer precomputed anchor_on_end, else derive from anchorCol)
  anchor_on_end <- g("anchor_on_end", default = {
    anchorCol <- g("anchor_col", "anchorCol", default = "cohort_start_date")
    .to_int_flag(identical(anchorCol, "cohort_end_date"))
  })
  time_a <- as.integer(g("time_a", "startOffsetDays", default = 0L))
  time_b <- as.integer(g("time_b", "endOffsetDays",   default = 365L))
  
  # Costing params
  cost_concept_id     <- as.integer(g("cost_concept_id",     "costConceptId"))
  currency_concept_id <- as.integer(g("currency_concept_id", "currencyConceptId"))
  n_filters           <- as.integer(g("n_filters",           "nFilters", default = 0L))
  
  # Flags (0/1 ints)
  has_visit_restriction <- .to_int_flag(g("has_visit_restriction", "hasVisitRestriction", default = FALSE))
  has_event_filters     <- .to_int_flag(g("has_event_filters",     "hasEventFilters",     default = FALSE))
  micro_costing         <- .to_int_flag(g("micro_costing",         "microCosting",        default = FALSE))
  cpi_adjustment        <- .to_int_flag(g("cpi_adjustment",        "cpiAdjustment",       default = FALSE))
  
  # Primary filter id (index in eventFilters) if provided; else 0
  primary_filter_id <- as.integer(g("primary_filter_id", default = {
    pefn <- g("primary_event_filter_name", "primaryEventFilterName")
    efs  <- g("event_filters", "eventFilters")
    if (!is.null(pefn) && !is.null(efs)) {
      names <- vapply(efs, function(f) f$name, character(1))
      idx <- which(names == pefn)
      if (length(idx) == 1L) idx else 0L
    } else 0L
  }))
  
  # Temp/helper table names (qualify if tempEmulationSchema is used)
  results_table         <- qualifyTableName(g("results_table",         "resultsTable"),        tempEmulationSchema)
  diag_table            <- qualifyTableName(g("diag_table",            "diagTable"),           tempEmulationSchema)
  restrict_visit_table  <- qualifyTableName(g("restrict_visit_table",  "restrictVisitTable"),  tempEmulationSchema)
  event_concepts_table  <- qualifyTableName(g("event_concepts_table",  "eventConceptsTable"),  tempEmulationSchema)
  cpi_adj_table         <- qualifyTableName(g("cpi_adj_table",         "cpiAdjTable"),         tempEmulationSchema)
  
  list(
    cdm_database_schema    = cdm_database_schema,
    cohort_database_schema = cohort_database_schema,
    cohort_table           = cohort_table,
    cohort_id              = cohort_id,
    anchor_on_end          = as.integer(anchor_on_end),
    time_a                 = time_a,
    time_b                 = time_b,
    cost_concept_id        = cost_concept_id,
    currency_concept_id    = currency_concept_id,
    n_filters              = n_filters,
    has_visit_restriction  = has_visit_restriction,
    has_event_filters      = has_event_filters,
    micro_costing          = micro_costing,
    primary_filter_id      = primary_filter_id,
    results_table          = results_table,
    diag_table             = diag_table,
    restrict_visit_table   = restrict_visit_table,
    event_concepts_table   = event_concepts_table,
    cpi_adjustment         = cpi_adjustment,
    cpi_adj_table          = cpi_adj_table
  )
}

#' Qualify Table Name with Schema
#'
#' @description
#' Prepend schema name to table name when provided (used for temp emulation).
#'
#' @param tableName Table name (string).
#' @param schema Schema name (string or NULL).
#'
#' @return "schema.table" or the original table name if no schema.
#' @noRd
qualifyTableName <- function(tableName, schema) {
  if (!is.null(schema) && !is.null(tableName) && nzchar(schema) && nzchar(tableName)) {
    paste(schema, tableName, sep = ".")
  } else {
    tableName
  }
}
