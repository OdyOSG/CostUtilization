executeSqlPlan <- function(
    connection,
    params,
    targetDialect,
    tempEmulationSchema = NULL,
    verbose = TRUE
) {
  logMessage("Starting SQL plan execution", verbose, "INFO")
  
  renderParams <- prepareSqlRenderParams(params, tempEmulationSchema)
  
  # Load the SQL file from the package (OHDSI-style layout)
  sqlPath <- system.file("sql", "sql_server", "MainCostUtilization.sql", package = "CostUtilization")
  if (!nzchar(sqlPath)) {
    sqlPath <- system.file("sql", "MainCostUtilization.sql", package = "CostUtilization")
  }
  if (!nzchar(sqlPath)) {
    stop("Cannot find 'MainCostUtilization.sql' in the CostUtilization package.", call. = FALSE)
  }
  sql <- SqlRender::readSql(sqlPath)
  
  logMessage(sprintf("Translating SQL to %s dialect", targetDialect), verbose, "DEBUG")
  
  # >>> KEY FIX: pass anchor_on_end; remove ancor_col <<<
  sql <- SqlRender::render(
    sql,
    cdm_database_schema     = renderParams$cdm_database_schema,
    cohort_database_schema  = renderParams$cohort_database_schema,
    cohort_table            = renderParams$cohort_table,
    cohort_id               = renderParams$cohort_id,
    anchor_on_end           = renderParams$anchor_on_end,     # <-- FIXED
    time_a                  = renderParams$time_a,
    time_b                  = renderParams$time_b,
    cost_concept_id         = renderParams$cost_concept_id,
    currency_concept_id     = renderParams$currency_concept_id,
    cost_type_concept_id    = renderParams$cost_type_concept_id,
    n_filters               = renderParams$n_filters,
    has_visit_restriction   = renderParams$has_visit_restriction,
    restrict_visit_table    = renderParams$restrict_visit_table,
    has_event_filters       = renderParams$has_event_filters,
    event_concepts_table    = renderParams$event_concepts_table,
    micro_costing           = renderParams$micro_costing,
    primary_filter_id       = renderParams$primary_filter_id,
    results_table           = renderParams$results_table,
    diag_table              = renderParams$diag_table,
    cpi_adjustment          = renderParams$cpi_adjustment,
    cpi_adj_table           = renderParams$cpi_adj_table
  )
  
  sql <- SqlRender::translate(
    sql                 = sql,
    targetDialect       = targetDialect,
    tempEmulationSchema = tempEmulationSchema
  )
  
  split <- SqlRender::splitSql(sql)
  sqlStatements <- if (is.list(split) && !is.null(split$sql)) split$sql else split
  
  logMessage(sprintf("Executing %d SQL statements", length(sqlStatements)), verbose, "INFO")
  
  executeSqlStatements(
    connection    = connection,
    sqlStatements = sqlStatements,
    verbose       = verbose
  )
  
  logMessage("SQL plan execution completed", verbose, "INFO")
  invisible(NULL)
}

executeSqlPlan <- function(
    connection,
    params,
    targetDialect,
    tempEmulationSchema = NULL,
    verbose = TRUE
) {
  logMessage("Starting SQL plan execution", verbose, "INFO")
  
  renderParams <- prepareSqlRenderParams(params, tempEmulationSchema)
  
  # Load the SQL file from the package (OHDSI-style layout)
  sqlPath <- system.file("sql", "sql_server", "MainCostUtilization.sql", package = "CostUtilization")
  if (!nzchar(sqlPath)) {
    sqlPath <- system.file("sql", "MainCostUtilization.sql", package = "CostUtilization")
  }
  if (!nzchar(sqlPath)) {
    stop("Cannot find 'MainCostUtilization.sql' in the CostUtilization package.", call. = FALSE)
  }
  sql <- SqlRender::readSql(sqlPath)
  
  logMessage(sprintf("Translating SQL to %s dialect", targetDialect), verbose, "DEBUG")
  
  # >>> KEY FIX: pass anchor_on_end; remove ancor_col <<<
  sql <- SqlRender::render(
    sql,
    cdm_database_schema     = renderParams$cdm_database_schema,
    cohort_database_schema  = renderParams$cohort_database_schema,
    cohort_table            = renderParams$cohort_table,
    cohort_id               = renderParams$cohort_id,
    anchor_on_end           = renderParams$anchor_on_end,     # <-- FIXED
    time_a                  = renderParams$time_a,
    time_b                  = renderParams$time_b,
    cost_concept_id         = renderParams$cost_concept_id,
    currency_concept_id     = renderParams$currency_concept_id,
    cost_type_concept_id    = renderParams$cost_type_concept_id,
    n_filters               = renderParams$n_filters,
    has_visit_restriction   = renderParams$has_visit_restriction,
    restrict_visit_table    = renderParams$restrict_visit_table,
    has_event_filters       = renderParams$has_event_filters,
    event_concepts_table    = renderParams$event_concepts_table,
    micro_costing           = renderParams$micro_costing,
    primary_filter_id       = renderParams$primary_filter_id,
    results_table           = renderParams$results_table,
    diag_table              = renderParams$diag_table,
    cpi_adjustment          = renderParams$cpi_adjustment,
    cpi_adj_table           = renderParams$cpi_adj_table
  )
  
  sql <- SqlRender::translate(
    sql                 = sql,
    targetDialect       = targetDialect,
    tempEmulationSchema = tempEmulationSchema
  )
  
  split <- SqlRender::splitSql(sql)
  sqlStatements <- if (is.list(split) && !is.null(split$sql)) split$sql else split
  
  logMessage(sprintf("Executing %d SQL statements", length(sqlStatements)), verbose, "INFO")
  
  executeSqlStatements(
    connection    = connection,
    sqlStatements = sqlStatements,
    verbose       = verbose
  )
  
  logMessage("SQL plan execution completed", verbose, "INFO")
  invisible(NULL)
}

prepareSqlRenderParams <- function(params, tempEmulationSchema) {
  sqlParams <- list()
  
  # --- Validate required parameters ---
  requiredParams <- c("cdmDatabaseSchema", "cohortDatabaseSchema", "cohortTable", "cohortId")
  for (param in requiredParams) {
    if (is.null(params[[param]]) || params[[param]] == "") {
      cli::cli_abort(c(
        "Required parameter '{param}' is missing or empty.",
        "i" = "Please provide a valid value for '{param}'."
      ))
    }
  }
  
  # --- Direct mapping from R camelCase to SQL snake_case ---
  sqlParams$cdm_database_schema <- params$cdmDatabaseSchema
  sqlParams$cohort_database_schema <- params$cohortDatabaseSchema
  sqlParams$cohort_table <- params$cohortTable
  sqlParams$cohort_id <- params$cohortId
  sqlParams$anchor_on_end <- ifelse(params$anchorCol == "cohort_end_date", 1, 0) # <-- FIX HERE
  sqlParams$time_a <- params$startOffsetDays
  sqlParams$time_b <- params$endOffsetDays
  
  # --- Cost-related parameters with defaults ---
  sqlParams$cost_concept_id <- if (!is.null(params$costConceptId) && params$costConceptId != "") {
    params$costConceptId
  } else {
    31978  # Default: Total paid
  }
  
  sqlParams$currency_concept_id <- if (!is.null(params$currencyConceptId) && params$currencyConceptId != "") {
    params$currencyConceptId
  } else {
    44818668 # Default: US Dollar
  }
  
  # --- FIX START ---
  # Handle the optional cost_type_concept_id filter
  sqlParams$cost_type_concept_id <- if (!is.null(params$costTypeConceptId) && params$costTypeConceptId != "") {
    params$costTypeConceptId
  } else {
    "NULL" # Pass NULL to disable the filter in SQL
  }
  # --- FIX END ---
  
  sqlParams$cost_event_field_concept_id <- if (!is.null(params$costEventFieldConceptId) && params$costEventFieldConceptId != "") {
    params$costEventFieldConceptId
  } else {
    1147332  # Default: cost_event_id
  }
  
  # --- Boolean flags for conditional SQL ---
  sqlParams$has_visit_restriction <- isTRUE(params$hasVisitRestriction)
  sqlParams$has_event_filters <- isTRUE(params$hasEventFilters)
  sqlParams$micro_costing <- isTRUE(params$microCosting)
  sqlParams$cpi_adjustment <- isTRUE(params$cpiAdjustment)
  
  # --- Derived parameters ---
  if (isTRUE(params$microCosting) && !is.null(params$primaryEventFilterName)) {
    filterNames <- purrr::map_chr(params$eventFilters, "name")
    primaryFilterIndex <- which(filterNames == params$primaryEventFilterName)
    if (length(primaryFilterIndex) == 1) {
      sqlParams$primary_filter_id <- primaryFilterIndex
    } else {
      cli::cli_abort(c(
        "Could not find a unique primary event filter for micro-costing.",
        "i" = "Expected one filter named '{params$primaryEventFilterName}' but found {length(primaryFilterIndex)}."
      ))
    }
  } else {
    sqlParams$primary_filter_id <- 0
  }
  
  # --- Schema-qualified table names ---
  qualifyTableName <- function(tableName, schema) {
    if (!is.null(schema) && !is.null(tableName)) {
      return(paste(schema, tableName, sep = "."))
    }
    return(tableName)
  }
  
  sqlParams$results_table <- qualifyTableName(params$resultsTable, tempEmulationSchema)
  sqlParams$diag_table <- qualifyTableName(params$diagTable, tempEmulationSchema)
  sqlParams$restrict_visit_table <- qualifyTableName(params$restrictVisitTable, tempEmulationSchema)
  sqlParams$event_concepts_table <- qualifyTableName(params$eventConceptsTable, tempEmulationSchema)
  # --- CPI Adjustment parameters (make the flag dependent on the table) ---
  sqlParams$cpi_adj_table <- params$cpiAdjTable
  sqlParams$cpi_adjustment <- isTRUE(params$cpiAdjustment) && !is.null(sqlParams$cpi_adj_table)
  
  
  return(sqlParams)
}
