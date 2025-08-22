#' Calculate Cost of Care Analysis
#'
#' @description
#' Performs cost-of-care analysis for a specified cohort using modern R practices
#' with rlang and purrr for robust error handling and functional programming.
#'
#' @param connection A `DatabaseConnector` connection object.
#' @param connectionDetails A `DatabaseConnector` connectionDetails object (alternative to connection).
#' @param cdmDatabaseSchema Schema name for CDM tables.
#' @param cohortDatabaseSchema Schema name for the cohort table.
#' @param cohortTable Name of the cohort table.
#' @param cohortId The cohort definition ID to analyze.
#' @param costOfCareSettings A settings object created by `createCostOfCareSettings()`.
#' @param tempEmulationSchema Schema for temporary tables (if needed).
#' @param verbose Print progress messages.
#'
#' @return A list containing two tibbles: `results` and `diagnostics`.
#' @export
#' 
#' @examples
#' \dontrun{
#' settings <- createCostOfCareSettings(
#'   anchorCol = "cohort_start_date",
#'   startOffsetDays = 0,
#'   endOffsetDays = 365
#' )
#' 
#' results <- calculateCostOfCare(
#'   connection = connection,
#'   cdmDatabaseSchema = "cdm",
#'   cohortDatabaseSchema = "results",
#'   cohortTable = "cohort",
#'   cohortId = 1,
#'   costOfCareSettings = settings
#' )
#' }
calculateCostOfCare <- function(
    connection = NULL,
    connectionDetails = NULL,
    cdmDatabaseSchema,
    cohortDatabaseSchema,
    cohortTable,
    cohortId,
    costOfCareSettings,
    tempEmulationSchema = NULL,
    verbose = TRUE
) {
  # Input validation using rlang and checkmate
  rlang::check_required(cdmDatabaseSchema)
  rlang::check_required(cohortDatabaseSchema)
  rlang::check_required(cohortTable)
  rlang::check_required(cohortId)
  rlang::check_required(costOfCareSettings)
  
  checkmate::assert_class(costOfCareSettings, "CostOfCareSettings")
  checkmate::assert_string(cdmDatabaseSchema)
  checkmate::assert_string(cohortDatabaseSchema)
  checkmate::assert_string(cohortTable)
  checkmate::assert_int(cohortId, lower = 1)
  checkmate::assert_string(tempEmulationSchema, null.ok = TRUE)
  checkmate::assert_flag(verbose)
  
  # Connection handling with proper error messages
  if (is.null(connectionDetails) && is.null(connection)) {
    rlang::abort("Must provide either connectionDetails or connection")
  }
  if (!is.null(connectionDetails) && !is.null(connection)) {
    rlang::abort("Provide either connectionDetails or connection, not both")
  }
  
  # Establish connection if needed
  if (!is.null(connectionDetails)) {
    checkmate::assert_class(connectionDetails, "ConnectionDetails")
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  } else {
    checkmate::assert_class(connection, "DatabaseConnectorJdbcConnection")
  }
  
  # Start analysis
  start_time <- Sys.time()
  target_dialect <- DatabaseConnector::dbms(connection)
  
  # Generate unique table names
  session_id <- generate_session_id()
  table_names <- generate_table_names(session_id)
  
  # Ensure cleanup on exit
  on.exit({
    cleanup_analysis_tables(
      connection = connection,
      schema = tempEmulationSchema,
      table_names = table_names,
      verbose = verbose
    )
  }, add = TRUE)
  
  # Upload helper tables if needed
  upload_helper_tables(
    connection = connection,
    settings = costOfCareSettings,
    table_names = table_names,
    tempEmulationSchema = tempEmulationSchema,
    verbose = verbose
  )
  
  # Prepare SQL parameters
  sql_params <- prepare_sql_parameters(
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = cohortId,
    costOfCareSettings = costOfCareSettings,
    table_names = table_names,
    tempEmulationSchema = tempEmulationSchema
  )
  
  # Execute SQL analysis
  execute_cost_analysis(
    connection = connection,
    params = sql_params,
    targetDialect = target_dialect,
    tempEmulationSchema = tempEmulationSchema,
    verbose = verbose
  )
  
  # Fetch results
  results <- fetch_analysis_results(
    connection = connection,
    table_names = table_names,
    tempEmulationSchema = tempEmulationSchema,
    verbose = verbose
  )
  
  # Log completion
  if (verbose) {
    elapsed_time <- round(difftime(Sys.time(), start_time, units = "secs"), 1)
    cli::cli_alert_success("Analysis complete in {elapsed_time}s")
  }
  
  return(results)
}

# Helper functions ----

#' Generate unique session ID
#' @noRd
generate_session_id <- function() {
  paste0("cu_", stringi::stri_rand_strings(1, 10, pattern = "[a-z]"))
}

#' Generate table names for analysis
#' @noRd
generate_table_names <- function(session_id) {
  list(
    results = paste0(session_id, "_results"),
    diagnostics = paste0(session_id, "_diag"),
    visit_restriction = paste0(session_id, "_visit_restr"),
    event_concepts = paste0(session_id, "_evt_concepts")
  )
}

#' Cleanup analysis tables
#' @noRd
cleanup_analysis_tables <- function(connection, schema, table_names, verbose) {
  if (verbose) {
    cli::cli_alert_info("Cleaning up temporary tables...")
  }
  
  purrr::walk(table_names, function(table_name) {
    if (!is.null(table_name)) {
      sql <- "DROP TABLE IF EXISTS @schema.@table;"
      tryCatch({
        DatabaseConnector::renderTranslateExecuteSql(
          connection,
          sql,
          schema = schema %||% "dbo",
          table = table_name
        )
      }, error = function(e) {
        rlang::warn(glue::glue("Failed to drop table {table_name}: {e$message}"))
      })
    }
  })
}

#' Upload helper tables for analysis
#' @noRd
upload_helper_tables <- function(connection, settings, table_names, 
                                tempEmulationSchema, verbose) {
  # Upload visit restriction table if needed
  if (isTRUE(settings$hasVisitRestriction)) {
    visit_concepts <- dplyr::tibble(
      visit_concept_id = settings$restrictVisitConceptIds
    )
    
    upload_table_safe(
      connection = connection,
      tableName = table_names$visit_restriction,
      data = visit_concepts,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      verbose = verbose,
      description = "visit concepts"
    )
  }
  
  # Upload event filter table if needed
  if (isTRUE(settings$hasEventFilters)) {
    event_concepts <- purrr::imap_dfr(
      settings$eventFilters, 
      ~ dplyr::tibble(
        filter_id = .y,
        filter_name = .x$name,
        domain_scope = .x$domain,
        concept_id = .x$conceptIds
      )
    )
    
    upload_table_safe(
      connection = connection,
      tableName = table_names$event_concepts,
      data = event_concepts,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      verbose = verbose,
      description = "event concepts"
    )
  }
}

#' Safely upload table with error handling
#' @noRd
upload_table_safe <- function(connection, tableName, data, tempTable,
                             tempEmulationSchema, verbose, description) {
  tryCatch({
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = tableName,
      data = data,
      tempTable = tempTable,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE
    )
    
    if (verbose) {
      cli::cli_alert_info("Uploaded {nrow(data)} {description} to #{tableName}")
    }
  }, error = function(e) {
    rlang::abort(
      glue::glue("Failed to upload {description}"),
      parent = e,
      tableName = tableName
    )
  })
}

#' Prepare SQL parameters
#' @noRd
prepare_sql_parameters <- function(cdmDatabaseSchema, cohortDatabaseSchema,
                                  cohortTable, cohortId, costOfCareSettings,
                                  table_names, tempEmulationSchema) {
  # Base parameters
  params <- list(
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = cohortId,
    resultsTable = table_names$results,
    diagTable = table_names$diagnostics,
    restrictVisitTable = table_names$visit_restriction,
    eventConceptsTable = table_names$event_concepts
  )
  
  # Add settings parameters
  params <- c(params, as.list(costOfCareSettings))
  
  # Qualify table names if using temp emulation schema
  if (!is.null(tempEmulationSchema)) {
    params$resultsTable <- paste(tempEmulationSchema, params$resultsTable, sep = ".")
    params$diagTable <- paste(tempEmulationSchema, params$diagTable, sep = ".")
    
    if (!is.null(params$restrictVisitTable)) {
      params$restrictVisitTable <- paste(tempEmulationSchema, params$restrictVisitTable, sep = ".")
    }
    
    if (!is.null(params$eventConceptsTable)) {
      params$eventConceptsTable <- paste(tempEmulationSchema, params$eventConceptsTable, sep = ".")
    }
  }
  
  return(params)
}

#' Execute cost analysis SQL
#' @noRd
execute_cost_analysis <- function(connection, params, targetDialect,
                                 tempEmulationSchema, verbose) {
  if (verbose) {
    cli::cli_alert_info("Starting SQL plan execution")
  }
  
  # Read SQL template
  sql_path <- system.file(
    "sql", 
    "MainCostUtilization_v55.sql", 
    package = "CostUtilization", 
    mustWork = TRUE
  )
  
  sql <- SqlRender::readSql(sql_path)
  
  # Prepare render parameters
  render_params <- prepare_render_parameters(params, tempEmulationSchema)
  
  # Render SQL with parameters
  if (verbose) {
    cli::cli_alert_info("Translating SQL to {targetDialect} dialect")
  }
  
  rendered_sql <- do.call(SqlRender::render, c(list(sql = sql), render_params))
  
  # Split and execute statements
  sql_statements <- SqlRender::splitSql(rendered_sql)
  
  if (verbose) {
    cli::cli_alert_info("Executing {length(sql_statements)} SQL statements")
  }
  
  purrr::walk(sql_statements, function(statement) {
    tryCatch({
      DatabaseConnector::executeSql(connection, statement)
    }, error = function(e) {
      rlang::abort(
        "SQL execution failed",
        parent = e,
        statement = substr(statement, 1, 200)
      )
    })
  })
  
  if (verbose) {
    cli::cli_alert_success("SQL plan execution completed")
  }
}

#' Prepare render parameters for SQL
#' @noRd
prepare_render_parameters <- function(params, tempEmulationSchema) {
  # Convert R parameter names to SQL parameter names
  sql_params <- list()
  
  # Direct mappings
  param_mappings <- list(
    cdm_database_schema = "cdmDatabaseSchema",
    cohort_database_schema = "cohortDatabaseSchema",
    cohort_table = "cohortTable",
    cohort_id = "cohortId",
    anchor_col = "anchorCol",
    time_a = "startOffsetDays",
    time_b = "endOffsetDays",
    cost_concept_id = "costConceptId",
    currency_concept_id = "currencyConceptId",
    n_filters = "nFilters",
    has_visit_restriction = "hasVisitRestriction",
    has_event_filters = "hasEventFilters",
    micro_costing = "microCosting",
    results_table = "resultsTable",
    diag_table = "diagTable",
    restrict_visit_table = "restrictVisitTable",
    event_concepts_table = "eventConceptsTable"
  )
  
  purrr::iwalk(param_mappings, function(r_name, sql_name) {
    if (!is.null(params[[r_name]])) {
      sql_params[[sql_name]] <- params[[r_name]]
    }
  })
  
  # Handle primary filter ID for micro-costing
  if (isTRUE(params$microCosting) && !is.null(params$primaryEventFilterName)) {
    filter_names <- purrr::map_chr(params$eventFilters, "name")
    primary_index <- which(filter_names == params$primaryEventFilterName)
    
    if (length(primary_index) == 1) {
      sql_params$primary_filter_id <- primary_index
    } else {
      rlang::abort(
        "Could not find unique primary event filter for micro-costing",
        primaryEventFilterName = params$primaryEventFilterName,
        found = length(primary_index)
      )
    }
  } else {
    sql_params$primary_filter_id <- 0
  }
  
  return(sql_params)
}

#' Fetch analysis results
#' @noRd
fetch_analysis_results <- function(connection, table_names, tempEmulationSchema, verbose) {
  if (verbose) {
    cli::cli_alert_info("Fetching results from database")
  }
  
  # Qualify table names
  results_table <- qualify_table_name(table_names$results, tempEmulationSchema)
  diag_table <- qualify_table_name(table_names$diagnostics, tempEmulationSchema)
  
  # Fetch results
  results <- fetch_table_safe(
    connection = connection,
    tableName = results_table,
    description = "results"
  )
  
  # Fetch diagnostics
  diagnostics <- fetch_table_safe(
    connection = connection,
    tableName = diag_table,
    description = "diagnostics",
    orderBy = "step_order"
  )
  
  return(list(
    results = results,
    diagnostics = diagnostics
  ))
}

#' Qualify table name with schema
#' @noRd
qualify_table_name <- function(tableName, schema) {
  if (!is.null(schema) && !is.null(tableName)) {
    paste(schema, tableName, sep = ".")
  } else {
    tableName
  }
}

#' Safely fetch table with error handling
#' @noRd
fetch_table_safe <- function(connection, tableName, description, orderBy = NULL) {
  sql <- glue::glue("SELECT * FROM {tableName}")
  
  if (!is.null(orderBy)) {
    sql <- glue::glue("{sql} ORDER BY {orderBy}")
  }
  
  tryCatch({
    DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE) |>
      dplyr::as_tibble()
  }, error = function(e) {
    rlang::abort(
      glue::glue("Failed to fetch {description}"),
      parent = e,
      tableName = tableName
    )
  })
}

# Null coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x