#' Build SQL for Resolving a Concept Set with Domain Information
#'
#' @description
#' This function generates the SQL code required to resolve a concept set definition
#' into a final set of concept IDs and their corresponding domain IDs, creating a
#' temporary table named `#final_codesets`.
#'
#' @param conceptSetDefinition A tibble created by `.parseConceptSet()`. Must have
#'                             columns: `conceptId`, `isExcluded`, `includeDescendants`,
#'                             `includeMapped`.
#'
#' @return A single character string containing the complete, executable SQL
#'         statement to create and populate `#final_codesets`. Returns an empty
#'         string if the input is NULL.
#'
#' @noRd
.buildConceptSetSql <- function(conceptSetDefinition) {
  if (is.null(conceptSetDefinition) || nrow(conceptSetDefinition) == 0) {
    return("")
  }
  
  # --- Internal SQL Snippet Helpers ---
  # These now select both concept_id and domain_id.
  .sql_query_concepts <- function(ids) {
    glue::glue("SELECT concept_id, domain_id FROM @cdm_database_schema.CONCEPT WHERE concept_id IN ({paste(ids, collapse = ',')})")
  }
  
  .sql_query_descendants <- function(ids) {
    # Must join back to CONCEPT to get the domain_id for the descendants
    glue::glue("
    SELECT c.concept_id, c.domain_id
    FROM @cdm_database_schema.CONCEPT c
    JOIN @cdm_database_schema.CONCEPT_ANCESTOR ca ON c.concept_id = ca.descendant_concept_id
    WHERE ca.ancestor_concept_id IN ({paste(ids, collapse = ',')})")
  }
  
  # --- Internal Query Builder ---
  # This helper builds a query part that returns both concept_id and domain_id
  .build_part_query <- function(df) {
    if (nrow(df) == 0) return(NULL)
    
    queries <- list()
    direct_ids <- df |> dplyr::filter(!.data$includeDescendants) |> dplyr::pull(.data$conceptId)
    if (length(direct_ids) > 0) {
      queries$direct <- .sql_query_concepts(direct_ids)
    }
    
    desc_ids <- df |> dplyr::filter(.data$includeDescendants) |> dplyr::pull(.data$conceptId)
    if (length(desc_ids) > 0) {
      queries$descendants <- .sql_query_descendants(desc_ids)
    }
    
    # Return NULL if no queries were generated to avoid empty strings
    if (length(queries) == 0) return(NULL)
    paste(queries, collapse = "\nUNION\n")
  }
  
  
  # --- Main Logic ---
  
  # 1. Build the full INCLUDE query
  include_df <- conceptSetDefinition |> dplyr::filter(!.data$isExcluded)
  include_queries <- list()
  
  base_include_query <- .build_part_query(include_df)
  if (!is.null(base_include_query)) {
    include_queries$base <- base_include_query
  }

  full_include_query <- paste(include_queries, collapse = "\nUNION\n")
  
  
  # 2. Build the EXCLUDE query (only need concept_id for exclusion)
  .build_exclude_id_query <- function(df) {
    if (nrow(df) == 0) return(NULL)
    # For exclusion, we only need the concept IDs, not their domains
    query <- .build_part_query(df)
    if (is.null(query)) return(NULL)
    glue::glue("SELECT concept_id FROM ({query}) exclusion_concepts")
  }
  exclude_df <- conceptSetDefinition |> dplyr::filter(.data$isExcluded)
  full_exclude_query <- .build_exclude_id_query(exclude_df)
  
  
  # 3. Assemble the final SQL statement
  final_sql <- glue::glue(
    "
    -- Create and populate the final codeset table with domain_id
    DROP TABLE IF EXISTS #final_codesets;
    CREATE TABLE #final_codesets (
      concept_id BIGINT NOT NULL,
      domain_id VARCHAR(20) NOT NULL
    );

    INSERT INTO #final_codesets (concept_id, domain_id)
    SELECT concept_id, domain_id
    FROM (
      -- Start with all concepts to include
      {full_include_query}
    ) AS included
    ",
    .open = "{", .close = "}"
  )
  
  # Append a WHERE NOT IN clause for exclusion, which is safer than a multi-column EXCEPT
  if (!is.null(full_exclude_query)) {
    final_sql <- glue::glue(
      "{final_sql}
      WHERE included.concept_id NOT IN (
        -- Subtract all concept IDs to exclude
        {full_exclude_query}
      )"
    )
  }
  final_sql <- paste0(final_sql, ";")
  return(final_sql)
}

#' Build the Event Union SQL Based on Settings
#'
#' @description
#' This function acts as a router, dynamically constructing a SQL query that
#' unions together event tables from different domains based on the user's
#' settings. It handles filtering by domain or by a specific concept set.
#'
#' The output is a complete SQL statement that creates a temporary table
#' named `#events_of_interest`. This table will contain the universe of events
#' to which costs will be attached.
#'
#' @param settings A `costUtilizationSettings` object.
#'
#' @return A character string containing a complete, executable SQL statement.
#'
#' @noRd
buildEventUnionSql <- function(settings) {
  # Get the static mapping of OMOP domains and tables
  metaData <- cdmMetadata()

  
  if (!is.null(settings$conceptSetDefinition)) {
    concept_set_sql <- .buildConceptSetSql(settings$conceptSetDefinition)

    
    
    
  } else if (!is.null(settings$costDomains)) {

    tables2Query <- metaData |> dplyr::filter(tolower(.data$domain_id) %in% settings$costDomains)
    
    
    
  } else {
    cli::cli_alert_success("-- No domain or concept set filter provided. Costs will be joined directly.")
  }
  
  # Check if any domains are left to query
  if (nrow(tables2Query) == 0) {
    stop("The specified costDomains do not match any queryable domains in the metadata.", call. = FALSE)
  }
  union_statements <- purrr::pmap_chr(
    tables2Query, ~ glue::glue(
        "SELECT  t.person_id,
          t.{..4} AS event_id,
          t.{..5} AS event_date,
          t.{..6} AS event_concept_id,
          {..1} AS cost_event_field_concept_id
        FROM @cdm_database_schema.{..2} t
        ",
        .open = "{", .close = "}"
      )
  ) |> paste(collapse = "\nUNION ALL\n")


  final_sql <- glue::glue(
    "
    -- Step 1: Create concept set temp tables (if needed)
    {concept_set_sql$createdSql}

    -- Step 2: Union all relevant event tables into a single source
    DROP TABLE IF EXISTS #events_of_interest;

    SELECT
      d.person_id,
      d.event_id,
      d.event_date,
      d.domain_id
    INTO #events_of_interest
    FROM (
      {all_unions}
    ) d
    ",
    .open = "{", .close = "}"
  )
  
  return(final_sql)
}