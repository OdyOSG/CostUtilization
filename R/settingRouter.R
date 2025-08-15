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
    if (nrow(df) == 0) {
      return(NULL)
    }

    queries <- list()
    direct_ids <- df |>
      dplyr::filter(!.data$includeDescendants) |>
      dplyr::pull(.data$conceptId)
    if (length(direct_ids) > 0) {
      queries$direct <- .sql_query_concepts(direct_ids)
    }

    desc_ids <- df |>
      dplyr::filter(.data$includeDescendants) |>
      dplyr::pull(.data$conceptId)
    if (length(desc_ids) > 0) {
      queries$descendants <- .sql_query_descendants(desc_ids)
    }

    # Return NULL if no queries were generated to avoid empty strings
    if (length(queries) == 0) {
      return(NULL)
    }
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
    if (nrow(df) == 0) {
      return(NULL)
    }
    # For exclusion, we only need the concept IDs, not their domains
    query <- .build_part_query(df)
    if (is.null(query)) {
      return(NULL)
    }
    glue::glue("SELECT concept_id FROM ({query}) exclusion_concepts")
  }
  exclude_df <- conceptSetDefinition |> dplyr::filter(.data$isExcluded)
  full_exclude_query <- .build_exclude_id_query(exclude_df)


  # 3. Assemble the final SQL statement
  finalSql <- glue::glue(
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
    finalSql <- glue::glue(
      "{finalSql}
      WHERE included.concept_id NOT IN (
        -- Subtract all concept IDs to exclude
        {full_exclude_query}
      )"
    )
  }
  finalSql <- paste0(finalSql, ";")
  return(finalSql)
}

#' Build the Event Union Spec Based on Settings
#'
#' @description
#' This function acts as a router, dynamically constructing a SQL query that
#' unions together event tables from different domains based on the user's
#' settings.
#'
#' @param settings A `costUtilizationSettings` object.
#'
#' @param relevantDomains An optional character vector of domain IDs. If provided
#'                        (typically from a pre-query on a concept set)
#'
#' @return A character string containing a complete, executable SQL statement or vector of concept ids
#'
#' @noRd
buildEventUnionSpec <- function(settings, relevantDomains = NULL) {
  metaData <- cdmMetadata()
  tables2Query <- NULL
  # --- Determine which tables to query ---

  # Case 1: Relevant domains were pre-queried from a concept set.
  if (!is.null(relevantDomains)) {
    tables2Query <- metaData |>
      dplyr::filter(tolower(.data$domain_id) %in% tolower(relevantDomains))
    allUnions <- purrr::pmap_chr(tables2Query, ~ glue::glue("SELECT
          t.person_id,t.{..4} AS event_id,
          t.{..5} AS event_date, {..3}' AS domain_id
        FROM @cdm_database_schema.{..2} t
        INNER JOIN #final_codesets cs ON t.{..6} = cs.concept_id
        ")) |> paste(collapse = "\nUNION ALL\n")
    return(allUnions)
    # Case 2: A list of cost domains is provided directly in settings.
  } else if (!is.null(settings$costDomains)) {
    fieldConceptIds <- metaData |>
      dplyr::filter(tolower(.data$domain_id) %in% tolower(settings$costDomains)) |> 
      dplyr::pull(.data$domain_concept_id)
    return(fieldConceptIds)
  } else rlang::abort('Double-check settings: no relevant domains and no cost domains found')

}


#' Build the Full Analysis SQL Script
#'
#' @description
#' This function assembles the final, complete SQL script by combining the
#' concept set resolution SQL (if any) with the event union SQL.
#'
#' @param settings A `costUtilizationSettings` object.
#' @param relevantDomains An optional character vector of pre-queried domains.
#'
#' @return A single character string containing the complete SQL script.
#' @noRd
buildAnalysisSql <- function(settings, relevantDomains = NULL, finalCodesetTable) {
  # Step 2: Get the SQL for the UNION ALL block, now optimized
  eventUnionSql <- buildEventUnionSpec(settings, relevantDomains)

  # Step 3: Assemble the final script
  finalSql <- glue::glue("
    DROP TABLE IF EXISTS #events_of_interest;
    SELECT
      d.person_id,
      d.event_id,
      d.event_date,
      d.domain_id
    INTO {finalCodesetTable}
    FROM (
      {eventUnionSql}
    ) d;

    -- (The rest of the analysis SQL would follow here...)
    ")

  return(finalSql)
}



conceptSetRoute <- function(costUtilizationSettings, connection, cdmDatabaseSchema) {
  cli::cli_alert_info("Concept set provided. Pre-querying for relevant domains to optimize SQL.")
  finalCodesetTable <- SqlRender::translate("#final_codesets", connection@dbms)
  tempSql <- purrr::compose(.buildConceptSetSql, \(x) gsub("#final_codesets", finalCodesetTable, x),
                            .dir = "forward"
  )(costUtilizationSettings$conceptSetDefinition)
  relevantDomains <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql = paste0(tempSql, glue::glue("select distinct domain_id from {finalCodesetTable}")),
    cdm_database_schema = cdmDatabaseSchema
  ) |> dplyr::pull(.data$DOMAIN_ID)
  cli::cli_alert_success("Found relevant domains: {paste(relevantDomains, collapse = ', ')}")
  prefixSql <-buildAnalysisSql(costUtilizationSettings, relevantDomains = relevantDomains, finalCodesetTable)
     
  return(
    sql = prefixSql,
    finalCodeset = finalCodesetTable
  )
}

