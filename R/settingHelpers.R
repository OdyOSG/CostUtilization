#' Get Static Mapping of OMOP CDM Event Tables and Domains
#'
#' @description
#' This internal helper function provides a consolidated, static mapping that links
#' OMOP domain IDs to their corresponding event tables, standard column names,
#' and their `domain_concept_id` from the OMOP `DOMAIN` table. (using CDM Metadata)
#'
#' @details
#' This function serves as a single, hard-coded source of truth for the package's
#' knowledge of the CDM structure. It is designed to be self-contained, avoiding
#' the need for a live database query for this specific metadata.
#'
#' The column `domain_concept_id` corresponds to `domain.domain_concept_id` and
#' represents the concept ID for the table itself (e.g., the concept for 'procedure_occurrence').
#'
#' @return A tibble with a consolidated view of event table metadata.
#'
#' @noRd
cdmMetadata <- function() {
  dplyr::tibble(
    # Column names are updated for clarity and alignment with OMOP standards
    domain_concept_id = c(1147333, 1147305, 1147339, 1147330, 1147304, 1147301, 1147332),
    table_name = c("condition_occurrence", "device_exposure", "drug_exposure", "measurement", "observation", "procedure_occurrence", "visit_occurrence"),
    domain_id = c("Condition", "Device", "Drug", "Measurement", "Observation", "Procedure", "Visit"),
    event_id = c("condition_occurrence_id", "device_exposure_id", "drug_exposure_id", "measurement_id", "observation_id", "procedure_occurrence_id", "visit_occurrence_id"),
    event_date = c("condition_start_date", "device_exposure_start_date", "drug_exposure_start_date", "measurement_date", "observation_date", "procedure_date", "visit_start_date"),
    event_concept_id = c("condition_concept_id", "device_concept_id", "drug_concept_id", "measurement_concept_id", "observation_concept_id", "procedure_concept_id", "visit_concept_id")
  )
}


#' Normalize Settings and Resolve Conflicts
#'
#' @description
#' This internal helper function takes a list of settings, resolves any conflicts
#' according to predefined precedence rules, and informs the user of any
#' changes made.
#'
#' @param settings A list of settings, typically from the environment of the
#'                 `createCostUtilizationSettings` function.
#'
#' @return A modified list of settings with conflicts resolved.
#'
#' @noRd
.normalizeSettings <- function(settings) {
  # --- Conflict 1: useConceptSet vs. costDomains ---
  # Rule: If a concept set is provided, it defines the events of interest.
  # The broader `costDomains` filter becomes redundant and is ignored.
  if (!is.null(settings$useConceptSet) && !is.null(settings$costDomains)) {
    cli::cli_warn(c(
      "Both {.arg useConceptSet} and {.arg costDomains} were provided.",
      "i" = "Prioritizing {.arg useConceptSet} for fine-grained event selection.",
      "!" = "The {.arg costDomains} argument will be ignored if not align with Concept Set"
    ))
    settings$costDomains <- NULL
  }

  return(settings)
}

#' Parse a Concept Set Input
#'
#' @description
#' This internal helper function takes various forms of a concept set and converts
#' them into a standardized tibble that can be used for SQL generation.
#'
#' @param conceptSet The input to parse. Can be NULL, a numeric vector, a `Capr`
#'                   `ConceptSet` object, or a list representing a Circe/JSON
#'                   concept set expression.
#'
#' @return
#' A tibble with columns `conceptId`, `isExcluded`, `includeDescendants`, `includeMapped`,
#' or NULL if the input is NULL.
#'
#' @noRd
.parseConceptSet <- function(conceptSet) {
  if (is.null(conceptSet)) {
    return(NULL)
  }
  
  # Case 1: Input is a Capr S4 ConceptSet object.
  # This check must come before is.list() because Capr objects are also lists.
  if (inherits(conceptSet, "ConceptSet")) {
    return(
      dplyr::tibble(
        conceptId = purrr::map_int(conceptSet@Expression, c("Concept", "concept_id")),
        isExcluded = purrr::map_lgl(conceptSet@Expression, "isExcluded"),
        includeDescendants = purrr::map_lgl(conceptSet@Expression, "includeDescendants")
      )
    )
  }
  
  # Case 2: Input is a simple numeric vector of concept IDs.
  if (checkmate::test_integerish(conceptSet)) {
    # Defaulting includeDescendants to TRUE is the most common and expected
    # behavior for a simple vector of concepts in OHDSI tools.
    return(
      dplyr::tibble(
        conceptId = as.integer(conceptSet),
        isExcluded = FALSE,
        includeDescendants = FALSE
      )
    )
  }
  
  # Case 3: Input is a list (from JSON, representing a Circe expression).
  if (checkmate::test_list(conceptSet)) {
    # Detect if it's a full concept set definition or just the items list
    items <- purrr::pluck(conceptSet, 'items')
    return(
      purrr::map_dfr(items, ~ dplyr::tibble(
        conceptId = as.integer(.x$concept$CONCEPT_ID),
        isExcluded = as.logical(.x$isExcluded),
        includeDescendants = as.logical(.x$includeDescendants)
      ))
    )
  }
  
  # If none of the above, the format is not recognized.
  stop(
    "The 'useConceptSet' argument is not in a recognized format.\n",
    "It must be a numeric vector, a Capr ConceptSet object, or a list ",
    "representing a Circe concept set expression.",
    call. = FALSE
  )
}
