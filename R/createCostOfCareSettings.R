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

#' Create Cost of Care Settings
#'
#' @description
#' Create a validated settings object for the `calculateCostOfCare` analysis.
#' This is the recommended way to specify analysis parameters.
#'
#' @param window                   A list containing the time window specifications:
#'                                 - startWith: Event to use as window start (e.g., 'start')
#'                                 - startOffset: Days to add/subtract from startWith date
#'                                 - endWith: Event to use as window end (e.g., 'end')
#'                                 - endOffset: Days to add/subtract from endWith date
#' @param restrictVisitConceptIds Optional integer vector of visit concept IDs to restrict analysis.
#'   If provided, only visits with these concept IDs are considered.
#' @param eventFilters Optional list of event filters (see Details). Each filter defines a set of
#'   concepts by OMOP domain; if provided, they can be used to constrain qualifying events/visits.
#' @param microCosting Logical; if `TRUE`, performs line-level costing at the `visit_detail` level.
#'   Requires `eventFilters`. Default: `FALSE`.
#' @param costConceptId Integer; concept ID for the cost type. Default: `31973` (charged).
#' @param currencyConceptId Integer; concept ID for the currency. Default: `44818668` (USD).
#' @param additionalCostConceptIds Optional integer vector of additional cost concept IDs to include.
#'   These can be used by downstream SQL to widen the cost types considered.
#' @param cpiAdjustment Logical; if `TRUE`, adjust costs using CPI factors from `cpiFilePath`. Default: `FALSE`.
#' @param cpiFilePath Optional character path to a CPI adjustment table/file. Required if `cpiAdjustment = TRUE`.
#'
#' @details
#' **Event filters structure**
#'
#' The `eventFilters` argument must be a list of lists, where each inner list has:
#' \itemize{
#'   \item `name`: A unique character string for the filter.
#'   \item `conceptSet`: A Circe concept set list defining the concepts to include.
#' }
#'
#' **CPI adjustment**
#'
#' If `cpiAdjustment = TRUE`, you must provide `cpiFilePath`. Your pipeline should read this
#' into a table that exposes, at minimum, a `year` and an `adj_factor` column; downstream SQL
#' joins on the `year` extracted from cost dates. The function only validates the file path;
#' loading/attaching the table is left to the calling workflow.
#'
#' @return A `covariateSettings` object (list with class) containing analysis specifications.
#'
#' @export
createCostOfCareSettings <- function(
    window = list(
      startWith = "start",
      startOffset = 0,
      endWith = "end",
      endOffset = 0
    ),
    restrictVisitConceptIds = NULL,
    eventFilters = NULL,
    microCosting = FALSE,
    costConceptId = 31973,
    currencyConceptId = 44818668,
    additionalCostConceptIds = NULL,
    cpiAdjustment = FALSE,
    cpiFilePath = NULL) {
  # --- Input Validation with checkmate ---
  errorMessages <- checkmate::makeAssertCollection()

  # 1. Window validation (Structure check)
  checkmate::assertList(window, len = 4, names = "strict", add = errorMessages)
  if (checkmate::testList(window, len = 4, names = "strict")) {
    valid_anchors <- c("start", "end") # Assuming start/end were placeholders
    checkmate::assertChoice(window$startWith, choices = valid_anchors, add = errorMessages)
    checkmate::assertChoice(window$endWith, choices = valid_anchors, add = errorMessages)

    checkmate::assertIntegerish(window$startOffset, len = 1, any.missing = FALSE, add = errorMessages)
    checkmate::assertIntegerish(window$endOffset, len = 1, any.missing = FALSE, add = errorMessages)
  }

  # 2. Costs/Currency validation
  checkmate::assertIntegerish(costConceptId, len = 1, lower = 1, any.missing = FALSE, add = errorMessages)
  checkmate::assertIntegerish(currencyConceptId, len = 1, lower = 1, any.missing = FALSE, add = errorMessages)

  # 3. Optional additional cost concepts
  if (!is.null(additionalCostConceptIds)) {
    checkmate::assertIntegerish(additionalCostConceptIds, lower = 1, any.missing = FALSE, unique = TRUE, add = errorMessages)
  }

  # 4. Flags
  checkmate::assertFlag(microCosting, add = errorMessages)
  checkmate::assertFlag(cpiAdjustment, add = errorMessages)

  # 5. Visit restrictions
  if (!is.null(restrictVisitConceptIds)) {
    checkmate::assertIntegerish(restrictVisitConceptIds, lower = 1, min.len = 1, unique = TRUE, any.missing = FALSE, add = errorMessages)
  }

  # 6. Event filters (structural check before detailed validation)
  if (!is.null(eventFilters)) {
    checkmate::assertList(eventFilters, add = errorMessages, min.len = 1)
  }

  # Collect assertion failures (so far)
  checkmate::reportAssertions(errorMessages)

  # --- Detailed/Conditional Validation (Base R Error Handling) ---

  # 7. Event filters detailed validation
  if (!is.null(eventFilters)) {
    validateEventFilters(eventFilters)
    nFilters <- length(eventFilters)
    message(sprintf("Configured %d event filter(s) for analysis.", nFilters))
  } else {
    nFilters <- 0L
  }

  # 8. Micro-costing constraints
  if (isTRUE(microCosting) && is.null(eventFilters)) {
    stop(paste(
      "Micro-costing requires event filters. eventFilters is NULL but microCosting is TRUE.",
      "Define at least one event filter for micro-costing analysis."
    ))
  }

  # 9. CPI constraints
  if (isTRUE(cpiAdjustment)) {
    checkmate::assertCharacter(cpiFilePath, len = 1, any.missing = FALSE, min.chars = 1, add = errorMessages)
    checkmate::reportAssertions(errorMessages) # Re-report if path is invalid type

    if (!file.exists(cpiFilePath)) {
      stop(sprintf(
        "CPI file not found. File does not exist: '%s'. Provide a valid path to CPI adjustment data.",
        cpiFilePath
      ))
    }
  }

  # Helpful notice for visit restrictions
  if (!is.null(restrictVisitConceptIds)) {
    message(sprintf("Analysis will be restricted to %d visit concept(s).", length(restrictVisitConceptIds)))
  }

  # --- Create Settings Object ---
  settings <- structure(
    list(
      window = window,
      hasVisitRestriction = !is.null(restrictVisitConceptIds),
      restrictVisitConceptIds = if (is.null(restrictVisitConceptIds)) NULL else as.integer(restrictVisitConceptIds),
      hasEventFilters = !is.null(eventFilters),
      eventFilters = eventFilters,
      nFilters = as.integer(nFilters),
      microCosting = microCosting,
      costConceptId = as.integer(costConceptId),
      currencyConceptId = as.integer(currencyConceptId),
      additionalCostConceptIds = if (is.null(additionalCostConceptIds)) NULL else as.integer(additionalCostConceptIds),
      cpiAdjustment = cpiAdjustment,
      cpiFilePath = cpiFilePath
    ),
    class = "CostOfCareSettings"
  )

  return(settings)
}

#' Validate Event Filters
#'
#' @description
#' Internal function to validate the structure of event filters.
#'
#' @param eventFilters List of event filter specifications
#' @noRd
validateEventFilters <- function(eventFilters) {
  if (!is.list(eventFilters)) {
    stop(sprintf("Invalid event filters format. Expected a list, received class: %s", class(eventFilters)[1]))
  }

  validDomains <- c(
    "Drug", "Condition", "Procedure", "Observation",
    "Measurement", "Device",  "All"
  )

  filter_names <- character(length(eventFilters))

  # Iterate through filters using Base R loop
  for (i in seq_along(eventFilters)) {
    filter <- eventFilters[[i]]
    filter_label <- paste("Filter at index", i)

    if (!is.list(filter)) {
      stop(sprintf("Invalid event filter structure: %s is not a list.", filter_label))
    }

    # Check required fields (Updated to conceptSet)
    requiredFields <- c("name", "domain", "conceptSet")
    missingFields <- setdiff(requiredFields, names(filter))

    if (length(missingFields) > 0) {
      stop(sprintf(
        "Missing required fields in %s. Missing: %s. Required fields are: %s",
        filter_label,
        paste(missingFields, collapse = ", "),
        paste(requiredFields, collapse = ", ")
      ))
    }

    # Validate name
    if (!is.character(filter$name) || length(filter$name) != 1 || nchar(filter$name) == 0) {
      stop(sprintf("Invalid filter name in %s. Name must be a non-empty character string.", filter_label))
    }
    filter_names[i] <- filter$name

    # Validate domain
    if (!filter$domain %in% validDomains) {
      stop(sprintf(
        "Invalid domain in filter '%s'. '%s' is not a valid OMOP domain. Valid domains: %s",
        filter$name,
        filter$domain,
        paste(validDomains, collapse = ", ")
      ))
    }

    # Validate conceptSet (Must be a list structure, typical for Circe)
    if (!is.list(filter$conceptSet)) {
      stop(sprintf(
        "Invalid 'conceptSet' in filter '%s'. 'conceptSet' must be a list (Circe concept set structure). Received class: %s",
        filter$name,
        class(filter$conceptSet)[1]
      ))
    }
  }

  # Check for duplicate names
  duplicates <- filter_names[duplicated(filter_names)]
  if (length(duplicates) > 0) {
    stop(sprintf(
      "Duplicate filter names detected: %s. Each event filter must have a unique name.",
      paste(unique(duplicates), collapse = ", ")
    ))
  }

  invisible(TRUE)
}
