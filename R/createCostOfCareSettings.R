#' Create Cost of Care Settings
#'
#' @description
#' Creates a validated settings object for the `calculateCostOfCare` analysis. This
#' is the recommended way to specify analysis parameters.
#'
#' @param anchorCol Column to use as anchor for time window ("cohort_start_date" or "cohort_end_date").
#' @param startOffsetDays Integer, days to add to anchor date for window start (can be negative).
#' @param endOffsetDays Integer, days to add to anchor date for window end.
#' @param restrictVisitConceptIds Optional vector of visit concept IDs to restrict analysis.
#' @param eventFilters Optional list of event filters. See details for structure.
#' @param microCosting Logical; if TRUE, performs line-level costing at the visit_detail level.
#' @param primaryEventFilterName For micro-costing, the name of the primary event filter that identifies the line-level events to cost.
#' @param costConceptId Concept ID for the cost type (e.g., total charge). Default: 31978.
#' @param currencyConceptId Concept ID for the currency. Default: 44818668 (USD).
#'
#' @details
#' The `eventFilters` argument must be a list of lists, where each inner list has:
#' \itemize{
#'   \item `name`: A unique character string for the filter.
#'   \item `domain`: The OMOP domain (e.g., "Drug", "Procedure").
#'   \item `conceptIds`: A vector of integer concept IDs.
#' }
#'
#' @return A `CostOfCareSettings` object.
#' @export
createCostOfCareSettings <- function(
    anchorCol = c("cohort_start_date", "cohort_end_date"),
    startOffsetDays = 0L,
    endOffsetDays = 365L,
    restrictVisitConceptIds = NULL,
    eventFilters = NULL,
    microCosting = FALSE,
    primaryEventFilterName = NULL,
    costConceptId = 31978L, # OMOP 'total charge'
    currencyConceptId = 44818668L # OMOP 'USD'
) {
  # --- Input Validation ---
  anchorCol <- rlang::arg_match(anchorCol)
  checkmate::assertIntegerish(startOffsetDays, len = 1)
  checkmate::assertIntegerish(endOffsetDays, len = 1)
  if (endOffsetDays <= startOffsetDays) {
    cli::cli_abort("{.arg endOffsetDays} must be greater than {.arg startOffsetDays}.")
  }
  
  checkmate::assertFlag(microCosting)
  if (!is.null(restrictVisitConceptIds)) {
    checkmate::assertIntegerish(restrictVisitConceptIds, lower = 1, min.len = 1, unique = TRUE)
  }
  
  if (!is.null(eventFilters)) {
    validateEventFilters(eventFilters) # Assumes validator from validators.R
  }
  
  if (microCosting) {
    checkmate::assertString(primaryEventFilterName, null.ok = FALSE)
    if (is.null(eventFilters) || !primaryEventFilterName %in% purrr::map_chr(eventFilters, "name")) {
      cli::cli_abort("When {.code microCosting = TRUE}, {.arg primaryEventFilterName} must match a name in {.arg eventFilters}.")
    }
  }
  
  checkmate::assertInt(costConceptId)
  checkmate::assertInt(currencyConceptId)
  
  # --- Create Settings Object ---
  settings <- list(
    anchorCol = anchorCol,
    startOffsetDays = startOffsetDays,
    endOffsetDays = endOffsetDays,
    hasVisitRestriction = !is.null(restrictVisitConceptIds),
    restrictVisitConceptIds = restrictVisitConceptIds,
    hasEventFilters = !is.null(eventFilters),
    eventFilters = eventFilters,
    nFilters = if (is.null(eventFilters)) 0 else length(eventFilters),
    microCosting = microCosting,
    primaryEventFilterName = primaryEventFilterName,
    costConceptId = costConceptId,
    currencyConceptId = currencyConceptId
  )
  
  class(settings) <- c("CostOfCareSettings", "list")
  return(settings)
}





sqlParams <- list(
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  cohortId = cohortId,
  anchorCol = anchorCol,
  start_odffset_days = startOffsetDays,
  end_odffset_days = endOffsetDays,
  cost_concept_id = costConceptId,
  currency_concep_id = currencyConceptId,
  restrict_visit_concept_ids = restrictVisitConceptIds,
  eventFilters = eventFilters,
  micro_costing = microCosting,
  primary_filter_id = primaryFilterId
)