validateInputs <- function(params) {
  # Basic type checks
  rlang::check_installed("checkmate")
  checkmate::assertClass(params$connection, "DatabaseConnection")  # Not 'conn'
  checkmate::assertString(params$cdmSchema)
  checkmate::assertString(params$cohortSchema)
  checkmate::assertString(params$cohortTable)
  checkmate::assertInt(params$cohortId)
  checkmate::assertInt(params$startOffsetDays)
  checkmate::assertInt(params$endOffsetDays)
  checkmate::assertIntegerish(params$restrictVisitConceptIds, null.ok = TRUE)
  checkmate::assertList(params$eventFilters, null.ok = TRUE, types = "list")
  # ... more checks for each parameter

  validateInputs <- function(params) {

    if (!is.null(params$eventFilters)) {
      purrr::iwalk(params$eventFilters, ~ {
        checkmate::assertList(.x, names = "named")
        # Fix field names to match actual usage
        checkmate::assertNames(names(.x), must.include = c("name", "domain", "conceptIds"))
        checkmate::assertCharacter(.x$name, len = 1)
        checkmate::assertIntegerish(.x$conceptIds)  # Changed from 'concepts'
        checkmate::assertChoice(.x$domain, c("All", "Condition", "Procedure", "Drug", "Measurement", "Observation"))
      })
    }

  invisible(TRUE)
}}

checkMicrocostingPrerequisites <- function(conn, cdmSchema) {
  sql <- "SELECT TOP 1 visit_detail_id FROM @cdmSchema.visit_detail;"
  sql <- SqlRender::render(sql, cdmSchema = cdmSchema)
  sql <- SqlRender::translate(sql, targetDialect = DatabaseConnector::dbms(conn))
  hasVisitDetail <- try(DatabaseConnector::querySql(conn, sql), silent = TRUE)
  if (inherits(hasVisitDetail, "try-error")) {
    rlang::abort("microCosting=TRUE requires the cdm.visit_detail table, which was not found or is not accessible.")
  }
  invisible(TRUE)
}

# Validate event filters structure
validateEventFilters <- function(eventFilters) {
  if (!is.list(eventFilters)) {
    stop("eventFilters must be a list")
  }

  for (i in seq_along(eventFilters)) {
    filter <- eventFilters[[i]]

    if (!all(c("name", "domain", "conceptIds") %in% names(filter))) {
      stop(sprintf("Event filter %d missing required fields (name, domain, conceptIds)", i))
    }

    if (!filter$domain %in% c("All", "Drug", "Procedure", "Condition", "Measurement", "Observation")) {
      stop(sprintf("Invalid domain '%s' in event filter %d", filter$domain, i))
    }

    if (!is.numeric(filter$conceptIds) || length(filter$conceptIds) == 0) {
      stop(sprintf("conceptIds must be non-empty numeric vector in event filter %d", i))
    }
  }
}
