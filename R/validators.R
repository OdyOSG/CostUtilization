validateInputs <- function(params) {
  # Basic type checks
  rlang::check_installed("checkmate")
  checkmate::assertClass(params$conn, "DatabaseConnection")
  checkmate::assertString(params$cdmSchema)
  checkmate::assertString(params$cohortSchema)
  checkmate::assertString(params$cohortTable)
  checkmate::assertInt(params$cohortId)
  checkmate::assertInt(params$startOffsetDays)
  checkmate::assertInt(params$endOffsetDays)
  checkmate::assertIntegerish(params$restrictVisitConceptIds, null.ok = TRUE)
  checkmate::assertList(params$eventFilters, null.ok = TRUE, types = "list")
  # ... more checks for each parameter
  
  if (!is.null(params$eventFilters)) {
    purrr::iwalk(params$eventFilters, ~{
      checkmate::assertList(.x, names = "named")
      checkmate::assertNames(names(.x), must.include = c("name", "concepts", "domainScope"))
      checkmate::assertCharacter(.x$name, len = 1)
      checkmate::assertIntegerish(.x$concepts)
      checkmate::assertChoice(.x$domainScope, c("All","Condition","Procedure","Drug","Measurement","Observation"))
    })
  }
  
  invisible(TRUE)
}

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