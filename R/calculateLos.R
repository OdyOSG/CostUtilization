#' #' Calculate Length of Stay (LOS) for a Cohort
#' #'
#' #' @description
#' #' Calculates the length of stay (LOS) in days for visits that occur within a
#' #' specified time window relative to a cohort's start date.
#' #' @return A dplyr `tibble` with the calculated LOS for each qualifying visit.
#' #' @export
#' calculateLos <- function(connection = NULL,
#'                          params,
#'                          visitTable = c("visit_occurrence", "visit_detail"),
#'                          visitConceptIds = NULL,
#'                          verbose = TRUE) {
#'   
#'   # --- Validation ---
#'   visitTable <- match.arg(visitTable)
#'   errorMessages <- checkmate::makeAssertCollection()
#'   checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
#'   checkmate::assertCharacter(cohortDatabaseSchema, len = 1, add = errorMessages)
#'   checkmate::assertCharacter(cohortTable, len = 1, add = errorMessages)
#'   checkmate::assertIntegerish(cohortId, len = 1, add = errorMessages)
#'   checkmate::assertIntegerish(timeA, len = 1, add = errorMessages)
#'   checkmate::assertIntegerish(timeB, len = 1, add = errorMessages)
#'   if (timeB < timeA) {
#'     checkmate::addFailure("`timeB` must be greater than or equal to `timeA`.", add = errorMessages)
#'   }
#'   if (!is.null(visitConceptIds)) {
#'     checkmate::assertIntegerish(visitConceptIds, min.len = 1, unique = TRUE, add = errorMessages)
#'   }
#'   checkmate::reportAssertions(errorMessages)
#'   
#'   # --- Connection Handling ---
#'   if (is.null(connectionDetails) && is.null(connection)) {
#'     rlang::abort("Provide either `connectionDetails` or an open `connection`.")
#'   }
#'   if (!is.null(connectionDetails)) {
#'     connection <- DatabaseConnector::connect(connectionDetails)
#'     on.exit(DatabaseConnector::disconnect(connection))
#'   }
#'   
#'   # --- Parameter Preparation ---
#'   visitIdCol <- if (visitTable == "visit_occurrence") "visit_occurrence_id" else "visit_detail_id"
#'   
#'   if (!is.null(openEndedAsOfDate)) {
#'     openEndedAsOfDate <- as.character(as.Date(openEndedAsOfDate))
#'   }
#'   
#'   logMessage(sprintf("Calculating LOS for cohort ID %d...", cohortId), verbose, "INFO")
#'   
#'   # --- SQL Execution ---
#'   results <- DatabaseConnector::renderTranslateQuerySql(
#'     connection = connection,
#'     sql = SqlRender::readSql(system.file("sql/sql_server", "CalculateLos.sql", package = "CostUtilization")),
#'     cdm_database_schema = cdmDatabaseSchema,
#'     cohort_database_schema = cohortDatabaseSchema,
#'     cohort_table = cohortTable,
#'     cohort_id = cohortId,
#'     visit_table = visitTable,
#'     visit_id_col = visitIdCol,
#'     visit_concept_ids = if (is.null(visitConceptIds)) "" else paste(visitConceptIds, collapse = ","),
#'     time_a = timeA,
#'     time_b = timeB,
#'     open_ended_as_of_date = openEndedAsOfDate,
#'     snakeCaseToCamelCase = TRUE
#'   )
#'   
#'   logMessage(sprintf("Calculation complete. Found %d visit records for the cohort.", nrow(results)), verbose, "SUCCESS")
#'   
#'   return(dplyr::as_tibble(results))
#' }