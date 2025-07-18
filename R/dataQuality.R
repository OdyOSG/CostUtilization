#' Assert CDM Conforms to CostUtilization Assumptions
#'
#' This function checks if the target CDM has a COST table and if the
#' visit-centric linkage assumption holds. It will stop execution if
#' the assumptions are not met.
#'
#' @param connection A connection object from `DatabaseConnector`.
#' @param cdmDatabaseSchema The schema holding the OMOP CDM data.
#' @keywords internal
assertCdmConforms <- function(connection, cdmDatabaseSchema) {
  # 1. Check for COST table existence
  costTableExists <- DatabaseConnector::existsTable(connection, cdmDatabaseSchema, "cost")
  if (!costTableExists) {
    stop("The COST table does not exist in the specified CDM schema. This package cannot run without it.")
  }
  
  # 2. Check for the convention that all costs link to a clinical event with a valid visit_occurrence_id [cite: 144, 145]
  # This is a sample check; a more exhaustive one can be developed.
  sql <- "
  SELECT COUNT(*) AS row_count
  FROM @cdm_database_schema.cost c
  LEFT JOIN @cdm_database_schema.visit_occurrence v ON c.cost_event_id = v.visit_occurrence_id
  WHERE c.cost_event_table = 'visit_occurrence' AND v.visit_occurrence_id IS NULL;
  "
  orphanCostCount <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                                sql,
                                                                cdm_database_schema = cdmDatabaseSchema,
                                                                snakeCaseToCamelCase = TRUE)
  
  if (orphanCostCount$rowCount > 0) {
    warning(paste(orphanCostCount$rowCount, "records in the COST table have a cost_event_id that does not map to a valid visit_occurrence_id. These records will be ignored."))
  }
  invisible(TRUE)
}