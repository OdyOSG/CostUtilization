# Copyright 2025 Observational Health Data Sciences and Informatics
# (This file is part of CostUtilization)

#' (S3 method) Construct Cost and Utilization Covariates
#'
#' @description
#' This is an S3 method for the `FeatureExtraction::getDbCovariateData` generic.
#' It is called when the `covariateSettings` object is of type `costCovariateSettings`.
#'
#' @details
#' This function is not intended to be called directly but is automatically dispatched
#' by `FeatureExtraction::getDbCovariateData`. It serves as a wrapper around the main
#' `getDbCostCovariateData` function to ensure compatibility with the OHDSI framework.
#'
#' @param connection              A connection to the server containing the schema.
#' @param oracleTempSchema        DEPRECATED. Use `tempEmulationSchema` instead.
#' @param cdmDatabaseSchema       The name of the database schema that contains the OMOP CDM instance.
#' @param cohortTable             Name of the table holding the cohort.
#' @param cohortId                The ID of the cohort for which to construct covariates. Use `cohortIds` for multiple cohorts.
#' @param cohortIds               A vector of cohort definition IDs.
#' @param cdmVersion              The version of the OMOP CDM.
#' @param rowIdField              The name of the field in the cohort table that is to be used as the `row_id` field.
#' @param covariateSettings       An S3 object of type `costCovariateSettings`.
#' @param aggregated              A logical flag indicating whether to compute aggregate statistics (`TRUE`) or person-level data (`FALSE`).
#' @param tempEmulationSchema     A schema with write privileges for emulating temp tables.
#'
#' @return
#' A `CovariateData` object.
#'
#' @export
getDbCovariateData.costCovariateSettings <- function(connection,
                                                     oracleTempSchema = NULL,
                                                     cdmDatabaseSchema,
                                                     cohortTable,
                                                     cohortId = -1,
                                                     cohortIds = c(-1),
                                                     cdmVersion = "5",
                                                     rowIdField = "subject_id",
                                                     covariateSettings,
                                                     aggregated = FALSE,
                                                     tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  # --- Deprecation and ID handling ---
  if (cohortId != -1) {
    warning("The 'cohortId' argument is deprecated. Please use 'cohortIds'.")
    cohortIds <- cohortId
  }
  
  # --- Call the main workhorse function ---
  results <- getDbCostCovariateData(
    connection = connection,
    oracleTempSchema = oracleTempSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohortIds,
    cdmVersion = cdmVersion,
    rowIdField = rowIdField,
    covariateSettings = covariateSettings,
    aggregated = aggregated,
    tempEmulationSchema = tempEmulationSchema
  )
  return(results)
}

# (The internal helper `generateReferenceTablesFromUnifiedSettings` can either live here
# or in the same file as getDbCostCovariateData. Keeping it with the main function
# might be cleaner.)
#' Internal helper to generate reference tables from the UNIFIED settings object
#' @keywords internal
generateReferenceTablesFromUnifiedSettings <- function(covariateSettings) {
  # --- Define base components ---
  domains <- dplyr::tibble(
    domainIdValue = 1:7,
    costDomainId = c("Drug", "Visit", "Procedure", "Device", "Measurement", "Observation", "Specimen")
  )
  
  # --- Unify all temporal windows into a single data frame ---
  temporalWindows <- dplyr::tibble()
  s <- covariateSettings$temporal
  a <- covariateSettings$analyses
  
  # Add standard windows if they are used by any analysis
  if (any(grepl("LongTerm", names(which(unlist(a)))))) {
    temporalWindows <- temporalWindows %>% dplyr::add_row(startDay = s$longTermStartDays, endDay = s$endDays)
  }
  if (any(grepl("MediumTerm", names(which(unlist(a)))))) {
    temporalWindows <- temporalWindows %>% dplyr::add_row(startDay = s$mediumTermStartDays, endDay = s$endDays)
  }
  if (any(grepl("ShortTerm", names(which(unlist(a)))))) {
    temporalWindows <- temporalWindows %>% dplyr::add_row(startDay = s$shortTermStartDays, endDay = s$endDays)
  }
  
  # Add custom windows
  if (length(s$customTemporalWindows) > 0) {
    customWindowsDf <- do.call(rbind, lapply(s$customTemporalWindows, function(x) data.frame(startDay=x[1], endDay=x[2])))
    temporalWindows <- temporalWindows %>% dplyr::bind_rows(customWindowsDf)
  }
  
  # Finalize the list of unique windows to be processed
  temporalWindows <- temporalWindows %>% 
    dplyr::distinct() %>%
    dplyr::mutate(windowId = dplyr::row_number())
  
  if (nrow(temporalWindows) == 0) {
    return(list(analysisRef = tibble::tibble(), covariateRef = tibble::tibble(), temporalWindows = tibble::tibble()))
  }
  
  # --- Construct analysis and covariate references based on active analyses ---
  analysisRef <- dplyr::tibble()
  covariateRef <- dplyr::tibble()
  
  # Helper function to check if any analysis of a certain type is active
  isAnalysisActive <- function(baseName) {
    any(grepl(baseName, names(which(unlist(a)))))
  }
  
  # Analysis 1: Total Cost
  if (isAnalysisActive("CostTotal")) {
    analysisId <- 1
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Total cost", domainId = "Cost", isBinary = "N", missingMeansZero = "Y")
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      temporalWindows %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 1000 + .data$windowId,
          covariateName = paste0("Total cost during day ", .data$startDay, " to ", .data$endDay, " relative to index"),
          conceptId = 0
        )
    )
  }
  
  # Analysis 2: Cost By Domain
  if (isAnalysisActive("CostByDomain")) {
    analysisId <- 2
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Cost by domain", domainId = "Cost", isBinary = "N", missingMeansZero = "Y")
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      tidyr::crossing(temporalWindows, domains) %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 2000 + (.data$windowId * 10) + .data$domainIdValue,
          covariateName = paste0(.data$costDomainId, " cost during day ", .data$startDay, " to ", .data$endDay, " relative to index"),
          conceptId = 0
        )
    )
  }
  
  # Analysis 3: Cost By Type
  if (isAnalysisActive("CostByType")) {
    analysisId <- 3
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Cost by type", domainId = "Cost", isBinary = "N", missingMeansZero = "Y")
  }
  
  # Analysis 4: Utilization
  if (isAnalysisActive("Utilization")) {
    analysisId <- 4
    analysisRef <- analysisRef %>% dplyr::add_row(analysisId = analysisId, analysisName = "Utilization", domainId = "Cost", isBinary = "N", missingMeansZero = "Y")
    covariateRef <- covariateRef %>% dplyr::bind_rows(
      temporalWindows %>%
        dplyr::mutate(
          analysisId = analysisId,
          covariateId = 4000 + .data$windowId,
          covariateName = paste0("Utilization days during day ", .data$startDay, " to ", .data$endDay, " relative to index"),
          conceptId = 0
        )
    )
  }
  
  return(list(
    analysisRef = analysisRef,
    covariateRef = covariateRef %>% dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId),
    temporalWindows = temporalWindows %>% dplyr::select(.data$windowId, .data$startDay, .data$endDay)
  ))
}