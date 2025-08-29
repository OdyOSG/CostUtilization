
.fetchResults <- function(params, connection, tempEmulationSchema, verbose) {
  # step 1 - fulfill analysis
  
  executeSqlPlan(
    connection          = connection,
    params              = params,
    targetDialect       = DatabaseConnector::dbms(connection),
    tempEmulationSchema = tempEmulationSchema,
    verbose             = verbose
  )
  andromeda <- Andromeda::andromeda()
  DatabaseConnector::querySqlToAndromeda(
    connection,
    glue::glue("select * from {params$cohortDatabaseSchema}.{params$resultTable}"),
    andromeda = andromeda,
    andromedaTableName = "covariates_raw"
  )
  
  
}





#' Create Covariate Settings
#'
#' @description
#' Creates a tibble containing the covariate settings.
#'
#' @param costOfCareSettings  A settings object from `createCostOfCareSettings()`.
#' @param analysisId          An ID for the analysis.
#'
#' @return
#' A tibble with the covariate settings.
#'
#' @noRd
createCovariateSettings <- function(costOfCareSettings, analysisId) {
  settings <- as.list(costOfCareSettings)
  settings <- purrr::map_if(settings, is.null, ~ NA)
  settings <- purrr::map_if(settings, is.list, ~ paste(unlist(.), collapse = "; "))
  settings <- purrr::map_if(settings, is.logical, as.integer)
  
  
  dplyr::tibble(
    analysisId = analysisId,
    analysisName = "Cost of Care",
    analysisType = "Cost",
    settings = as.character(jsonlite::toJSON(settings, auto_unbox = TRUE))
  )
}

#' Create Analysis Reference
#'
#' @description
#' Creates a tibble with the analysis reference metadata.
#'
#' @param costOfCareSettings  A settings object from `createCostOfCareSettings()`.
#' @param analysisId          An ID for the analysis.
#'
#' @return
#' A tibble with the analysis reference.
#'
#' @noRd
createAnalysisRef <- function(costOfCareSettings, analysisId) {
  dplyr::tibble(
    analysisId = analysisId,
    analysisName = "Cost of Care",
    domainId = "Cost",
    startDay = costOfCareSettings$startOffsetDays,
    endDay = costOfCareSettings$endOffsetDays,
    isBinary = "N",
    missingMeansZero = "Y"
  )
}

#' Create Covariate Reference
#'
#' @description
#' Creates a tibble with the covariate reference metadata.
#'
#' @param analysisId An ID for the analysis.
#'
#' @return
#' A tibble with the covariate reference.
#'
#' @noRd
createCovariateRef <- function(analysisId) {
  covariates <- dplyr::tribble(
    ~covariateName, ~conceptId,
    "total_cost", 0,
    "total_adjusted_cost", 0,
    "cost_pppm", 0,
    "adjusted_cost_pppm", 0,
    "cost_pppy", 0,
    "adjusted_cost_pppy", 0,
    "events_per_1000_py", 0
  )
  
  covariates %>%
    dplyr::mutate(
      analysisId = analysisId,
      covariateId = analysisId * 1000 + dplyr::row_number()
    ) %>%
    dplyr::select(covariateId, covariateName, analysisId, conceptId)
}

#' Create Metadata
#'
#' @description
#' Creates a list of all metadata tibbles.
#'
#' @param costOfCareSettings  A settings object from `createCostOfCareSettings()`.
#'
#' @return
#' A list of metadata tibbles.
#'
#' @noRd
createMetaData <- function(costOfCareSettings) {
  analysisId <- createAnalysisId(costOfCareSettings)
  list(
    analysisRef = createAnalysisRef(costOfCareSettings, analysisId),
    covariateRef = createCovariateRef(analysisId),
    covariateSettings = createCovariateSettings(costOfCareSettings, analysisId)
  )
}