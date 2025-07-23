#' Create detailed cost covariate settings
#'
#' @description
#' Creates detailed settings for extracting specific cost-based covariates with
#' fine-grained control over concept sets and temporal windows.
#'
#' @param analyses A data frame with columns:
#'   \itemize{
#'     \item{analysisId}{Unique identifier for the analysis}
#'     \item{analysisName}{Name of the analysis}
#'     \item{domainIds}{Comma-separated list of domain IDs}
#'     \item{startDay}{Start day relative to index}
#'     \item{endDay}{End day relative to index}
#'     \item{isBinary}{Whether to create binary covariate}
#'     \item{missingMeansZero}{Whether missing values mean zero}
#'   }
#' @param covariateIds A vector of covariate IDs corresponding to each analysis
#' @param conceptSets A list of concept sets, each containing:
#'   \itemize{
#'     \item{conceptSetId}{Unique identifier}
#'     \item{conceptSetName}{Name of the concept set}
#'     \item{conceptIds}{Vector of concept IDs}
#'   }
#' @param temporalSequence A data frame defining temporal sequences with columns:
#'   \itemize{
#'     \item{sequenceId}{Unique identifier}
#'     \item{sequenceName}{Name of the sequence}
#'     \item{startDays}{Vector of start days}
#'     \item{endDays}{Vector of end days}
#'   }
#'
#' @return
#' An object of type \code{detailedCostCovariateSettings}
#'
#' @export
createDetailedCostCovariateSettings <- function(analyses = NULL,
                                                covariateIds = NULL,
                                                conceptSets = NULL,
                                                temporalSequence = NULL) {
  
  # Default analyses if none provided
  if (is.null(analyses)) {
    analyses <- data.frame(
      analysisId = c(1, 2, 3, 4, 5),
      analysisName = c(
        "Total costs in prior year",
        "Medical costs in prior year",
        "Pharmacy costs in prior year",
        "Total costs on index date",
        "Total costs in future year"
      ),
      domainIds = c("", "Visit,Procedure,Measurement,Observation", "Drug", "", ""),
      startDay = c(-365, -365, -365, 0, 1),
      endDay = c(-1, -1, -1, 0, 365),
      isBinary = rep(FALSE, 5),
      missingMeansZero = rep(TRUE, 5),
      stringsAsFactors = FALSE
    )
  }
  
  # Validate analyses
  requiredColumns <- c("analysisId", "analysisName", "startDay", "endDay")
  missingColumns <- setdiff(requiredColumns, names(analyses))
  if (length(missingColumns) > 0) {
    stop(paste("analyses missing required columns:", paste(missingColumns, collapse = ", ")))
  }
  
  # Set default values for optional columns
  if (!"domainIds" %in% names(analyses)) {
    analyses$domainIds <- ""
  }
  if (!"isBinary" %in% names(analyses)) {
    analyses$isBinary <- FALSE
  }
  if (!"missingMeansZero" %in% names(analyses)) {
    analyses$missingMeansZero <- TRUE
  }
  
  # Generate covariate IDs if not provided
  if (is.null(covariateIds)) {
    covariateIds <- analyses$analysisId * 1000
  }
  
  if (length(covariateIds) != nrow(analyses)) {
    stop("Length of covariateIds must match number of analyses")
  }
  
  # Create settings object
  settings <- list(
    analyses = analyses,
    covariateIds = covariateIds,
    conceptSets = conceptSets,
    temporalSequence = temporalSequence
  )
  
  class(settings) <- c("detailedCostCovariateSettings", "costCovariateSettings")
  return(settings)
}