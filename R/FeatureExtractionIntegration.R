#' The CovariateData Class
#'
#' @description
#' A custom S4 class that inherits from `Andromeda::Andromeda`. It is designed to
#' store covariate data from a `CostUtilization` analysis in a format compatible
#' with the OHDSI FeatureExtraction standard.
#'
#' @seealso [createCostCovariateData()]
#' @name CovariateData-class
#' @aliases CovariateData
#' @export
setClass("CovariateData", contains = "Andromeda")

#' Save Covariate Data to a File
#'
#' @description
#' Saves a `CovariateData` object to a file.
#'
#' @param covariateData   An object of type `CovariateData`.
#' @param file            The path to the file where the data will be written.
#'
#' @return No return value, called for its side effect.
#' @export
saveCovariateData <- function(covariateData, file) {
  if (missing(covariateData) || !isCovariateData(covariateData)) {
    stop("`covariateData` must be an object of class 'CovariateData'.")
  }
  if (missing(file)) {
    stop("`file` argument must be specified.")
  }
  Andromeda::saveAndromeda(covariateData, file)
}

#' Load Covariate Data from a File
#'
#' @description
#' Loads a `CovariateData` object from a file.
#'
#' @param file       The path to the file containing the data.
#'
#' @return An object of class `CovariateData`.
#' @export
loadCovariateData <- function(file) {
  if (!file.exists(file)) {
    stop("Cannot find file: ", file)
  }
  covariateData <- Andromeda::loadAndromeda(file)
  class(covariateData) <- c("CovariateData", class(covariateData))
  return(covariateData)
}

#' Show Method for CovariateData
#' @param object  An object of class `CovariateData`.
#' @rdname CovariateData-class
#' @export
setMethod("show", "CovariateData", function(object) {
  cli::cat_line(pillar::style_subtle("# CostUtilization CovariateData object"))
  cli::cat_line("")

  metaData <- attr(object, "metaData")
  if (!is.null(metaData)) {
    cli::cat_bullet("Analysis ID: ", crayon::cyan(metaData$analysisId))
    cli::cat_bullet("Cohort ID:   ", crayon::cyan(metaData$cohortId))
    cli::cat_bullet("Format:      ", crayon::cyan(metaData$resultFormat))
    cli::cat_bullet("DB ID:       ", crayon::cyan(metaData$databaseId))
  }
  cli::cat_line("")
  cli::cat_line(pillar::style_subtle("Inherits from Andromeda:"))

  # Temporarily remove CovariateData class to call Andromeda's show method
  class(object) <- "Andromeda"
  show(object)
})

#' Summary Method for CovariateData
#' @param object An object of class `CovariateData`.
#' @rdname CovariateData-class
#' @export
setMethod("summary", "CovariateData", function(object) {
  if (!isCovariateData(object) || !Andromeda::isValidAndromeda(object)) {
    stop("Object is not a valid or open CovariateData object.")
  }

  covariateValueCount <- 0
  if ("covariates" %in% names(object)) {
    covariateValueCount <- object$covariates %>% dplyr::count() %>% dplyr::pull()
  }

  covariateCount <- 0
  if ("covariateRef" %in% names(object)) {
    covariateCount <- object$covariateRef %>% dplyr::count() %>% dplyr::pull()
  }

  result <- list(
    metaData = attr(object, "metaData"),
    covariateCount = covariateCount,
    covariateValueCount = covariateValueCount
  )
  class(result) <- "summary.CovariateData"
  return(result)
})

#' @export
print.summary.CovariateData <- function(x, ...) {
  cli::cli_h1("CovariateData Summary")

  cli::cli_h2("Analysis Information")
  meta <- x$metaData
  cli::cat_bullet("Analysis ID: ", crayon::cyan(meta$analysisId))
  cli::cat_bullet("Cohort ID:   ", crayon::cyan(meta$cohortId))
  cli::cat_bullet("Format:      ", crayon::cyan(meta$resultFormat))

  cli::cli_h2("Data Summary")
  cli::cat_bullet("Unique Covariates: ", crayon::green(format(x$covariateCount, big.mark = ",")))
  cli::cat_bullet("Covariate Values:  ", crayon::green(format(x$covariateValueCount, big.mark = ",")))
}

#' Check if an object is a CovariateData object
#'
#' @param x The object to check.
#' @return A logical value.
#' @export
isCovariateData <- function(x) {
  inherits(x, "CovariateData")
}

#' Check if CovariateData is aggregated
#' @description
#' Checks if a `CovariateData` object contains aggregated summary statistics
#' rather than person-level data. This implementation uses the `resultFormat`
#' attribute in the metadata, which is more robust than checking for column names.
#'
#' @param x The `CovariateData` object to check.
#' @return A logical value.
#' @export
isAggregatedCovariateData <- function(x) {
  if (!isCovariateData(x)) stop("Object must be of class 'CovariateData'")
  if (!Andromeda::isValidAndromeda(x)) stop("CovariateData object is closed")

  metaData <- attr(x, "metaData")
  return(isTRUE(metaData$resultFormat == "aggregated"))
}

#' Check if CovariateData is temporal
#'
#' @description
#' Checks if a `CovariateData` object contains temporal data (i.e., has a `timeId` column).
#'
#' @param x The `CovariateData` object to check.
#' @return A logical value.
#' @export
isTemporalCovariateData <- function(x) {
  if (!isCovariateData(x)) stop("Object must be of class 'CovariateData'")
  if (!Andromeda::isValidAndromeda(x)) stop("CovariateData object is closed")

  return("timeId" %in% names(x$covariates))
}