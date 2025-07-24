# R/CostCovariateData.R

#' The CostCovariateData class
#'
#' @description
#' A class for storing cost and utilization covariate data extracted from the database.
#' This object uses the Andromeda package to store data on disk, which allows it
#' to handle data sets that are much larger than the available RAM.
#'
#' @field andromeda An Andromeda object containing the covariate data.
#' @field metaData A list of metadata about the object.
#'
#' @seealso \code{\link{getDbCostData}}
#' @export
#' @importFrom methods setClass
setClass("CostCovariateData",
  slots = list(
    andromeda = "ANY",
    metaData = "list"
  )
)

#' Print a CostCovariateData object
#'
#' @param x A CostCovariateData object.
#' @param ... Additional arguments (ignored).
#'
#' @return Invisibly returns the input object.
#' @export
print.CostCovariateData <- function(x, ...) {
  writeLines("CostCovariateData object")
  writeLines("")
  writeLines(paste("Database ID:", x$metaData$databaseId))
  writeLines(paste("Cohort ID:", x_getMetaData("cohortId", x, "...")))
  writeLines(paste("Aggregated:", x_getMetaData("aggregated", x, "...")))
  writeLines("")

  if (x_getMetaData("aggregated", x, FALSE)) {
    covariateCount <- x$andromeda$covariates %>%
      count() %>%
      pull()
    nonZeroCount <- x$andromeda$covariates %>%
      filter(.data$meanValue > 0) %>%
      count() %>%
      pull()
    writeLines(paste("Number of covariates:", covariateCount))
    writeLines(paste("Number of non-zero covariates:", nonZeroCount))
  } else {
    subjectCount <- x$andromeda$covariates %>%
      distinct(.data$subjectId) %>%
      count() %>%
      pull()
    covariateDefCount <- x$andromeda$covariateRef %>%
      count() %>%
      pull()
    valueCount <- x$andromeda$covariates %>%
      count() %>%
      pull()

    writeLines(paste("Number of subjects:", subjectCount))
    writeLines(paste("Number of covariates:", covariateDefCount))
    writeLines(paste("Number of covariate values:", valueCount))
  }
  invisible(x)
}

#' Summary of CostCovariateData
#'
#' @description
#' Provides a summary of the covariates. This function executes a query on the
#' Andromeda object and returns an in-memory data frame.
#'
#' @param object A CostCovariateData object.
#' @param ... Additional arguments (ignored).
#'
#' @return A tibble with the summary statistics.
#' @export
summary.CostCovariateData <- function(object, ...) {
  if (object$metaData$aggregated) {
    # For aggregated data, just join and collect
    summary_data <- object$andromeda$covariates %>%
      inner_join(object$andromeda$covariateRef, by = "covariateId") %>%
      arrange(desc(.data$meanValue)) %>%
      select("covariateName", "meanValue", "sdValue", "minValue", "medianValue", "maxValue") %>%
      collect() # Bring result into memory
  } else {
    # For person-level data, compute summary stats using dplyr, then join and collect
    summary_stats <- object$andromeda$covariates %>%
      group_by(.data$covariateId) %>%
      summarise(
        n = n(),
        mean = mean(.data$covariateValue, na.rm = TRUE),
        sd = sd(.data$covariateValue, na.rm = TRUE),
        min = min(.data$covariateValue, na.rm = TRUE),
        median = median(.data$covariateValue, na.rm = TRUE),
        max = max(.data$covariateValue, na.rm = TRUE)
      )

    summary_data <- summary_stats %>%
      inner_join(object$andromeda$covariateRef, by = "covariateId") %>%
      arrange(desc(.data$mean)) %>%
      select("covariateName", "n", "mean", "sd", "min", "median", "max") %>%
      collect() # Bring result into memory
  }
  return(summary_data)
}

#' Convert CostCovariateData to a standard Andromeda table
#'
#' @param costCovariateData A CostCovariateData object.
#'
#' @return
#' A `tbl_Andromeda` object. This is a pointer to the data on disk and can be
#' manipulated using `dplyr` verbs. Use `collect()` to bring the data into memory.
#' @export
toAndromedaTable <- function(costCovariateData) {
  if (!inherits(costCovariateData, "CostCovariateData")) {
    stop("Input must be a CostCovariateData object")
  }
  return(costCovariateData$andromeda$covariates)
}

#' Save CostCovariateData to a file
#'
#' @description
#' Saves the `CostCovariateData` object to a file. The Andromeda data is saved in a
#' zip file in the specified folder.
#'
#' @param costCovariateData A CostCovariateData object.
#' @param file The path to the file where the data should be saved.
#'
#' @export
saveCostCovariateData <- function(costCovariateData, file) {
  if (!inherits(costCovariateData, "CostCovariateData")) {
    stop("Input must be a CostCovariateData object")
  }
  # The metaData is already an attribute of the andromeda object
  Andromeda::saveAndromeda(costCovariateData$andromeda, fileName = file)
  invisible(TRUE)
}

#' Load CostCovariateData from a file
#'
#' @param file The path to the file where the data was saved.
#'
#' @return A CostCovariateData object.
#' @export
loadCostCovariateData <- function(file) {
  if (!file.exists(file)) {
    stop("File does not exist: ", file)
  }
  andromeda <- Andromeda::loadAndromeda(file)
  metaData <- attr(andromeda, "metaData")

  result <- new("CostCovariateData",
    andromeda = andromeda,
    metaData = metaData
  )
  return(result)
}

# Helper function to safely get metadata
x_getMetaData <- function(key, costCovariateData, default) {
  if (is.null(costCovariateData$metaData[[key]])) default else costCovariateData$metaData[[key]]
}
