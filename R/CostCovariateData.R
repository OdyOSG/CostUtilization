#' CostCovariateData class
#'
#' @description
#' A class for storing cost covariate data extracted from the database.
#' This class extends the standard covariate data structure with cost-specific
#' functionality.
#'
#' @field covariates Data frame with cost covariates
#' @field covariateRef Reference table with covariate definitions
#' @field analysisRef Reference table with analysis definitions
#' @field metaData Metadata about the extraction
#'
#' @export
#' @importFrom methods setClass
setClass("CostCovariateData",
         slots = list(
           covariates = "data.frame",
           covariateRef = "data.frame",
           analysisRef = "data.frame",
           metaData = "list"
         ))

#' Print CostCovariateData object
#'
#' @param x CostCovariateData object
#' @param ... Additional arguments (ignored)
#' @return Invisibly returns the input
#' @export
print.CostCovariateData <- function(x, ...) {
  writeLines("CostCovariateData object")
  writeLines("")
  writeLines(paste("Database ID:", x$metaData$databaseId))
  writeLines(paste("Cohort ID:", x$metaData$cohortId))
  writeLines(paste("Aggregated:", x$metaData$aggregated))
  writeLines("")
  
  if (x$metaData$aggregated) {
    writeLines(paste("Number of covariates:", nrow(x$covariates)))
    writeLines(paste("Number of non-zero covariates:", sum(x$covariates$meanValue > 0)))
  } else {
    writeLines(paste("Number of subjects:", length(unique(x$covariates$subjectId))))
    writeLines(paste("Number of covariates:", length(unique(x$covariates$covariateId))))
    writeLines(paste("Number of covariate values:", nrow(x$covariates)))
  }
  
  invisible(x)
}

#' Summary of CostCovariateData
#'
#' @param object CostCovariateData object
#' @param ... Additional arguments (ignored)
#' @return Summary data frame
#' @export
summary.CostCovariateData <- function(object, ...) {
  if (object$metaData$aggregated) {
    # Aggregated summary
    summary_data <- merge(
      object$covariates,
      object$covariateRef,
      by = "covariateId"
    )
    
    summary_data <- summary_data[order(-summary_data$meanValue), ]
    
    return(summary_data[, c("covariateName", "meanValue", "sdValue", 
                            "minValue", "medianValue", "maxValue")])
  } else {
    # Person-level summary
    summary_stats <- aggregate(
      covariateValue ~ covariateId,
      data = object$covariates,
      FUN = function(x) c(
        n = length(x),
        mean = mean(x),
        sd = sd(x),
        min = min(x),
        median = median(x),
        max = max(x)
      )
    )
    
    summary_stats <- do.call(data.frame, summary_stats)
    names(summary_stats) <- c("covariateId", "n", "mean", "sd", "min", "median", "max")
    
    summary_data <- merge(
      summary_stats,
      object$covariateRef,
      by = "covariateId"
    )
    
    return(summary_data[order(-summary_data$mean), 
                        c("covariateName", "n", "mean", "sd", "min", "median", "max")])
  }
}

#' Filter CostCovariateData by analysis ID
#'
#' @param costCovariateData CostCovariateData object
#' @param analysisIds Vector of analysis IDs to keep
#' @return Filtered CostCovariateData object
#' @export
filterByAnalysisId <- function(costCovariateData, analysisIds) {
  if (!inherits(costCovariateData, "CostCovariateData")) {
    stop("Input must be a CostCovariateData object")
  }
  
  # Filter covariate reference
  filteredCovariateRef <- costCovariateData$covariateRef[
    costCovariateData$covariateRef$analysisId %in% analysisIds, 
  ]
  
  # Filter covariates
  filteredCovariates <- costCovariateData$covariates[
    costCovariateData$covariates$covariateId %in% filteredCovariateRef$covariateId, 
  ]
  
  # Filter analysis reference
  filteredAnalysisRef <- costCovariateData$analysisRef[
    costCovariateData$analysisRef$analysisId %in% analysisIds, 
  ]
  
  # Create new object
  result <- list(
    covariates = filteredCovariates,
    covariateRef = filteredCovariateRef,
    analysisRef = filteredAnalysisRef,
    metaData = costCovariateData$metaData
  )
  
  class(result) <- "CostCovariateData"
  return(result)
}

#' Filter CostCovariateData by time window
#'
#' @param costCovariateData CostCovariateData object
#' @param startDay Start day of window
#' @param endDay End day of window
#' @return Filtered CostCovariateData object
#' @export
filterByTimeWindow <- function(costCovariateData, startDay, endDay) {
  if (!inherits(costCovariateData, "CostCovariateData")) {
    stop("Input must be a CostCovariateData object")
  }
  
  # Extract time window from covariate names
  # This is a simplified approach - in practice would need more sophisticated parsing
  timePattern <- paste0("day ", startDay, " to ", endDay)
  
  filteredCovariateRef <- costCovariateData$covariateRef[
    grepl(timePattern, costCovariateData$covariateRef$covariateName), 
  ]
  
  filteredCovariates <- costCovariateData$covariates[
    costCovariateData$covariates$covariateId %in% filteredCovariateRef$covariateId, 
  ]
  
  result <- list(
    covariates = filteredCovariates,
    covariateRef = filteredCovariateRef,
    analysisRef = costCovariateData$analysisRef,
    metaData = costCovariateData$metaData
  )
  
  class(result) <- "CostCovariateData"
  return(result)
}

#' Convert CostCovariateData to standard covariate data format
#'
#' @param costCovariateData CostCovariateData object
#' @return Data frame in standard covariate format
#' @export
toStandardCovariateData <- function(costCovariateData) {
  if (!inherits(costCovariateData, "CostCovariateData")) {
    stop("Input must be a CostCovariateData object")
  }
  
  # Return covariates in standard format
  if (costCovariateData$metaData$aggregated) {
    return(costCovariateData$covariates[, c("cohortDefinitionId", "covariateId", 
                                            "meanValue", "sdValue", "countValue")])
  } else {
    return(costCovariateData$covariates[, c("cohortDefinitionId", "subjectId", 
                                            "covariateId", "covariateValue")])
  }
}

#' Save CostCovariateData to file
#'
#' @param costCovariateData CostCovariateData object
#' @param file Path to save file (RDS format)
#' @export
saveCostCovariateData <- function(costCovariateData, file) {
  if (!inherits(costCovariateData, "CostCovariateData")) {
    stop("Input must be a CostCovariateData object")
  }
  
  saveRDS(costCovariateData, file)
  invisible(TRUE)
}

#' Load CostCovariateData from file
#'
#' @param file Path to RDS file
#' @return CostCovariateData object
#' @export
loadCostCovariateData <- function(file) {
  if (!file.exists(file)) {
    stop("File does not exist: ", file)
  }
  
  data <- readRDS(file)
  
  if (!inherits(data, "CostCovariateData")) {
    stop("File does not contain a CostCovariateData object")
  }
  
  return(data)
}