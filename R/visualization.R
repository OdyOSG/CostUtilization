#' Plot CostCovariateData
#'
#' @param x A CostCovariateData object.
#' @param type The type of plot, either "boxplot" or "barchart".
#' @param covariateIds A vector of covariate IDs to include in the plot.
#' @param ... Additional arguments (not used).
#'
#' @return A ggplot object.
#' @export
plot.CostCovariateData <- function(x, type = "boxplot", covariateIds = NULL, ...) {
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    stop("ggplot2 package is required for plotting. Please install it.")
  }
  
  plotData <- x$covariates %>%
    dplyr::inner_join(x$covariateRef, by = "covariateId")
  
  if (!is.null(covariateIds)) {
    plotData <- plotData %>%
      dplyr::filter(.data$covariateId %in% covariateIds)
  }
  
  if (type == "boxplot") {
    if (x$metaData$aggregated) {
      stop("Boxplots require person-level data (aggregated = FALSE).")
    }
    p <- ggplot2::ggplot(plotData, ggplot2::aes(x = .data$covariateName, y = .data$covariateValue)) +
      ggplot2::geom_boxplot() +
      ggplot2::coord_flip() +
      ggplot2::labs(title = "Cost Distribution", x = "Covariate", y = "Cost Value")
  } else if (type == "barchart") {
    if (!x$metaData$aggregated) {
      stop("Bar charts require aggregated data (aggregated = TRUE).")
    }
    p <- ggplot2::ggplot(plotData, ggplot2::aes(x = .data$covariateName, y = .data$meanValue)) +
      ggplot2::geom_bar(stat = "identity") +
      ggplot2::geom_errorbar(ggplot2::aes(ymin = .data$meanValue - .data$sdValue, ymax = .data.meanValue + .data$sdValue), width = 0.2) +
      ggplot2::coord_flip() +
      ggplot2::labs(title = "Mean Costs", x = "Covariate", y = "Mean Cost")
  } else {
    stop("Invalid plot type. Choose 'boxplot' or 'barchart'.")
  }
  
  return(p)
}