# R/visualization.R

#' Plot Covariate Data
#'
#' @description
#' Creates a plot of the covariate data, either as boxplots for person-level data
#' or bar charts for aggregated data. This is an S3 method for `CovariateData` objects.
#'
#' @param x            A `CovariateData` object.
#' @param type         The type of plot to create. Options are `"boxplot"` for person-level
#'                     data (`aggregated = FALSE`) or `"barchart"` for aggregated data
#'                     (`aggregated = TRUE`).
#' @param top_n        The maximum number of covariates to plot, sorted by frequency (for boxplots)
#'                     or mean value (for barcharts). Defaults to 20.
#' @param title        An optional title for the plot. If `NULL`, a default title is generated.
#' @param ...          Additional arguments (not used).
#'
#' @return
#' A `ggplot` object.
#'
#' @export
plot.CovariateData <- function(x,
                               type = "barchart",
                               top_n = 20,
                               title = NULL,
                               ...) {
  
  # --- Input Validation ---
  checkmate::assertClass(x, "CovariateData")
  checkmate::assertChoice(type, choices = c("barchart", "boxplot"))
  checkmate::assertInt(top_n, lower = 1)
  checkmate::assertString(title, null.ok = TRUE)
  
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    stop("The 'ggplot2' package is required for plotting. Please install it.")
  }
  
  tableNames <- Andromeda::listAndromedaTables(x)
  
  if (type == "barchart") {
    # --- Bar Chart for Aggregated Data ---
    if (!"covariatesContinuous" %in% tableNames) {
      stop("Cannot create a barchart. The CovariateData object does not contain aggregated data (`covariatesContinuous` table).")
    }
    
    ParallelLogger::logInfo("Generating bar chart for aggregated data.")
    
    plotData <- x$covariatesContinuous %>%
      dplyr::inner_join(x$covariateRef, by = "covariateId") %>%
      dplyr::arrange(dplyr::desc(.data$averageValue)) %>%
      dplyr::slice_head(n = top_n) %>%
      dplyr::collect() # Bring data into memory for plotting
    
    plotTitle <- title %||% "Mean Covariate Values with Standard Deviation"
    
    p <- ggplot2::ggplot(plotData, ggplot2::aes(x = stats::reorder(.data$covariateName, .data$averageValue), y = .data$averageValue)) +
      ggplot2::geom_bar(stat = "identity", fill = "skyblue", alpha = 0.8) +
      ggplot2::geom_errorbar(ggplot2::aes(ymin = .data$averageValue - .data$standardDeviation,
                                          ymax = .data$averageValue + .data$standardDeviation),
                             width = 0.25) +
      ggplot2::coord_flip() +
      ggplot2::labs(
        title = plotTitle,
        subtitle = paste("Showing top", nrow(plotData), "covariates by mean value"),
        x = "Covariate",
        y = "Mean Value"
      ) +
      ggplot2::theme_minimal()
    
  } else if (type == "boxplot") {
    # --- Boxplot for Person-Level Data ---
    if (!"covariates" %in% tableNames) {
      stop("Cannot create a boxplot. The CovariateData object does not contain person-level data (`covariates` table).")
    }
    
    ParallelLogger::logInfo("Generating boxplot for person-level data.")
    
    # Identify the top N covariates by frequency
    topCovariates <- x$covariates %>%
      dplyr::group_by(.data$covariateId) %>%
      dplyr::summarise(n = dplyr::n()) %>%
      dplyr::arrange(dplyr::desc(.data$n)) %>%
      dplyr::slice_head(n = top_n) %>%
      dplyr::pull(.data$covariateId)
    
    plotData <- x$covariates %>%
      dplyr::filter(.data$covariateId %in% topCovariates) %>%
      dplyr::inner_join(x$covariateRef, by = "covariateId") %>%
      dplyr::collect()
    
    plotTitle <- title %||% "Distribution of Covariate Values"
    
    p <- ggplot2::ggplot(plotData, ggplot2::aes(x = stats::reorder(.data$covariateName, .data$covariateValue, FUN = stats::median), y = .data$covariateValue)) +
      ggplot2::geom_boxplot(outlier.shape = NA) +
      ggplot2::geom_jitter(width = 0.2, alpha = 0.2, color = "gray50") +
      ggplot2::coord_flip() +
      ggplot2::labs(
        title = plotTitle,
        subtitle = paste("Showing top", length(topCovariates), "most frequent covariates"),
        x = "Covariate",
        y = "Value"
      ) +
      ggplot2::theme_minimal()
  }
  
  return(p)
}

# Helper for handling NULL title
`%||%` <- function(a, b) {
  if (is.null(a)) b else a
}