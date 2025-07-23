#' Get default cost analyses
#'
#' @description
#' Returns a data frame with predefined cost analyses commonly used in 
#' healthcare cost studies.
#'
#' @param includeUtilization Whether to include utilization-based analyses
#' @param includeDistribution Whether to include distribution analyses
#'
#' @return
#' A data frame with analysis definitions
#'
#' @export
getDefaultCostAnalyses <- function(includeUtilization = TRUE,
                                   includeDistribution = FALSE) {
  
  # Base cost analyses
  analyses <- data.frame(
    analysisId = integer(),
    analysisName = character(),
    domainIds = character(),
    startDay = integer(),
    endDay = integer(),
    isBinary = logical(),
    missingMeansZero = logical(),
    aggregateMethod = character(),
    stringsAsFactors = FALSE
  )
  
  # Total costs across time windows
  totalCostAnalyses <- data.frame(
    analysisId = 1:7,
    analysisName = c(
      "Total costs 365d prior",
      "Total costs 180d prior",
      "Total costs 30d prior",
      "Total costs on index",
      "Total costs 30d after",
      "Total costs 180d after",
      "Total costs 365d after"
    ),
    domainIds = "",
    startDay = c(-365, -180, -30, 0, 1, 1, 1),
    endDay = c(-1, -1, -1, 0, 30, 180, 365),
    isBinary = FALSE,
    missingMeansZero = TRUE,
    aggregateMethod = "sum",
    stringsAsFactors = FALSE
  )
  analyses <- rbind(analyses, totalCostAnalyses)
  
  # Domain-specific costs
  domainAnalyses <- data.frame(
    analysisId = 11:16,
    analysisName = c(
      "Medical costs 365d prior",
      "Pharmacy costs 365d prior",
      "Procedure costs 365d prior",
      "Visit costs 365d prior",
      "Device costs 365d prior",
      "Other costs 365d prior"
    ),
    domainIds = c(
      "Visit,Procedure,Measurement,Observation",
      "Drug",
      "Procedure",
      "Visit",
      "Device",
      "Condition,Specimen"
    ),
    startDay = -365,
    endDay = -1,
    isBinary = FALSE,
    missingMeansZero = TRUE,
    aggregateMethod = "sum",
    stringsAsFactors = FALSE
  )
  analyses <- rbind(analyses, domainAnalyses)
  
  # Cost type analyses (allowed vs paid)
  costTypeAnalyses <- data.frame(
    analysisId = 21:24,
    analysisName = c(
      "Allowed amount 365d prior",
      "Paid amount 365d prior",
      "Allowed amount on index",
      "Paid amount on index"
    ),
    domainIds = "",
    startDay = c(-365, -365, 0, 0),
    endDay = c(-1, -1, 0, 0),
    isBinary = FALSE,
    missingMeansZero = TRUE,
    aggregateMethod = "sum",
    costTypeConceptIds = c("44818668", "44818669", "44818668", "44818669"),
    stringsAsFactors = FALSE
  )
  analyses <- rbind(analyses, costTypeAnalyses)
  
  if (includeUtilization) {
    utilizationAnalyses <- data.frame(
      analysisId = 31:36,
      analysisName = c(
        "Any costs 365d prior",
        "Any medical costs 365d prior",
        "Any pharmacy costs 365d prior",
        "High cost patient (>$10k) 365d prior",
        "High cost patient (>$50k) 365d prior",
        "High cost patient (>$100k) 365d prior"
      ),
      domainIds = c("", "Visit,Procedure,Measurement,Observation", "Drug", "", "", ""),
      startDay = -365,
      endDay = -1,
      isBinary = c(TRUE, TRUE, TRUE, FALSE, FALSE, FALSE),
      missingMeansZero = TRUE,
      aggregateMethod = c("max", "max", "max", "sum", "sum", "sum"),
      threshold = c(NA, NA, NA, 10000, 50000, 100000),
      stringsAsFactors = FALSE
    )
    analyses <- rbind(analyses, utilizationAnalyses)
  }
  
  if (includeDistribution) {
    distributionAnalyses <- data.frame(
      analysisId = 41:46,
      analysisName = c(
        "Cost variance 365d prior",
        "Cost coefficient of variation 365d prior",
        "Cost 25th percentile 365d prior",
        "Cost median 365d prior",
        "Cost 75th percentile 365d prior",
        "Cost 95th percentile 365d prior"
      ),
      domainIds = "",
      startDay = -365,
      endDay = -1,
      isBinary = FALSE,
      missingMeansZero = FALSE,
      aggregateMethod = c("variance", "cv", "p25", "median", "p75", "p95"),
      stringsAsFactors = FALSE
    )
    analyses <- rbind(analyses, distributionAnalyses)
  }
  
  return(analyses)
}