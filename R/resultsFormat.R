#' Convert Cost Analysis Results to FeatureExtraction CovariateData Format
#'
#' @description
#' Transforms cost analysis results from `calculateCostOfCare()` into a standardized
#' `CovariateData` object compatible with the FeatureExtraction package. This enables
#' seamless integration with OHDSI study pipelines and provides standardized
#' metadata, covariate references, and analysis references.
#'
#' @param costResults A list returned by `calculateCostOfCare()` containing `results` and `diagnostics`.
#' @param costOfCareSettings The `CostOfCareSettings` object used in the analysis.
#' @param cohortId The cohort ID analyzed.
#' @param databaseId Optional database identifier for multi-database studies.
#' @param analysisId Optional analysis ID for tracking multiple analyses.
#'
#' @return A `CovariateData` object with Andromeda backend containing:
#'   - `covariates`: Person-level cost and utilization metrics
#'   - `covariateRef`: Reference table describing each covariate
#'   - `analysisRef`: Reference table describing analysis parameters
#'   - `timeRef`: Reference table for time windows (if applicable)
#'   - `metaData`: Analysis metadata and settings
#'
#' @export
#' @examples
#' \dontrun{
#' # Run cost analysis
#' settings <- createCostOfCareSettings(costConceptId = 31973L)
#' results <- calculateCostOfCare(
#'   connection = connection,
#'   cdmDatabaseSchema = "main",
#'   cohortDatabaseSchema = "main", 
#'   cohortTable = "cohort",
#'   cohortId = 1,
#'   costOfCareSettings = settings
#' )
#' 
#' # Convert to FeatureExtraction format
#' covariateData <- createCostCovariateData(
#'   costResults = results,
#'   costOfCareSettings = settings,
#'   cohortId = 1
#' )
#' 
#' # Use with FeatureExtraction functions
#' summary(covariateData)
#' }
createCostCovariateData <- function(costResults,
                                   costOfCareSettings,
                                   cohortId,
                                   databaseId = "Unknown",
                                   analysisId = 1L) {
  
  # Validate inputs
  checkmate::assertClass(costResults, "Andromeda")
  checkmate::assertClass(costOfCareSettings, "CostOfCareSettings")
  checkmate::assertIntegerish(cohortId, len = 1)
  checkmate::assertCharacter(databaseId, len = 1)
  checkmate::assertIntegerish(analysisId, len = 1)
  
  # Create Andromeda object
  covariateData <- Andromeda::andromeda()
  
  # Generate analysis metadata
  analysisRef <- createAnalysisRef(costOfCareSettings, analysisId)
  
  # Generate covariate reference table
  covariateRef <- createCovariateRef(costResults$results, analysisRef, costOfCareSettings)
  
  # Generate covariates (person-level data)
  covariates <- createCovariatesTable(costResults$results, covariateRef, cohortId)
  
  # Generate time reference (if applicable)
  timeRef <- createTimeRef(costOfCareSettings)
  
  # Create metadata
  metaData <- createMetaData(costOfCareSettings, costResults, databaseId, analysisId)
  
  # Store in Andromeda
  covariateData$covariates <- covariates
  covariateData$covariateRef <- covariateRef
  covariateData$analysisRef <- analysisRef
  
  if (!is.null(timeRef)) {
    covariateData$timeRef <- timeRef
  }
  
  # Add metadata as attributes
  attr(covariateData, "metaData") <- metaData
  class(covariateData) <- c("CovariateData", class(covariateData))
  
  return(covariateData)
}

#' Create Analysis Reference Table
#'
#' @description
#' Creates a standardized analysis reference table describing the cost analysis parameters.
#'
#' @param costOfCareSettings The settings object used in the analysis.
#' @param analysisId The analysis ID.
#'
#' @return A tibble with analysis reference information.
#' @noRd
createAnalysisRef <- function(costOfCareSettings, analysisId) {
  
  # Determine analysis type based on settings
  analysisName <- if (isTRUE(costOfCareSettings$microCosting)) {
    "Cost Analysis - Line Level"
  } else {
    "Cost Analysis - Visit Level"
  }
  
  # Create domain description
  domainDescription <- if (!is.null(costOfCareSettings$eventFilters) && length(costOfCareSettings$eventFilters) > 0) {
    domains <- purrr::map_chr(costOfCareSettings$eventFilters, ~ .x$domain)
    paste("Filtered by domains:", paste(unique(domains), collapse = ", "))
  } else {
    "All clinical domains"
  }
  
  # Create time window description
  timeWindow <- sprintf(
    "Days %d to %d relative to %s",
    costOfCareSettings$startOffsetDays %||% 0L,
    costOfCareSettings$endOffsetDays %||% 365L,
    costOfCareSettings$anchorCol %||% "cohort_start_date"
  )
  
  # Cost concept description
  costConceptDesc <- switch(
    as.character(costOfCareSettings$costConceptId %||% 31973L),
    "31973" = "Total Charge",
    "31985" = "Total Cost", 
    "31980" = "Paid by Payer",
    "31981" = "Paid by Patient",
    "31974" = "Patient Copay",
    "31975" = "Patient Coinsurance",
    "31976" = "Patient Deductible",
    "31979" = "Amount Allowed",
    paste("Cost Concept ID", costOfCareSettings$costConceptId)
  )
  
  dplyr::tibble(
    analysisId = as.integer(analysisId),
    analysisName = analysisName,
    domainId = "Cost",
    startDay = as.integer(costOfCareSettings$startOffsetDays %||% 0L),
    endDay = as.integer(costOfCareSettings$endOffsetDays %||% 365L),
    isBinary = "N",
    missingMeansZero = "Y",
    description = paste(
      costConceptDesc, "analysis.",
      domainDescription, ".",
      timeWindow, ".",
      if (costOfCareSettings$cpiAdjustment) "CPI adjusted." else "Not CPI adjusted."
    )
  )
}

#' Create Covariate Reference Table
#'
#' @description
#' Creates a reference table describing each covariate generated from the cost analysis.
#'
#' @param results The results tibble from cost analysis.
#' @param analysisRef The analysis reference table.
#' @param costOfCareSettings The settings object.
#'
#' @return A tibble with covariate reference information.
#' @noRd
createCovariateRef <- function(results, analysisRef, costOfCareSettings) {
  
  # Base covariate ID (using analysis ID * 1000 as base)
  baseId <- analysisRef$analysisId * 1000L
  
  # Determine cost type for naming
  costType <- switch(
    as.character(costOfCareSettings$costConceptId %||% 31973L),
    "31973" = "charge",
    "31985" = "cost", 
    "31980" = "payer_payment",
    "31981" = "patient_payment",
    "31974" = "copay",
    "31975" = "coinsurance", 
    "31976" = "deductible",
    "31979" = "allowed_amount",
    "cost"
  )
  
  # Create covariates based on available metrics
  covariateRefs <- list()
  
  # Total cost/charge
  if ("totalCost" %in% names(results) || "total_cost" %in% names(results)) {
    covariateRefs[[length(covariateRefs) + 1]] <- dplyr::tibble(
      covariateId = baseId + 1L,
      covariateName = paste0("Total ", stringr::str_to_title(costType)),
      analysisId = analysisRef$analysisId,
      conceptId = as.integer(costOfCareSettings$costConceptId %||% 31973L)
    )
  }
  
  # CPI adjusted cost (if available)
  if ("totalAdjustedCost" %in% names(results) || "total_adjusted_cost" %in% names(results)) {
    covariateRefs[[length(covariateRefs) + 1]] <- dplyr::tibble(
      covariateId = baseId + 2L,
      covariateName = paste0("Total ", stringr::str_to_title(costType), " (CPI Adjusted)"),
      analysisId = analysisRef$analysisId,
      conceptId = as.integer(costOfCareSettings$costConceptId %||% 31973L)
    )
  }
  
  # Per-person-per-month (PPPM)
  if ("costPppm" %in% names(results) || "cost_pppm" %in% names(results)) {
    covariateRefs[[length(covariateRefs) + 1]] <- dplyr::tibble(
      covariateId = baseId + 3L,
      covariateName = paste0(stringr::str_to_title(costType), " Per Person Per Month"),
      analysisId = analysisRef$analysisId,
      conceptId = as.integer(costOfCareSettings$costConceptId %||% 31973L)
    )
  }
  
  # Per-person-per-year (PPPY)
  if ("costPppy" %in% names(results) || "cost_pppy" %in% names(results)) {
    covariateRefs[[length(covariateRefs) + 1]] <- dplyr::tibble(
      covariateId = baseId + 4L,
      covariateName = paste0(stringr::str_to_title(costType), " Per Person Per Year"),
      analysisId = analysisRef$analysisId,
      conceptId = as.integer(costOfCareSettings$costConceptId %||% 31973L)
    )
  }
  
  # Utilization metrics
  if ("distinctVisits" %in% names(results) || "distinct_visits" %in% names(results)) {
    covariateRefs[[length(covariateRefs) + 1]] <- dplyr::tibble(
      covariateId = baseId + 10L,
      covariateName = "Number of Visits",
      analysisId = analysisRef$analysisId,
      conceptId = 0L
    )
  }
  
  if ("distinctEvents" %in% names(results) || "distinct_events" %in% names(results)) {
    covariateRefs[[length(covariateRefs) + 1]] <- dplyr::tibble(
      covariateId = baseId + 11L,
      covariateName = "Number of Events",
      analysisId = analysisRef$analysisId,
      conceptId = 0L
    )
  }
  
  # Events per 1000 person-years
  if ("eventsPer1000Py" %in% names(results) || "events_per_1000_py" %in% names(results)) {
    covariateRefs[[length(covariateRefs) + 1]] <- dplyr::tibble(
      covariateId = baseId + 12L,
      covariateName = "Events Per 1000 Person-Years",
      analysisId = analysisRef$analysisId,
      conceptId = 0L
    )
  }
  
  # Combine all covariate references
  if (length(covariateRefs) == 0) {
    # Fallback if no standard metrics found
    covariateRefs[[1]] <- dplyr::tibble(
      covariateId = baseId + 1L,
      covariateName = "Total Cost",
      analysisId = analysisRef$analysisId,
      conceptId = as.integer(costOfCareSettings$costConceptId %||% 31973L)
    )
  }
  
  dplyr::bind_rows(covariateRefs)
}

#' Create Covariates Table (Person-Level Data)
#'
#' @description
#' Creates the main covariates table with person-level cost and utilization metrics.
#' Since the current results are aggregated, this creates a single row representing
#' the cohort-level metrics that can be applied to all persons in the cohort.
#'
#' @param results The results tibble from cost analysis.
#' @param covariateRef The covariate reference table.
#' @param cohortId The cohort ID.
#'
#' @return A tibble with person-level covariate data.
#' @noRd
createCovariatesTable <- function(results, covariateRef, cohortId) {
  
  # Since current results are aggregated at cohort level, we create template
  # that can be expanded to person level in future versions
  
  # Create base structure
  covariates <- purrr::map_dfr(seq_len(nrow(covariateRef)), function(i) {
    ref <- covariateRef[i, ]
    
    # Extract value based on covariate name pattern
    value <- extractCovariateValue(results, ref$covariateName, ref$covariateId)
    
    dplyr::tibble(
      rowId = as.integer(cohortId),  # Using cohortId as rowId for now
      covariateId = ref$covariateId,
      covariateValue = as.numeric(value)
    )
  })
  
  # Filter out zero/NA values if needed
  covariates <- covariates |>
    dplyr::filter(!is.na(.data$covariateValue) & .data$covariateValue != 0)
  
  return(covariates)
}

#' Extract Covariate Value from Results
#'
#' @description
#' Helper function to extract the appropriate value from results based on covariate name.
#'
#' @param results The results tibble.
#' @param covariateName The name of the covariate.
#' @param covariateId The covariate ID for fallback.
#'
#' @return The numeric value for the covariate.
#' @noRd
extractCovariateValue <- function(results, covariateName, covariateId) {
  
  # Normalize column names to handle both camelCase and snake_case
  resultsNorm <- results |>
    dplyr::rename_all(~ stringr::str_to_lower(stringr::str_replace_all(.x, "([A-Z])", "_\\1"))) |>
    dplyr::rename_all(~ stringr::str_replace_all(.x, "^_", ""))
  
  # Extract value based on covariate name pattern
  value <- switch(
    stringr::str_to_lower(covariateName),
    
    # Cost metrics
    "total charge" = resultsNorm$total_cost[1] %||% 0,
    "total cost" = resultsNorm$total_cost[1] %||% 0,
    "total payer_payment" = resultsNorm$total_cost[1] %||% 0,
    "total patient_payment" = resultsNorm$total_cost[1] %||% 0,
    "total copay" = resultsNorm$total_cost[1] %||% 0,
    "total coinsurance" = resultsNorm$total_cost[1] %||% 0,
    "total deductible" = resultsNorm$total_cost[1] %||% 0,
    "total allowed_amount" = resultsNorm$total_cost[1] %||% 0,
    
    # CPI adjusted
    "total charge (cpi adjusted)" = resultsNorm$total_adjusted_cost[1] %||% 0,
    "total cost (cpi adjusted)" = resultsNorm$total_adjusted_cost[1] %||% 0,
    
    # PPPM/PPPY
    "charge per person per month" = resultsNorm$cost_pppm[1] %||% 0,
    "cost per person per month" = resultsNorm$cost_pppm[1] %||% 0,
    "charge per person per year" = resultsNorm$cost_pppy[1] %||% 0,
    "cost per person per year" = resultsNorm$cost_pppy[1] %||% 0,
    
    # Utilization
    "number of visits" = resultsNorm$distinct_visits[1] %||% 0,
    "number of events" = resultsNorm$distinct_events[1] %||% 0,
    "events per 1000 person-years" = resultsNorm$events_per_1000_py[1] %||% 0,
    
    # Fallback
    0
  )
  
  return(as.numeric(value))
}

#' Create Time Reference Table
#'
#' @description
#' Creates a time reference table for time-windowed analyses.
#'
#' @param costOfCareSettings The settings object.
#'
#' @return A tibble with time reference information, or NULL if not applicable.
#' @noRd
createTimeRef <- function(costOfCareSettings) {
  
  # Only create time reference if we have meaningful time windows
  startDay <- costOfCareSettings$startOffsetDays %||% 0L
  endDay <- costOfCareSettings$endOffsetDays %||% 365L
  
  if (startDay == 0L && endDay == 365L) {
    return(NULL)  # Standard 1-year window, no need for special reference
  }
  
  dplyr::tibble(
    timeId = 1L,
    startDay = as.integer(startDay),
    endDay = as.integer(endDay),
    description = sprintf("Days %d to %d", startDay, endDay)
  )
}

#' Create Metadata for CovariateData
#'
#' @description
#' Creates comprehensive metadata describing the cost analysis.
#'
#' @param costOfCareSettings The settings object.
#' @param costResults The results from cost analysis.
#' @param databaseId The database identifier.
#' @param analysisId The analysis ID.
#'
#' @return A list with metadata information.
#' @noRd
createMetaData <- function(costOfCareSettings, costResults, databaseId, analysisId) {
  
  list(
    # Basic identifiers
    databaseId = databaseId,
    analysisId = analysisId,
    
    # Package and version info
    packageName = "CostUtilization",
    packageVersion = utils::packageVersion("CostUtilization"),
    
    # Analysis parameters
    call = list(
      anchorCol = costOfCareSettings$anchorCol %||% "cohort_start_date",
      startOffsetDays = costOfCareSettings$startOffsetDays %||% 0L,
      endOffsetDays = costOfCareSettings$endOffsetDays %||% 365L,
      costConceptId = costOfCareSettings$costConceptId %||% 31973L,
      currencyConceptId = costOfCareSettings$currencyConceptId,
      microCosting = costOfCareSettings$microCosting %||% FALSE,
      cpiAdjustment = costOfCareSettings$cpiAdjustment %||% FALSE,
      hasEventFilters = costOfCareSettings$hasEventFilters %||% FALSE,
      hasVisitRestriction = costOfCareSettings$hasVisitRestriction %||% FALSE
    ),
    
    # Results summary
    populationSize = costResults$results$nPersonsWithCost[1] %||% 0L,
    metricType = costResults$results$metricType[1] %||% "unknown",
    
    # Timing
    executionTime = Sys.time(),
    
    # Data characteristics
    isBinary = FALSE,
    isAggregated = TRUE,
    
    # Diagnostics summary
    diagnosticsAvailable = !is.null(costResults$diagnostics),
    nDiagnosticSteps = if (!is.null(costResults$diagnostics)) nrow(costResults$diagnostics) else 0L
  )
}

#' Summary Method for CostCovariateData
#'
#' @description
#' Provides a summary of the cost covariate data object.
#'
#' @param object A `CovariateData` object created by `createCostCovariateData()`.
#' @param ... Additional arguments (not used).
#'
#' @return A summary object.
#' @export
summary.CovariateData <- function(object, ...) {
  
  if (!inherits(object, "CovariateData")) {
    stop("Object must be of class 'CovariateData'")
  }
  
  metaData <- attr(object, "metaData")
  
  # Count covariates
  nCovariates <- if ("covariateRef" %in% names(object)) {
    nrow(dplyr::collect(object$covariateRef))
  } else {
    0L
  }
  
  nPersons <- if ("covariates" %in% names(object)) {
    length(unique(dplyr::pull(dplyr::collect(object$covariates), "rowId")))
  } else {
    0L
  }
  
  cat("Cost Covariate Data Summary\n")
  cat("===========================\n\n")
  
  if (!is.null(metaData)) {
    cat("Database ID:", metaData$databaseId, "\n")
    cat("Analysis ID:", metaData$analysisId, "\n")
    cat("Package:", metaData$packageName, "v", as.character(metaData$packageVersion), "\n")
    cat("Metric Type:", metaData$metricType, "\n")
    cat("Population Size:", metaData$populationSize, "\n\n")
    
    cat("Analysis Parameters:\n")
    cat("- Time Window:", metaData$call$startOffsetDays, "to", metaData$call$endOffsetDays, "days\n")
    cat("- Anchor Column:", metaData$call$anchorCol, "\n")
    cat("- Cost Concept ID:", metaData$call$costConceptId, "\n")
    cat("- Micro Costing:", metaData$call$microCosting, "\n")
    cat("- CPI Adjustment:", metaData$call$cpiAdjustment, "\n")
    cat("- Event Filters:", metaData$call$hasEventFilters, "\n")
    cat("- Visit Restrictions:", metaData$call$hasVisitRestriction, "\n\n")
  }
  
  cat("Data Summary:\n")
  cat("- Number of Covariates:", nCovariates, "\n")
  cat("- Number of Persons:", nPersons, "\n")
  
  if ("covariateRef" %in% names(object)) {
    cat("\nAvailable Covariates:\n")
    covRef <- dplyr::collect(object$covariateRef)
    for (i in seq_len(nrow(covRef))) {
      cat(sprintf("  %d: %s\n", covRef$covariateId[i], covRef$covariateName[i]))
    }
  }
  
  invisible(object)
}

#' Print Method for CostCovariateData
#'
#' @description
#' Print method for cost covariate data objects.
#'
#' @param x A `CovariateData` object.
#' @param ... Additional arguments (not used).
#'
#' @return The object invisibly.
#' @export
print.CovariateData <- function(x, ...) {
  summary(x, ...)
  invisible(x)
}

#' Convert Legacy Results to FeatureExtraction Format
#'
#' @description
#' Convenience function to convert existing cost analysis results to the
#' FeatureExtraction format. This is useful for updating existing workflows.
#'
#' @param costResults Results from `calculateCostOfCare()`.
#' @param costOfCareSettings Settings used in the analysis.
#' @param cohortId Cohort ID analyzed.
#' @param databaseId Database identifier.
#'
#' @return A `CovariateData` object.
#' @export
convertToFeatureExtractionFormat <- function(costResults,
                                            costOfCareSettings,
                                            cohortId,
                                            databaseId = "Unknown") {
  
  cli::cli_alert_info("Converting cost analysis results to FeatureExtraction format...")
  
  result <- createCostCovariateData(
    costResults = costResults,
    costOfCareSettings = costOfCareSettings,
    cohortId = cohortId,
    databaseId = databaseId
  )
  
  cli::cli_alert_success("Conversion completed successfully!")
  
  return(result)
}