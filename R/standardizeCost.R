#' Standardize Covariate Costs for Inflation
#'
#' @description
#' Adjusts the `covariateValue` for cost-based covariates in a `CovariateData`
#' object to a standard year's currency value. This function operates on
#' person-level (`aggregated = FALSE`) data.
#'
#' @details
#' This function takes a `CovariateData` object and adjusts the cost values using a
#' provided inflation table. It requires the original cohort data frame to link each
#' `rowId` to its corresponding `cohortStartDate` to determine the correct year for
#' inflation adjustment. It returns a new `CovariateData` object with the adjusted values.
#'
#' @param covariateData   A `CovariateData` object created with `aggregated = FALSE`.
#' @param cohort          A data frame containing the cohort information used to generate
#'                        the `covariateData`. Must contain the `rowIdField` used in the
#'                        call to `getDbCovariateData` and the `cohortStartDate`.
#' @param inflationTable  A data frame with at least two columns: 'year' and 'inflationFactor'.
#'                        The factor should represent the multiplier to convert a cost from
#'                        that 'year' to the target standard year's value.
#'
#' @return
#' A new `CovariateData` object with the cost values adjusted for inflation.
#'
#' @examples
#' \dontrun{
#' # Assume `covData` is a CovariateData object from getDbCovariateData(..., aggregated = FALSE)
#' # Assume `myCohort` is the data frame used for the cohort table
#'
#' # Create a sample inflation table (e.g., to standardize to 2021 costs)
#' # Using Consumer Price Index for Medical Care (CUUR0000SAM) from FRED
#' inflation <- data.frame(
#'   year = c(2018, 2019, 2020, 2021),
#'   cpi = c(497.6, 517.9, 524.3, 540.8) # Example values
#' ) %>%
#'   dplyr::mutate(inflationFactor = max(cpi) / cpi)
#'
#' adjustedCovs <- standardizeCost(
#'   covariateData = covData,
#'   cohort = myCohort,
#'   inflationTable = inflation
#' )
#' }
#'
#' @export
standardizeCost <- function(covariateData, cohort, inflationTable) {
  # --- Input Validation ---
  checkmate::assertClass(covariateData, "CovariateData")
  checkmate::assertDataFrame(cohort)
  checkmate::assertDataFrame(inflationTable)
  
  # Check for person-level data
  if (!"covariates" %in% Andromeda::listAndromedaTables(covariateData)) {
    stop("`covariateData` object must contain person-level data (`covariates` table). Please run with `aggregated = FALSE`.")
  }
  
  # Check for required columns in the inflation table
  checkmate::assertNames(names(inflationTable), must.include = c("year", "inflationFactor"))
  
  # Get the rowIdField from the original call's metadata
  metaData <- attr(covariateData, "metaData")
  rowIdField <- as.character(metaData$call$rowIdField)
  if (is.null(rowIdField) || length(rowIdField) == 0) {
    stop("Could not determine `rowIdField` from CovariateData metadata. Was it generated correctly?")
  }
  
  # Check for required columns in the cohort table
  checkmate::assertNames(names(cohort), must.include = c(rowIdField, "cohortStartDate"))
  
  writeLines("Starting cost standardization.")
  
  # --- Prepare Mapping Tables ---
  # Create a mapping from rowId to cohort year
  writeLines(paste("Mapping row ID from field:", rowIdField))
  rowIdToYear <- cohort %>%
    dplyr::select(
      rowId = !!rlang::sym(rowIdField),
      .data$cohortStartDate
    ) %>%
    dplyr::mutate(year = as.integer(format(.data$cohortStartDate, "%Y"))) %>%
    dplyr::select(.data$rowId, .data$year)
  
  # --- Adjust Costs ---
  writeLines("Joining covariate data with inflation factors and adjusting values.")
  
  # Join covariates with their cohort year, then with the inflation factor
  adjustedCovariates <- covariateData$covariates %>%
    dplyr::left_join(rowIdToYear, by = "rowId") %>%
    dplyr::left_join(inflationTable, by = "year") %>%
    # If a year has no inflation factor, default to 1 (no change) and warn user
    dplyr::mutate(inflationFactor = ifelse(is.na(.data$inflationFactor), 1, .data$inflationFactor)) %>%
    # Apply the adjustment
    dplyr::mutate(covariateValue = .data$covariateValue * .data$inflationFactor) %>%
    # Select original columns to maintain the standard format
    dplyr::select(.data$rowId, .data$covariateId, .data$covariateValue)
  
  # --- Create New CovariateData Object ---
  writeLines("Creating new CovariateData object with adjusted costs.")
  
  newCovariateData <- Andromeda::andromeda()
  newCovariateData$covariates <- adjustedCovariates
  
  # Copy over the reference tables and other data
  for (tableName in Andromeda::listAndromedaTables(covariateData)) {
    if (tableName != "covariates") {
      newCovariateData[[tableName]] <- covariateData[[tableName]]
    }
  }
  
  # Update metadata to reflect the change
  newMetaData <- metaData
  newMetaData$isStandardized <- TRUE
  newMetaData$standardizationDate <- Sys.time()
  
  attr(newCovariateData, "metaData") <- newMetaData
  class(newCovariateData) <- "CovariateData"
  
  writeLines("Cost standardization complete.")
  return(newCovariateData)
}