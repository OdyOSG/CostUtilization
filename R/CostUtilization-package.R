#' CostUtilization: A Package for Healthcare Cost and Utilization Analysis
#'
#' @description
#' The CostUtilization package provides tools for analyzing healthcare costs and 
#' utilization patterns using OMOP CDM data. It supports cohort-based cost analysis,
#' event filtering, micro-costing at the visit detail level, and comprehensive
#' utilization metrics.
#'
#' @section Main Functions:
#' \itemize{
#'   \item \code{\link{calculateCostOfCare}}: Performs cost-of-care analysis for a specified cohort
#'   \item \code{\link{injectCostData}}: Injects synthetic cost data for testing (Eunomia)
#' }
#'
#' @section Key Features:
#' \itemize{
#'   \item Flexible time window analysis around cohort entry or exit
#'   \item Support for visit-level and visit-detail-level (micro) costing
#'   \item Event filtering by concept sets and domains
#'   \item Calculation of per-member-per-month (PPPM) costs
#'   \item Utilization rates per 1000 person-years
#'   \item Multi-database support via DatabaseConnector
#' }
#'
#' @section Database Requirements:
#' \itemize{
#'   \item OMOP CDM version 5.3 or higher
#'   \item Cost table populated with cost data
#'   \item Visit detail table required for micro-costing
#' }
#'
#' @docType package
#' @name CostUtilization-package
#' @aliases CostUtilization
#'
#' @import DatabaseConnector
#' @import SqlRender
#' @import dplyr
#' @importFrom Andromeda andromeda close
#' @importFrom checkmate assertClass assertCharacter assertIntegerish assertLogical assertList assertChoice assertNames
#' @importFrom cli cli_abort cli_alert cli_alert_danger cli_alert_info cli_alert_success cli_alert_warning cli_h2 cli_h3 cli_ul cli_li cli_end cli_text cli_inform cli_warn
#' @importFrom purrr map map_chr map_dfr imap_dfr iwalk
#' @importFrom rlang arg_match .data := %||%
#' @importFrom stats runif
#' @importFrom utils packageVersion
NULL

#' Native pipe operator
#'
#' @name |>
#' @rdname native-pipe
#' @keywords internal
#' @usage lhs |> rhs
NULL

# Package startup message
.onAttach <- function(libname, pkgname) {
  packageStartupMessage(
    sprintf(
      "CostUtilization v%s - Healthcare Cost and Utilization Analysis\n",
      utils::packageVersion(pkgname)
    ),
    "Type ?CostUtilization for help"
  )
}

# Package environment for storing package-level variables
.pkgenv <- new.env(parent = emptyenv())

#' Get Default Cost Concept IDs
#'
#' @description
#' Returns commonly used cost concept IDs for reference
#'
#' @return Named list of cost concept IDs
#' @export
getDefaultCostConceptIds <- function() {
  list(
    # Cost type concepts
    totalCharge = 31978L,
    totalCost = 31980L,
    totalPaid = 31979L,
    paidByPayer = 31981L,
    paidByPatient = 31982L,
    
    # Currency concepts
    usd = 44818668L,
    eur = 44818669L,
    gbp = 44818670L,
    
    # Cost domains
    visitCost = 5032L,
    procedureCost = 5033L,
    drugCost = 5034L,
    deviceCost = 5035L
  )
}

#' Get Supported Database Dialects
#'
#' @description
#' Returns a vector of supported database dialects
#'
#' @return Character vector of supported dialects
#' @export
getSupportedDialects <- function() {
  c("oracle", "postgresql", "pdw", "redshift", "impala", 
    "netezza", "bigquery", "sql server", "spark", "snowflake")
}

#' Create Example Event Filters
#'
#' @description
#' Creates example event filters for common use cases
#'
#' @param type Type of example filters: "diabetes", "cardiovascular", "oncology"
#'
#' @return List of event filters
#' @export
#' @examples
#' # Get diabetes-related event filters
#' diabetesFilters <- createExampleEventFilters("diabetes")
#' 
#' # Use in analysis
#' \dontrun{
#' results <- calculateCostOfCare(
#'   connection = connection,
#'   cdmDatabaseSchema = "cdm",
#'   cohortDatabaseSchema = "results",
#'   cohortTable = "cohort",
#'   cohortId = 1,
#'   eventFilters = diabetesFilters
#' )
#' }
createExampleEventFilters <- function(type = c("diabetes", "cardiovascular", "oncology")) {
  type <- rlang::arg_match(type)
  
  filters <- switch(type,
                    diabetes = list(
                      list(
                        name = "Diabetes Diagnoses",
                        domain = "Condition",
                        conceptIds = c(201820L, 201826L, 443238L, 442793L) # Type 2 diabetes concepts
                      ),
                      list(
                        name = "Diabetes Medications",
                        domain = "Drug",
                        conceptIds = c(1503297L, 1502826L, 1502855L, 1510202L) # Metformin, insulin, etc.
                      ),
                      list(
                        name = "Diabetes Labs",
                        domain = "Measurement",
                        conceptIds = c(3004501L, 3003309L, 40758583L) # HbA1c, glucose
                      )
                    ),
                    
                    cardiovascular = list(
                      list(
                        name = "CV Conditions",
                        domain = "Condition",
                        conceptIds = c(316866L, 321318L, 316139L, 381591L) # MI, CHF, AFib, Stroke
                      ),
                      list(
                        name = "CV Procedures",
                        domain = "Procedure",
                        conceptIds = c(4336464L, 4178904L, 4296653L) # PCI, CABG, Cath
                      ),
                      list(
                        name = "CV Medications",
                        domain = "Drug",
                        conceptIds = c(1308216L, 1310149L, 1341927L, 1363749L) # Statins, BB, ACE/ARB
                      )
                    ),
                    
                    oncology = list(
                      list(
                        name = "Cancer Diagnoses",
                        domain = "Condition",
                        conceptIds = c(4089661L, 4096682L, 4111921L, 4112853L) # Various cancers
                      ),
                      list(
                        name = "Chemotherapy",
                        domain = "Drug",
                        conceptIds = c(35603348L, 35603365L, 35603392L) # Chemo drugs
                      ),
                      list(
                        name = "Radiation Therapy",
                        domain = "Procedure",
                        conceptIds = c(4030195L, 4036018L, 4202832L) # Radiation procedures
                      )
                    )
  )
  
  return(filters)
}

#' Format Cost Results
#'
#' @description
#' Formats cost analysis results for presentation
#'
#' @param results Results tibble from calculateCostOfCare
#' @param digits Number of decimal places for numeric values
#' @param currency Currency symbol to use
#'
#' @return Formatted tibble
#' @export
formatCostResults <- function(results, digits = 2, currency = "$") {
  checkmate::assertClass(results, "data.frame")
  checkmate::assertInt(digits, lower = 0)
  checkmate::assertString(currency)
  
  results |>
    dplyr::mutate(
      total_cost_formatted = paste0(currency, formatC(.data$total_cost, 
                                                      format = "f", 
                                                      digits = digits, 
                                                      big.mark = ",")),
      cost_pppm_formatted = paste0(currency, formatC(.data$cost_pppm, 
                                                     format = "f", 
                                                     digits = digits, 
                                                     big.mark = ",")),
      visits_per_1000_py = round(.data$visits_per_1000_py, digits),
      visit_dates_per_1000_py = round(.data$visit_dates_per_1000_py, digits),
      visit_details_per_1000_py = round(.data$visit_details_per_1000_py, digits)
    )
}

#' Validate OMOP CDM Tables
#'
#' @description
#' Validates that required OMOP CDM tables exist for cost analysis
#'
#' @param connection DatabaseConnector connection
#' @param cdmDatabaseSchema CDM schema name
#' @param requireCostTable Require cost table to exist
#' @param requireVisitDetail Require visit_detail table for micro-costing
#'
#' @return Invisible TRUE if validation passes
#' @export
validateOmopCdmTables <- function(connection, 
                                  cdmDatabaseSchema,
                                  requireCostTable = TRUE,
                                  requireVisitDetail = FALSE) {
  
  checkmate::assertClass(connection, "DatabaseConnection")
  checkmate::assertString(cdmDatabaseSchema)
  
  # Required tables for any analysis
  requiredTables <- c(
    "person",
    "observation_period", 
    "visit_occurrence",
    "condition_occurrence",
    "procedure_occurrence",
    "drug_exposure",
    "measurement",
    "observation"
  )
  
  # Conditionally required tables
  if (requireCostTable) {
    requiredTables <- c(requiredTables, "cost")
  }
  
  if (requireVisitDetail) {
    requiredTables <- c(requiredTables, "visit_detail")
  }
  
  # Check each table
  missingTables <- character()
  
  for (table in requiredTables) {
    sql <- SqlRender::render(
      "SELECT 1 FROM @cdm_database_schema.@table WHERE 1 = 0",
      cdm_database_schema = cdmDatabaseSchema,
      table = table
    )
    
    tableExists <- tryCatch(
      {
        DatabaseConnector::querySql(connection, sql)
        TRUE
      },
      error = function(e) FALSE
    )
    
    if (!tableExists) {
      missingTables <- c(missingTables, table)
    }
  }
  
  # Report results
  if (length(missingTables) > 0) {
    cli::cli_abort(
      "Required OMOP CDM tables missing in schema '{cdmDatabaseSchema}': {paste(missingTables, collapse = ', ')}"
    )
  }
  
  cli::cli_alert_success("All required OMOP CDM tables found in '{cdmDatabaseSchema}'")
  invisible(TRUE)
}

#' Get Package SQL Files
#'
#' @description
#' Lists available SQL files in the package
#'
#' @return Character vector of SQL file names
#' @export
getPackageSqlFiles <- function() {
  sqlDir <- system.file("sql", package = "CostUtilization")
  if (!dir.exists(sqlDir)) {
    return(character())
  }
  
  list.files(sqlDir, pattern = "\\.sql$", full.names = FALSE)
}

#' Create Cost Analysis Report
#'
#' @description
#' Creates a formatted report from cost analysis results
#'
#' @param results Results from calculateCostOfCare (list format)
#' @param outputFile Path to output file (HTML)
#' @param title Report title
#'
#' @return Invisible path to created report
#' @export
createCostAnalysisReport <- function(results, 
                                     outputFile = "cost_analysis_report.html",
                                     title = "Cost Analysis Report") {
  
  checkmate::assertList(results, names = "named")
  checkmate::assertString(outputFile)
  checkmate::assertString(title)
  
  # Check for required components
  if (!all(c("results", "diagnostics") %in% names(results))) {
    cli::cli_abort("Results must contain 'results' and 'diagnostics' components")
  }
  
  # Create simple HTML report
  html <- sprintf('
<!DOCTYPE html>
<html>
<head>
    <title>%s</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1, h2 { color: #333; }
        table { border-collapse: collapse; width: 100%%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .metadata { background-color: #f9f9f9; padding: 10px; margin: 20px 0; }
    </style>
</head>
<body>
    <h1>%s</h1>
    
    <div class="metadata">
        <h2>Analysis Metadata</h2>
        <p><strong>Analysis Time:</strong> %s</p>
        <p><strong>Package Version:</strong> %s</p>
    </div>
    
    <h2>Results Summary</h2>
    %s
    
    <h2>Diagnostics</h2>
    %s
</body>
</html>',
                  title,
                  title,
                  format(results$metadata$analysisTime, "%Y-%m-%d %H:%M:%S"),
                  as.character(packageVersion("CostUtilization")),
                  knitr::kable(results$results, format = "html", table.attr = "class='results-table'"),
                  knitr::kable(results$diagnostics, format = "html", table.attr = "class='diagnostics-table'")
  )
  
  # Write to file
  writeLines(html, outputFile)
  
  cli::cli_alert_success("Report created: {outputFile}")
  invisible(outputFile)
}

# Package constants
.packageConstants <- list(
  maxBatchSize = 1000L,
  defaultCurrency = "USD",
  defaultCostTypeConceptId = 31978L,
  defaultCurrencyConceptId = 44818668L
)

#' Get Package Constants
#'
#' @description
#' Returns package constants used internally
#'
#' @return List of package constants
#' @keywords internal
#' @export
getPackageConstants <- function() {
  .packageConstants
}