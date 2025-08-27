#' CostUtilization: Healthcare Cost and Utilization Analysis for OMOP CDM v5.5
#'
#' @description
#' The CostUtilization package provides a comprehensive framework for analyzing 
#' healthcare costs and resource utilization using OMOP Common Data Model (CDM) 
#' databases. This version is specifically designed for **CDM v5.5** with enhanced 
#' support for the new long-format COST table structure.
#'
#' @section Key Features:
#' 
#' **CDM v5.5 Compatibility:**
#' - Full support for the new long-format COST table structure
#' - Enhanced temporal precision with `effective_date`, `billed_date`, `paid_date`
#' - Standardized cost concepts for consistent analysis
#' - Improved traceability between costs and clinical events
#' 
#' **Flexible Analysis Framework:**
#' - Time window-based cost analysis relative to cohort index dates
#' - Event-specific cost filtering across multiple clinical domains
#' - Multi-cohort comparative analysis with stratification
#' - Micro-costing at the visit detail level
#' 
#' **Modern R Implementation:**
#' - Settings-based API using `createCostOfCareSettings()`
#' - Integration with tidyverse packages (`dplyr`, `purrr`, `rlang`)
#' - Comprehensive diagnostic information and patient flow tracking
#' - Efficient batch processing for large-scale analyses
#'
#' @section Main Functions:
#' 
#' **Data Setup:**
#' - `injectCostData()`: Create synthetic cost data for testing with Eunomia
#' - `transformCostToCdmV5dot5()`: Transform wide-format cost tables to CDM v5.5 long format
#' 
#' **Analysis Configuration:**
#' - `createCostOfCareSettings()`: Create validated settings objects for analysis
#' 
#' **Cost Analysis:**
#' - `calculateCostOfCare()`: Perform comprehensive cost and utilization analysis
#'
#' @section CDM v5.5 Cost Table Structure:
#' 
#' The CDM v5.5 COST table uses a long format where each cost component 
#' (e.g., total_charge, paid_by_payer) is represented as a separate row:
#' 
#' \preformatted{
#' cost_id | person_id | cost_concept_id | cost_source_value | cost | effective_date
#'    1    |    123    |      31973      |  "total_charge"   | 1000 | 2023-01-15
#'    2    |    123    |      31980      |  "paid_by_payer"  | 800  | 2023-01-15
#'    3    |    123    |      31981      | "paid_by_patient" | 200  | 2023-01-15
#' }
#' 
#' **Key Cost Concept IDs:**
#' - 31973: Total charge
#' - 31985: Total cost  
#' - 31980: Paid by payer
#' - 31981: Paid by patient
#' - 31974: Patient copay
#' - 31975: Patient coinsurance
#' - 31976: Patient deductible
#' - 31979: Amount allowed
#'
#' @section Getting Started:
#' 
#' 1. **Setup test environment with Eunomia:**
#' \preformatted{
#' library(CostUtilization)
#' connectionDetails <- Eunomia::getEunomiaConnectionDetails()
#' connection <- DatabaseConnector::connect(connectionDetails)
#' 
#' # Inject and transform cost data to CDM v5.5
#' transformCostToCdmV5dot5(connectionDetails)
#' }
#' 
#' 2. **Create analysis settings:**
#' \preformatted{
#' settings <- createCostOfCareSettings(
#'   anchorCol = "cohort_start_date",
#'   startOffsetDays = 0L,
#'   endOffsetDays = 365L,
#'   costConceptId = 31973L  # Total charge
#' )
#' }
#' 
#' 3. **Run cost analysis:**
#' \preformatted{
#' results <- calculateCostOfCare(
#'   connection = connection,
#'   cdmDatabaseSchema = "main",
#'   cohortDatabaseSchema = "main", 
#'   cohortTable = "cohort",
#'   cohortId = 1,
#'   costOfCareSettings = settings
#' )
#' }
#'
#' @section Advanced Features:
#' 
#' **Event Filtering:**
#' \preformatted{
#' eventFilters <- list(
#'   list(
#'     name = "Diabetes Medications",
#'     domain = "Drug",
#'     conceptIds = c(1503297L, 1502826L)
#'   )
#' )
#' 
#' settings <- createCostOfCareSettings(
#'   anchorCol = "cohort_start_date",
#'   startOffsetDays = -365L,
#'   endOffsetDays = 365L,
#'   eventFilters = eventFilters
#' )
#' }
#' 
#' **Multi-Cohort Analysis:**
#' \preformatted{
#' results <- purrr::map_dfr(cohortIds, function(id) {
#'   calculateCostOfCare(
#'     connection = connection,
#'     cohortId = id,
#'     costOfCareSettings = settings
#'   )$results |>
#'   dplyr::mutate(cohortId = id)
#' })
#' }
#'
#' @section System Requirements:
#' - R (version 4.1.0 or higher)
#' - Java 8 or higher (for DatabaseConnector)
#' - Access to an OMOP CDM v5.5+ database
#' - COST table in long format (use `transformCostToCdmV5dot5()` if needed)
#'
#' @section Package Dependencies:
#' - DatabaseConnector: Database connectivity
#' - dplyr: Data manipulation
#' - purrr: Functional programming
#' - rlang: Tidy evaluation
#' - cli: User interface
#' - checkmate: Input validation
#' - glue: String interpolation
#'
#' @examples
#' \dontrun{
#' # Basic cost analysis workflow
#' library(CostUtilization)
#' 
#' # Setup
#' connectionDetails <- Eunomia::getEunomiaConnectionDetails()
#' transformCostToCdmV5dot5(connectionDetails)
#' 
#' # Create settings
#' settings <- createCostOfCareSettings(
#'   anchorCol = "cohort_start_date",
#'   startOffsetDays = 0L,
#'   endOffsetDays = 365L,
#'   costConceptId = 31973L
#' )
#' 
#' # Run analysis
#' results <- calculateCostOfCare(
#'   connectionDetails = connectionDetails,
#'   cdmDatabaseSchema = "main",
#'   cohortDatabaseSchema = "main",
#'   cohortTable = "cohort", 
#'   cohortId = 1,
#'   costOfCareSettings = settings
#' )
#' 
#' # View results
#' print(results$results)
#' print(results$diagnostics)
#' }
#'
#' @author OHDSI Collaborative
#' @seealso 
#' - Package website: \url{https://ohdsi.github.io/CostUtilization/}
#' - OHDSI forums: \url{https://forums.ohdsi.org/}
#' - CDM v5.5 documentation: \url{https://ohdsi.github.io/CommonDataModel/}
#'
#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @import dplyr
#' @import rlang
#' @importFrom purrr map_dfr map2_dfr pmap_dfr imap_dfr map_chr walk
#' @importFrom cli cli_alert_info cli_alert_success cli_alert_warning cli_h2 cli_h3
#' @importFrom checkmate assertClass assertChoice assertIntegerish assertFlag assertString assertInt makeAssertCollection
#' @importFrom glue glue
#' @importFrom DatabaseConnector connect disconnect querySql executeSql insertTable getTableNames dbms
#' @importFrom stringi stri_rand_strings
## usethis namespace: end

NULL