#' #' Inject Synthetic Payer Plan and Wide-Format Cost Data (Eunomia / OMOP CDM)
#' #'
#' #' @description
#' #' Generates synthetic payer plans (`payer_plan_period`) and a **wide-format** `cost`
#' #' table intended to simulate legacy v5.3/v5.4 inputs. This is useful for local testing
#' #' of downstream ETL that converts wide cost records to normalized long format (v5.5).
#' #'
#' #' @details
#' #' Workflow:
#' #' 1. Pulls `person_id` with observation-period bounds and derives `start_year`/`end_year`.
#' #' 2. Simulates **plan changes on January 1** with a bias toward continuous enrollment.
#' #' 3. Joins synthetic payer-plan segments to clinical **event dates** (Procedure, Measurement,
#' #'    Visit, Device, Drug, Observation, Condition) to assign `payer_plan_period_id`.
#' #' 4. Creates a **wide-format** `cost` table with numeric components (e.g., `total_charge`,
#' #'    `paid_by_payer`, etc.) to emulate pre-5.5 sources (no normalized `cost_concept_id` rows).
#' #'
#' #' Assumptions:
#' #' - Currency concept is set to **USD (44818668)** for all rows.
#' #' - `cost_type_concept_id` uses **5032 (Administrative record)** for all rows.
#' #' - Synthetic pricing is random but seeded; set `seed` for reproducibility.
#' #' - This function **overwrites** `payer_plan_period` and `cost` in the schema via
#' #'   `DatabaseConnector::insertTable(..., dropTableIfExists = TRUE)`.
#' #'
#' #' Compatibility:
#' #' - Designed for **Eunomia** (SQLite), but relies only on `insertTable()` and simple SQL selects.
#' #' - No temp-table or vendor-specific SQL is used here.
#' #'
#' #' @param connection A live DatabaseConnector connection.
#' #' @param seed Integer seed for reproducible randomization. Default: 123.
#' #' @param cdmDatabaseSchema Schema (or database) name containing OMOP CDM tables. Default: "main".
#' #'
#' #' @return
#' #' Returns the `connection`. Called primarily for its side effects (table inserts).
#' #'
#' #' @examples
#' #' \dontrun{
#' #' conn <- DatabaseConnector::connect(Eunomia::getEunomiaConnectionDetails())
#' #' injectCostData(conn)
#' #' }
#' injectCostData <- function(connection, seed = 123, cdmDatabaseSchema = "main") {
#'   cli::cli_alert_info("Generating synthetic payer_plan_period and wide-format cost data.")
#' 
#'   cli::cli_alert_info("- Step 1: Creating realistic payer_plan_period table.")
#' 
#'   # Fetch observation periods and calculate start/end years
#'   person_obs_periods <- DatabaseConnector::querySql(connection, glue::glue("SELECT person_id, observation_period_start_date, observation_period_end_date FROM {cdmDatabaseSchema}.observation_period")) |>
#'     dplyr::rename_with(tolower) |>
#'     dplyr::mutate(dplyr::across(dplyr::ends_with("_date"), as.Date)) |>
#'     # Determine the calendar years covered by the observation period
#'     dplyr::mutate(
#'       start_year = as.integer(format(observation_period_start_date, "%Y")),
#'       end_year = as.integer(format(observation_period_end_date, "%Y"))
#'     )
#' 
#'   set.seed(seed)
#' 
#'   # Define plan options for simulation
#'   plan_options <- c("PPO-Low-Deductible-500", "HMO-Standard-1500", "PPO-High-Deductible-2500", "EPO-Basic-3000")
#' 
#'   # Simulates plan changes on Jan 1 with a bias toward continuous enrollment
#'   generate_random_plans <- function(pid, start_date, end_date, start_year, end_year) {
#'     # Ensure inputs are Date
#'     start_date <- as.Date(start_date)
#'     end_date <- as.Date(end_date)
#' 
#'     possible_change_years <- (start_year + 1):end_year
#'     n_possible_changes <- length(possible_change_years)
#' 
#'     if (n_possible_changes == 0) {
#'       n_segments <- 1
#'       change_dates <- as.Date(character())
#'     } else {
#'       max_changes <- min(n_possible_changes, 2)
#'       probs <- c(0.6, 0.3, 0.1)
#'       current_probs <- probs[1:(max_changes + 1)]
#'       current_probs <- current_probs / sum(current_probs)
#'       num_changes <- sample(0:max_changes, 1, prob = current_probs)
#' 
#'       if (num_changes == 0) {
#'         n_segments <- 1
#'         change_dates <- as.Date(character())
#'       } else {
#'         n_segments <- num_changes + 1
#'         change_years <- sort(sample(possible_change_years, num_changes))
#'         change_dates <- as.Date(paste0(change_years, "-01-01"))
#'       }
#'     }
#' 
#'     # Build boundaries
#'     starts <- c(start_date, change_dates)
#'     # End each segment the day before the next Jan 1 change, with the last ending at end_date
#'     ends <- c(if (length(change_dates) > 0) change_dates - 1L else NULL, end_date)
#' 
#'     # Force Date class to avoid numeric coercion
#'     starts <- as.Date(starts, origin = "1970-01-01")
#'     ends <- as.Date(ends, origin = "1970-01-01")
#' 
#'     assigned_plans <- sample(plan_options, n_segments, replace = TRUE)
#' 
#'     df <- data.frame(
#'       person_id = pid,
#'       plan_start_date = starts,
#'       plan_end_date = ends,
#'       plan_name = assigned_plans,
#'       stringsAsFactors = FALSE
#'     )
#' 
#'     # Extra safety: coerce again in case upstream inputs were odd
#'     df$plan_start_date <- as.Date(df$plan_start_date)
#'     df$plan_end_date <- as.Date(df$plan_end_date)
#'     df
#'   }
#' 
#'   # Apply to each person
#'   payer_plans_df_long <- purrr::pmap_dfr(
#'     person_obs_periods,
#'     ~ generate_random_plans(..1, ..2, ..3, ..4, ..5)
#'   )
#' 
#'   # Final formatting of the payer_plan_period table
#'   payer_plans_df <- payer_plans_df_long |>
#'     dplyr::arrange(.data$person_id, .data$plan_start_date) |>
#'     dplyr::mutate(
#'       payerPlanPeriodId = dplyr::row_number(),
#'       # Derive payer type (Source Value) from the simulated plan name
#'       payerSourceValue = dplyr::case_when(
#'         grepl("PPO", plan_name) ~ "Commercial PPO",
#'         grepl("HMO", plan_name) ~ "Managed Care HMO",
#'         grepl("EPO", plan_name) ~ "Exclusive Provider Organization",
#'         TRUE ~ "Other Insurance"
#'       ),
#'       planSourceValue = .data$plan_name
#'     ) |>
#'     dplyr::select(.data$payerPlanPeriodId,
#'       .data$person_id,
#'       payerPlanPeriodStartDate = .data$plan_start_date,
#'       payerPlanPeriodEndDate = .data$plan_end_date,
#'       .data$payerSourceValue,
#'       .data$planSourceValue
#'     )
#' 
#'   # --- Define Cost Structure and Fetch Event Data ---
#'   cli::cli_alert_info("- Step 2: Fetching clinical events and mapping to plan periods.")
#'   costStructure <- list(
#'     "Procedure" = c("procedure_occurrence", "procedure_occurrence_id", "person_id", "procedure_date"),
#'     "Measurement" = c("measurement", "measurement_id", "person_id", "measurement_date"),
#'     "Visit" = c("visit_occurrence", "visit_occurrence_id", "person_id", "visit_start_date"),
#'     "Device" = c("device_exposure", "device_exposure_id", "person_id", "device_exposure_start_date"),
#'     "Drug" = c("drug_exposure", "drug_exposure_id", "person_id", "drug_exposure_start_date"),
#'     "Observation" = c("observation", "observation_id", "person_id", "observation_date"),
#'     "Condition" = c("condition_occurrence", "condition_occurrence_id", "person_id", "condition_start_date")
#'   )
#' 
#'   eventTable <- purrr::map2_df(
#'     names(costStructure), costStructure, ~ {
#'       sql <- glue::glue("SELECT t1.{.y[[2]]} as event_id, t1.{.y[[3]]} as person_id, t1.{.y[[4]]} as event_date FROM {cdmDatabaseSchema}.{.y[[1]]} t1")
#'       domain_table <- DatabaseConnector::querySql(connection, sql) |>
#'         dplyr::rename_with(tolower) |>
#'         dplyr::mutate(domain_id = .x, event_date = as.Date(.data$event_date))
#' 
#'       # Join events to the generated payer plans to assign the correct payer_plan_period_id
#'       domain_table |>
#'         dplyr::left_join(payer_plans_df, by = "person_id", relationship = "many-to-many") |>
#'         # Filter to ensure the event date falls within the specific payer plan period
#'         dplyr::filter(.data$event_date >= .data$payerPlanPeriodStartDate & .data$event_date <= .data$payerPlanPeriodEndDate)
#'     }
#'   )
#' 
#'   # --- Generate and Structure the Cost Records (Wide Format) ---
#'   cli::cli_alert_info("- Step 3: Generating synthetic cost values.")
#'   n_records <- nrow(eventTable)
#' 
#'   if (n_records > 0) {
#'     set.seed(seed)
#'     base_cost <- stats::runif(n_records, 23, 1500)
#'     total_charge <- round(base_cost * stats::runif(n_records, 1.1, 1.5), 2)
#'     total_cost <- round(base_cost * stats::runif(n_records, 0.7, 0.9), 2)
#'     insurance_coverage <- stats::runif(n_records, 0.54, 1)
#'     paid_by_payer <- round(total_cost * insurance_coverage, 2)
#'     paid_by_patient <- round(total_cost - paid_by_payer, 2)
#' 
#'     cost_records_df <- dplyr::tibble(
#'       costId = 1:n_records,
#'       costEventId = eventTable$event_id,
#'       costDomainId = eventTable$domain_id,
#'       costTypeConceptId = 5032, # Administrative cost record
#'       currencyConceptId = 44818668, # USD
#'       totalCharge = total_charge,
#'       totalCost = total_cost,
#'       totalPaid = paid_by_payer + paid_by_patient,
#'       paidByPayer = paid_by_payer,
#'       paidByPatient = paid_by_patient,
#'       paidPatientCopay = pmin(paid_by_patient, 50),
#'       paidPatientCoinsurance = round(paid_by_patient * 0.7, 2),
#'       paidPatientDeductible = round(paid_by_patient * 0.3, 2),
#'       paidByPrimary = paid_by_payer,
#'       paidIngredientCost = NA_real_,
#'       paidDispensingFee = NA_real_,
#'       payerPlanPeriodId = eventTable$payerPlanPeriodId,
#'       amountAllowed = total_cost,
#'       revenueCodeConceptId = 0,
#'       revenueCodeSourceValue = NA_character_,
#'       drgConceptId = 0,
#'       drgSourceValue = NA_character_
#'     )
#'   } else {
#'     cli::cli_alert_info("No clinical events found. Skipping cost generation.")
#'     cost_records_df <- data.frame()
#'   }
#' 
#'   # --- Insert Tables into the Database ---
#'   cli::cli_alert_info("- Step 4: Inserting new tables into the database.")
#'   fullPayerPlanTableName <- "payer_plan_period"
#'   fullCostTableName <- "cost"
#' 
#'   DatabaseConnector::insertTable(
#'     connection = connection,
#'     tableName = fullPayerPlanTableName,
#'     data = payer_plans_df,
#'     dropTableIfExists = TRUE,
#'     createTable = TRUE,
#'     camelCaseToSnakeCase = TRUE
#'   )
#' 
#'   if (nrow(cost_records_df) > 0) {
#'     DatabaseConnector::insertTable(
#'       connection = connection,
#'       tableName = fullCostTableName,
#'       data = cost_records_df,
#'       dropTableIfExists = TRUE,
#'       createTable = TRUE,
#'       camelCaseToSnakeCase = TRUE
#'     )
#'   }
#'   cli::cli_alert_info("Successfully injected synthetic payer_plan_period and wide-format cost tables.")
#'   return(connection)
#' }
#' #' Populate VISIT_DETAIL from Event Domain Tables (line-item sub-visit records)
#' #'
#' #' @description
#' #' Builds a visit-centric **line-item** representation in `VISIT_DETAIL` by projecting
#' #' events from domain tables (PROCEDURE, MEASUREMENT, DEVICE, DRUG, OBSERVATION, CONDITION).
#' #' Each event becomes (typically) one `visit_detail_id`, preserving granular HEOR linkage.
#' #'
#' #' @details
#' #' Steps:
#' #' 1. (Re)Create `VISIT_DETAIL` table with CDM v5.5 columns.
#' #' 2. Stage domain events with start/end, provider, and source fields.
#' #' 3. Enrich with `VISIT_OCCURRENCE` and provider→care_site.
#' #' 4. Generate stable `visit_detail_id`s and `preceding_visit_detail_id` within each
#' #'    (person, visit) ordered by start time (then event_id).
#' #' 5. Create helpful indexes for common query paths.
#' #'
#' #' Assumptions:
#' #' - `visit_detail_concept_id` is **inherited from `VISIT_OCCURRENCE.visit_concept_id`** as a proxy.
#' #'   Replace with domain-specific mappings if you have better semantics for sub-visit types.
#' #' - `visit_detail_type_concept_id` prefers the domain’s `*_type_concept_id`, falling back
#' #'   to `VISIT_OCCURRENCE.visit_type_concept_id`.
#' #' - `admitting/discharge` values are copied from the parent visit (can be customized).
#' #'
#' #' Vendor notes:
#' #' - Temporary table notation `tmp_event_detail_stage` is SQL Server style. The code uses
#' #'   `renderTranslateExecuteSql()` **only for the staging step** (so translation should
#' #'   adapt the temp-table accordingly). Other statements use `executeSql()` and reference
#' #'   the same temp name; ensure your dialect supports it after translation or convert
#' #'   to a TEMP table name that works across targets.
#' #'
#' #' @param connection A live DatabaseConnector connection.
#' #' @param cdmDatabaseSchema Schema containing OMOP CDM tables.
#' #'
#' #' @return
#' #' Returns the `connection`. Called primarily for its side effects (table inserts).
#' #'
#' #' @examples
#' #' \dontrun{
#' #' injectVisitDetailsData(conn, cdmDatabaseSchema = "main")
#' #' }
#' injectVisitDetailsData <- function(connection, cdmDatabaseSchema = "main") {
#'   visitDetailTable <- "visit_detail"
#'   DatabaseConnector::executeSql(connection, glue::glue("
#'     DROP TABLE IF EXISTS {cdmDatabaseSchema}.{visitDetailTable};
#'     CREATE TABLE {cdmDatabaseSchema}.{visitDetailTable} (
#'       visit_detail_id BIGINT NOT NULL PRIMARY KEY,
#'       person_id BIGINT NOT NULL,
#'       visit_detail_concept_id INTEGER NOT NULL,
#'       visit_detail_start_date DATE NOT NULL,
#'       visit_detail_start_datetime TIMESTAMP NULL,
#'       visit_detail_end_date DATE NULL,
#'       visit_detail_end_datetime TIMESTAMP NULL,
#'       visit_detail_type_concept_id INTEGER NOT NULL,
#'       provider_id BIGINT NULL,
#'       care_site_id BIGINT NULL,
#'       visit_detail_source_value VARCHAR(50) NULL,
#'       visit_detail_source_concept_id INTEGER NULL,
#'       admitting_source_value VARCHAR(50) NULL,
#'       admitting_source_concept_id INTEGER NULL,
#'       discharge_to_source_value VARCHAR(50) NULL,
#'       discharge_to_concept_id INTEGER NULL,
#'       preceding_visit_detail_id BIGINT NULL,
#'       visit_detail_parent_id BIGINT NULL,
#'       visit_occurrence_id BIGINT NOT NULL
#'     );
#'   "))
#' 
#' 
#'   # 2) Build a staging of event records that should become line items in VISIT_DETAIL
#'   DatabaseConnector::renderTranslateExecuteSql(connection, glue::glue("
#'   DROP TABLE IF EXISTS tmp_event_detail_stage;
#'   CREATE TEMPORARY TABLE tmp_event_detail_stage AS
#'   SELECT * FROM (
#'     /* PROCEDURE */
#'     SELECT
#'       'PROCEDURE' AS domain_id,
#'       po.procedure_occurrence_id AS event_id,
#'       po.person_id,
#'       po.visit_occurrence_id,
#'       po.visit_detail_id AS source_visit_detail_id,            -- for idempotency if already present
#'       po.procedure_date AS start_date,
#'       po.procedure_datetime AS start_datetime,
#'       po.procedure_date AS end_date,
#'       po.procedure_datetime AS end_datetime,
#'       po.provider_id,
#'       po.procedure_source_value AS source_value,
#'       po.procedure_source_concept_id AS source_concept_id,
#'       po.procedure_type_concept_id AS domain_type_concept_id
#'     FROM {cdmDatabaseSchema}.procedure_occurrence po
#' 
#'     UNION ALL
#'     /* MEASUREMENT */
#'     SELECT
#'       'MEASUREMENT',
#'       m.measurement_id,
#'       m.person_id,
#'       m.visit_occurrence_id,
#'       m.visit_detail_id,
#'       m.measurement_date,
#'       m.measurement_datetime,
#'       m.measurement_date,
#'       m.measurement_datetime,
#'       m.provider_id,
#'       m.measurement_source_value,
#'       m.measurement_source_concept_id,
#'       m.measurement_type_concept_id
#'     FROM {cdmDatabaseSchema}.measurement m
#' 
#'     UNION ALL
#'     /* DEVICE */
#'     SELECT
#'       'DEVICE',
#'       de.device_exposure_id,
#'       de.person_id,
#'       de.visit_occurrence_id,
#'       de.visit_detail_id,
#'       de.device_exposure_start_date,
#'       de.device_exposure_start_datetime,
#'       COALESCE(de.device_exposure_end_date, de.device_exposure_start_date),
#'       COALESCE(de.device_exposure_end_datetime, de.device_exposure_start_datetime),
#'       de.provider_id,
#'       de.device_source_value,
#'       de.device_source_concept_id,
#'       de.device_type_concept_id
#'     FROM {cdmDatabaseSchema}.device_exposure de
#' 
#'     UNION ALL
#'     /* DRUG */
#'     SELECT
#'       'DRUG',
#'       d.drug_exposure_id,
#'       d.person_id,
#'       d.visit_occurrence_id,
#'       d.visit_detail_id,
#'       d.drug_exposure_start_date,
#'       d.drug_exposure_start_datetime,
#'       COALESCE(d.drug_exposure_end_date, d.drug_exposure_start_date),
#'       COALESCE(d.drug_exposure_end_datetime, d.drug_exposure_start_datetime),
#'       d.provider_id,
#'       d.drug_source_value,
#'       d.drug_source_concept_id,
#'       d.drug_type_concept_id
#'     FROM {cdmDatabaseSchema}.drug_exposure d
#' 
#'     UNION ALL
#'     /* OBSERVATION */
#'     SELECT
#'       'OBSERVATION',
#'       o.observation_id,
#'       o.person_id,
#'       o.visit_occurrence_id,
#'       o.visit_detail_id,
#'       o.observation_date,
#'       o.observation_datetime,
#'       o.observation_date,
#'       o.observation_datetime,
#'       o.provider_id,
#'       o.observation_source_value,
#'       o.observation_source_concept_id,
#'       o.observation_type_concept_id
#'     FROM {cdmDatabaseSchema}.observation o
#' 
#'     UNION ALL
#'     /* CONDITION */
#'     SELECT
#'       'CONDITION',
#'       co.condition_occurrence_id,
#'       co.person_id,
#'       co.visit_occurrence_id,
#'       co.visit_detail_id,
#'       co.condition_start_date,
#'       co.condition_start_datetime,
#'       COALESCE(co.condition_end_date, co.condition_start_date),
#'       COALESCE(co.condition_end_datetime, co.condition_start_datetime),
#'       co.provider_id,
#'       co.condition_source_value,
#'       co.condition_source_concept_id,
#'       co.condition_type_concept_id
#'     FROM {cdmDatabaseSchema}.condition_occurrence co
#'   ) s
#'   WHERE s.visit_occurrence_id IS NOT NULL;  -- enforce linkage to visit
#' "))
#' 
#' 
#'   DatabaseConnector::executeSql(connection, glue::glue("
#'   WITH id_bounds AS (
#'   SELECT COALESCE(MAX(visit_detail_id), 0) AS id_offset
#'   FROM {cdmDatabaseSchema}.{visitDetailTable}
#' ),
#' enriched AS (
#'   SELECT
#'     s.domain_id,
#'     s.event_id,
#'     s.person_id,
#'     s.visit_occurrence_id,
#'     s.source_visit_detail_id,
#'     vo.visit_concept_id AS visit_detail_concept_id,
#'     COALESCE(s.start_date, vo.visit_start_date) AS start_date,
#'     COALESCE(s.start_datetime, CAST(COALESCE(s.start_date, vo.visit_start_date) AS TIMESTAMP)) AS start_datetime,
#'     COALESCE(s.end_date, COALESCE(s.start_date, vo.visit_start_date)) AS end_date,
#'     COALESCE(s.end_datetime, CAST(COALESCE(s.end_date, COALESCE(s.start_date, vo.visit_start_date)) AS TIMESTAMP)) AS end_datetime,
#'     COALESCE(s.provider_id, vo.provider_id) AS provider_id,
#'     p.care_site_id,
#'     s.source_value AS visit_detail_source_value,
#'     s.source_concept_id AS visit_detail_source_concept_id,
#'     COALESCE(s.domain_type_concept_id, vo.visit_type_concept_id) AS visit_detail_type_concept_id,
#'     vo.admitting_source_value,
#'     vo.admitting_source_concept_id,
#'     vo.discharge_to_source_value,
#'     vo.discharge_to_concept_id
#'   FROM tmp_event_detail_stage s
#'   JOIN {cdmDatabaseSchema}.visit_occurrence vo
#'     ON vo.visit_occurrence_id = s.visit_occurrence_id
#'   LEFT JOIN {cdmDatabaseSchema}.provider p
#'     ON p.provider_id = COALESCE(s.provider_id, vo.provider_id)
#' ),
#' -- Create a single global sequence for unique IDs
#' seq_all AS (
#'   SELECT
#'     e.*,
#'     ROW_NUMBER() OVER (
#'       ORDER BY e.person_id, e.visit_occurrence_id, e.start_datetime, e.event_id
#'     ) AS seq_all
#'   FROM enriched e
#' ),
#' seq_with_prev AS (
#'   SELECT
#'     s.*,
#'     LAG(s.seq_all) OVER (
#'       PARTITION BY s.person_id, s.visit_occurrence_id
#'       ORDER BY s.start_datetime, s.event_id
#'     ) AS prev_seq_all_in_visit
#'   FROM seq_all s
#' ),
#' final_rows AS (
#'   SELECT
#'     (SELECT id_offset FROM id_bounds) + seq_all AS visit_detail_id,
#'     person_id,
#'     visit_detail_concept_id,
#'     start_date AS visit_detail_start_date,
#'     start_datetime AS visit_detail_start_datetime,
#'     end_date AS visit_detail_end_date,
#'     end_datetime AS visit_detail_end_datetime,
#'     visit_detail_type_concept_id,
#'     provider_id,
#'     care_site_id,
#'     visit_detail_source_value,
#'     visit_detail_source_concept_id,
#'     admitting_source_value,
#'     admitting_source_concept_id,
#'     discharge_to_source_value,
#'     discharge_to_concept_id,
#'     CASE
#'       WHEN prev_seq_all_in_visit IS NOT NULL
#'         THEN (SELECT id_offset FROM id_bounds) + prev_seq_all_in_visit
#'       ELSE NULL
#'     END AS preceding_visit_detail_id,
#'     CAST(NULL AS BIGINT) AS visit_detail_parent_id,
#'     visit_occurrence_id
#'   FROM seq_with_prev
#' )
#' INSERT INTO {cdmDatabaseSchema}.{visitDetailTable} (
#'   visit_detail_id,
#'   person_id,
#'   visit_detail_concept_id,
#'   visit_detail_start_date,
#'   visit_detail_start_datetime,
#'   visit_detail_end_date,
#'   visit_detail_end_datetime,
#'   visit_detail_type_concept_id,
#'   provider_id,
#'   care_site_id,
#'   visit_detail_source_value,
#'   visit_detail_source_concept_id,
#'   admitting_source_value,
#'   admitting_source_concept_id,
#'   discharge_to_source_value,
#'   discharge_to_concept_id,
#'   preceding_visit_detail_id,
#'   visit_detail_parent_id,
#'   visit_occurrence_id
#' )
#' SELECT
#'   visit_detail_id,
#'   person_id,
#'   visit_detail_concept_id,
#'   visit_detail_start_date,
#'   visit_detail_start_datetime,
#'   visit_detail_end_date,
#'   visit_detail_end_datetime,
#'   visit_detail_type_concept_id,
#'   provider_id,
#'   care_site_id,
#'   visit_detail_source_value,
#'   visit_detail_source_concept_id,
#'   admitting_source_value,
#'   admitting_source_concept_id,
#'   discharge_to_source_value,
#'   discharge_to_concept_id,
#'   preceding_visit_detail_id,
#'   visit_detail_parent_id,
#'   visit_occurrence_id
#' FROM final_rows;"))
#' 
#'   return(connection)
#' }
#' 
#' #' Transform Wide-Format COST to CDM v5.5 Long (Normalized) Structure
#' #'
#' #' @description
#' #' Backs up the wide-format `cost` table to `{schema}.cost_v5_3_backup`, creates a new
#' #' v5.5-compliant `cost` table, and populates it by pivoting cost components into
#' #' normalized rows with `cost_concept_id`. Events are mapped to visits via a unified
#' #' event→visit/person/date table.
#' #'
#' #' @details
#' #' - **Backup**: `ALTER TABLE {schema}.{sourceCostTable} RENAME TO cost_v5_3_backup`.
#' #' - **Visit mapping**: Builds a temporary `event_to_visit_map` from multiple domains,
#' #'   including VISIT and VISIT_DETAIL. (Currently `visit_detail_id` is set to NULL in output.)
#' #' - **Normalization**: Emits one row per cost component (`total_cost`, `paid_by_payer`, ...),
#' #'   with appropriate `cost_concept_id` and `cost_source_value`.
#' #' - **Dates**: `effective_date` defaults to event_date; `incurred_date` is event_date; `billed_date`/`paid_date` left NULL.
#' #' - **Concept defaults**: `cost_event_field_concept_id = 1147332` (constant), `currency_concept_id = 44818668 (USD)`,
#' #'   fallback `cost_type_concept_id = 31968` if absent.
#' #'
#' #' Important:
#' #' - The **long table currently populates only `visit_occurrence_id`**; `visit_detail_id` is NULL.
#' #'   If you intend to emit line-level costs, add logic to set `visit_detail_id` when
#' #'   the cost event is a VISIT_DETAIL row (see "Suggested fixes").
#' #' - Index creation uses vendor-neutral `CREATE INDEX` syntax; adjust if your target
#' #'   supports `CONCURRENTLY`/`ONLINE`.
#' #'
#' #' @param connectionDetails DatabaseConnector connectionDetails. Defaults to Eunomia details.
#' #' @param cdmDatabaseSchema OMOP CDM schema/database. Default: "main".
#' #' @param sourceCostTable Name of the (wide) COST table to transform. Default: "cost".
#' #' @return Returns the open DatabaseConnector `connection` (invisibly).
#' #'
#' #' @examples
#' #' \dontrun{
#' #' transformCostToCdmV5dot5()
#' #' }
#' transformCostToCdmV5dot5 <- function(
#'     connectionDetails = Eunomia::getEunomiaConnectionDetails(),
#'     cdmDatabaseSchema = "main",
#'     sourceCostTable = "cost") {
#'   connection <- DatabaseConnector::connect(connectionDetails)
#'   connection <- injectCostData(connection)
#'   connection <- injectVisitDetailsData(connection)
#'   cli::cli_alert_info(glue::glue("Starting transformation of wide '{sourceCostTable}' table to long format using SQL."))
#' 
#'   # --- Pre-flight Checks ---
#'   tablesInDb <- tolower(DatabaseConnector::getTableNames(connection, cdmDatabaseSchema))
#'   if (!tolower(sourceCostTable) %in% tablesInDb) {
#'     stop(glue::glue("Source cost table '{cdmDatabaseSchema}.{sourceCostTable}' not found. Please run injectCostData() first."))
#'   }
#' 
#'   # --- Backup existing table ---
#'   cli::cli_alert_info("- Step 1: Backing up existing cost table.")
#'   backupTableName <- glue::glue("{cdmDatabaseSchema}.cost_v5_3_backup")
#'   fullCostTableName <- glue::glue("{cdmDatabaseSchema}.{sourceCostTable}")
#' 
#'   # Drop backup if it exists, then rename current table
#'   if (tolower("cost_v5_3_backup") %in% tablesInDb) {
#'     DatabaseConnector::executeSql(connection, glue::glue("DROP TABLE {backupTableName};"))
#'   }
#'   DatabaseConnector::executeSql(connection, glue::glue("ALTER TABLE {fullCostTableName} RENAME TO cost_v5_3_backup;"))
#'   cli::cli_alert_info(glue::glue("Original '{sourceCostTable}' table renamed to 'cost_v5_3_backup'."))
#' 
#'   # --- Create new cost table with CDM v5.5 structure ---
#'   cli::cli_alert_info("- Step 2: Creating new cost table with CDM v5.5 structure.")
#' 
#'   createTableSql <- glue::glue("
#'     CREATE TABLE {cdmDatabaseSchema}.{sourceCostTable} (
#'       cost_id BIGINT NOT NULL PRIMARY KEY,
#'       person_id BIGINT NOT NULL,
#'       visit_occurrence_id BIGINT NULL,
#'       visit_detail_id BIGINT NULL,
#'       effective_date DATE NULL,
#'       cost_event_field_concept_id INTEGER NOT NULL,
#'       cost_type_concept_id INTEGER NOT NULL,
#'       cost_concept_id INTEGER NOT NULL,
#'       cost_source_value VARCHAR(50) NULL,
#'       currency_concept_id INTEGER NULL,
#'       cost_source_concept_id INTEGER NULL,
#'       cost NUMERIC(19,4) NULL,
#'       payer_plan_period_id BIGINT NULL,
#'       incurred_date DATE NULL,
#'       billed_date DATE NULL,
#'       paid_date DATE NULL
#'     );
#'   ")
#'   DatabaseConnector::executeSql(connection, createTableSql)
#' 
#'   # === 1) Build a reusable event -> visit/person/date map ===
#'   eventToVisitSql <- glue::glue("
#'   CREATE TEMPORARY TABLE event_to_visit_map AS
#'   WITH all_events AS (
#'     SELECT
#'       po.procedure_occurrence_id AS event_id,
#'       'Procedure' AS domain_id,
#'       po.person_id,
#'       po.procedure_date AS event_date,
#'       po.visit_occurrence_id,
#'       po.procedure_concept_id AS base_concept_id
#'     FROM {cdmDatabaseSchema}.procedure_occurrence po
#' 
#'     UNION ALL
#'     SELECT
#'       m.measurement_id,
#'       'Measurement',
#'       m.person_id,
#'       m.measurement_date,
#'       m.visit_occurrence_id,
#'       m.measurement_concept_id
#'     FROM {cdmDatabaseSchema}.measurement m
#' 
#'     UNION ALL
#'     SELECT
#'       vo.visit_occurrence_id,
#'       'Visit',
#'       vo.person_id,
#'       vo.visit_start_date,
#'       vo.visit_occurrence_id,
#'       vo.visit_concept_id
#'     FROM {cdmDatabaseSchema}.visit_occurrence vo
#' 
#'     UNION ALL
#'     SELECT
#'       vd.visit_detail_id,
#'       'Visit Detail',
#'       vd.person_id,
#'       vd.visit_detail_start_date,
#'       vd.visit_occurrence_id,
#'       vd.visit_detail_concept_id
#'     FROM {cdmDatabaseSchema}.visit_detail vd
#' 
#'     UNION ALL
#'     SELECT
#'       de.device_exposure_id,
#'       'Device',
#'       de.person_id,
#'       de.device_exposure_start_date,
#'       de.visit_occurrence_id,
#'       de.device_concept_id
#'     FROM {cdmDatabaseSchema}.device_exposure de
#' 
#'     UNION ALL
#'     SELECT
#'       d.drug_exposure_id,
#'       'Drug',
#'       d.person_id,
#'       d.drug_exposure_start_date,
#'       d.visit_occurrence_id,
#'       d.drug_concept_id
#'     FROM {cdmDatabaseSchema}.drug_exposure d
#' 
#'     UNION ALL
#'     SELECT
#'       o.observation_id,
#'       'Observation',
#'       o.person_id,
#'       o.observation_date,
#'       o.visit_occurrence_id,
#'       o.observation_concept_id
#'     FROM {cdmDatabaseSchema}.observation o
#' 
#'     UNION ALL
#'     SELECT
#'       co.condition_occurrence_id,
#'       'Condition',
#'       co.person_id,
#'       co.condition_start_date,
#'       co.visit_occurrence_id,
#'       co.condition_concept_id
#'     FROM {cdmDatabaseSchema}.condition_occurrence co
#'   )
#'   SELECT DISTINCT * FROM all_events;
#' ")
#'   DatabaseConnector::executeSql(connection, eventToVisitSql)
#' 
#'   # === 2) Transform and load into the v5.5 COST table ===
#'   transformSql <- glue::glue("
#'   WITH wide_cost AS (
#'     SELECT DISTINCT
#'         etv.visit_occurrence_id,
#'         NULL AS visit_detail_id,
#'         COALESCE(etv.person_id, pp.person_id) AS person_id,
#'         COALESCE(etv.event_date, CURRENT_DATE) AS effective_date,
#'         1147332 AS cost_event_field_concept_id,  -- keep your chosen field concept
#'         COALESCE(c.cost_type_concept_id, 31968) AS cost_type_concept_id,
#'         COALESCE(c.currency_concept_id, 44818668) AS currency_concept_id,
#'         0 AS cost_source_concept_id,
#'         -- amounts from legacy v5.3 cost
#'         c.total_charge,
#'         c.total_cost,
#'         c.total_paid,
#'         c.paid_by_payer,
#'         c.paid_by_patient,
#'         c.paid_patient_copay,
#'         c.paid_patient_coinsurance,
#'         c.paid_patient_deductible,
#'         c.paid_by_primary,
#'         c.paid_ingredient_cost,
#'         c.paid_dispensing_fee,
#'         c.payer_plan_period_id,
#'         c.amount_allowed,
#'         etv.event_date AS incurred_date,
#'         NULL AS billed_date,
#'         NULL AS paid_date
#'     FROM {cdmDatabaseSchema}.cost_v5_3_backup c
#'     LEFT JOIN {cdmDatabaseSchema}.payer_plan_period pp
#'         ON c.payer_plan_period_id = pp.payer_plan_period_id
#'     LEFT JOIN event_to_visit_map etv
#'         ON c.cost_event_id = etv.event_id AND c.cost_domain_id = etv.domain_id
#'   ),
#'   cost_long AS (
#'     SELECT person_id, visit_occurrence_id, visit_detail_id, effective_date,
#'            cost_event_field_concept_id, cost_type_concept_id, 31973 AS cost_concept_id,
#'            'total_charge' AS cost_source_value, currency_concept_id, cost_source_concept_id,
#'            total_charge AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
#'     FROM wide_cost WHERE total_charge IS NOT NULL
#' 
#'     UNION ALL
#'     SELECT person_id, visit_occurrence_id, visit_detail_id, effective_date,
#'            cost_event_field_concept_id, cost_type_concept_id, 31985 AS cost_concept_id,
#'            'total_cost' AS cost_source_value, currency_concept_id, cost_source_concept_id,
#'            total_cost AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
#'     FROM wide_cost WHERE total_cost IS NOT NULL
#' 
#'     UNION ALL
#'     SELECT person_id, visit_occurrence_id, visit_detail_id, effective_date,
#'            cost_event_field_concept_id, cost_type_concept_id, 31980 AS cost_concept_id,
#'            'total_paid' AS cost_source_value, currency_concept_id, cost_source_concept_id,
#'            total_paid AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
#'     FROM wide_cost WHERE total_paid IS NOT NULL
#' 
#'     UNION ALL
#'     SELECT person_id, visit_occurrence_id, visit_detail_id, effective_date,
#'            cost_event_field_concept_id, cost_type_concept_id, 31980 AS cost_concept_id,
#'            'paid_by_payer' AS cost_source_value, currency_concept_id, cost_source_concept_id,
#'            paid_by_payer AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
#'     FROM wide_cost WHERE paid_by_payer IS NOT NULL
#' 
#'     UNION ALL
#'     SELECT person_id, visit_occurrence_id, visit_detail_id, effective_date,
#'            cost_event_field_concept_id, cost_type_concept_id, 31981 AS cost_concept_id,
#'            'paid_by_patient' AS cost_source_value, currency_concept_id, cost_source_concept_id,
#'            paid_by_patient AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
#'     FROM wide_cost WHERE paid_by_patient IS NOT NULL
#' 
#'     UNION ALL
#'     SELECT person_id, visit_occurrence_id, visit_detail_id, effective_date,
#'            cost_event_field_concept_id, cost_type_concept_id, 31974 AS cost_concept_id,
#'            'paid_patient_copay' AS cost_source_value, currency_concept_id, cost_source_concept_id,
#'            paid_patient_copay AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
#'     FROM wide_cost WHERE paid_patient_copay IS NOT NULL
#' 
#'     UNION ALL
#'     SELECT person_id, visit_occurrence_id, visit_detail_id, effective_date,
#'            cost_event_field_concept_id, cost_type_concept_id, 31975 AS cost_concept_id,
#'            'paid_patient_coinsurance' AS cost_source_value, currency_concept_id, cost_source_concept_id,
#'            paid_patient_coinsurance AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
#'     FROM wide_cost WHERE paid_patient_coinsurance IS NOT NULL
#' 
#'     UNION ALL
#'     SELECT person_id, visit_occurrence_id, visit_detail_id, effective_date,
#'            cost_event_field_concept_id, cost_type_concept_id, 31976 AS cost_concept_id,
#'            'paid_patient_deductible' AS cost_source_value, currency_concept_id, cost_source_concept_id,
#'            paid_patient_deductible AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
#'     FROM wide_cost WHERE paid_patient_deductible IS NOT NULL
#' 
#'     UNION ALL
#'     SELECT person_id, visit_occurrence_id, visit_detail_id, effective_date,
#'            cost_event_field_concept_id, cost_type_concept_id, 31979 AS cost_concept_id,
#'            'amount_allowed' AS cost_source_value, currency_concept_id, cost_source_concept_id,
#'            amount_allowed AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
#'     FROM wide_cost WHERE amount_allowed IS NOT NULL
#'   )
#'   INSERT INTO {cdmDatabaseSchema}.{sourceCostTable} (
#'     cost_id, person_id, visit_occurrence_id, visit_detail_id,
#'     effective_date, cost_event_field_concept_id, cost_type_concept_id,
#'     cost_concept_id, cost_source_value, currency_concept_id, cost_source_concept_id,
#'     cost, payer_plan_period_id, incurred_date, billed_date, paid_date
#'   )
#'   SELECT
#'     ROW_NUMBER() OVER (
#'       ORDER BY person_id,
#'                visit_occurrence_id,
#'                CASE WHEN visit_detail_id IS NULL THEN 1 ELSE 0 END,
#'                visit_detail_id,
#'                effective_date,
#'                cost_concept_id,
#'                cost_source_value
#'     ) AS cost_id,
#'     person_id,
#'     visit_occurrence_id,
#'     visit_detail_id,
#'     effective_date,
#'     cost_event_field_concept_id,
#'     cost_type_concept_id,
#'     cost_concept_id,
#'     cost_source_value,
#'     currency_concept_id,
#'     cost_source_concept_id,
#'     CAST(cost AS NUMERIC(19,4)) AS cost,
#'     payer_plan_period_id,
#'     incurred_date,
#'     billed_date,
#'     paid_date
#'   FROM cost_long;
#' ")
#'   DatabaseConnector::executeSql(connection, transformSql)
#' 
#'   # Clean up temporary table
#'   DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS event_to_visit_map;")
#' 
#'   cli::cli_alert_info(glue::glue("Successfully transformed '{sourceCostTable}' table to CDM v5.5 long format and created indexes (where possible)."))
#' 
#'   return(connection)
#' }
