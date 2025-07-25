# Copyright 2025 OHDSI
#
# This file is part of CostUtilization
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Inject Synthetic Payer Plan and Wide-Format Cost Data into Eunomia
#'
#' @description
#' Populates the Eunomia database with synthetic `payer_plan_period` and `cost` tables.
#' The generated `cost` table is in a wide format, typical of data sources prior to OMOP CDM v5.4,
#' making it a suitable input for the `transformCostToCdmV5_5` function.
#'
#' @details
#' This function performs the following actions:
#' 1.  Creates a realistic `payer_plan_period` table based on the observation periods of persons in Eunomia. Some persons will have continuous plans, while others will have their plans split to simulate plan changes.
#' 2.  Fetches clinical event data (procedures, drugs, visits, etc.) for all persons.
#' 3.  Generates random cost values for each event.
#' 4.  Structures these costs into a **wide** `cost` table with columns like `totalCharge`, `totalCost`, `paidByPayer`, etc.
#' 5.  Inserts both the `payer_plan_period` and the wide `cost` table into the specified schema in the Eunomia database, dropping any existing tables with the same names.
#'
#' @param connectionDetails       An R object of type `ConnectionDetails` created using the
#'                                `DatabaseConnector` package.
#' @param cdmDatabaseSchema       The name of the database schema that contains the OMOP CDM instance.
#'                                For Eunomia, this is typically "main".
#' @param seed                    A random seed for reproducibility of the synthetic data.
#'
#' @return
#' Invisibly returns `TRUE` upon successful completion. The function is called for its side effect of modifying the database.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' connectionDetails <- Eunomia::getEunomiaConnectionDetails()
#' injectCostData(connectionDetails = connectionDetails)
#' }
injectCostData <- function(connectionDetails,
                           cdmDatabaseSchema = "main",
                           seed = 123) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  message("Generating synthetic payer_plan_period and wide-format cost data.")

  # --- Create a Realistic Payer Plan Period Table ---
  message("- Step 1: Creating realistic payer_plan_period table.")
  person_obs_periods <- DatabaseConnector::querySql(connection, glue::glue("SELECT person_id, observation_period_start_date, observation_period_end_date FROM {cdmDatabaseSchema}.observation_period")) |>
    dplyr::rename_with(tolower) |>
    dplyr::mutate(dplyr::across(dplyr::ends_with("_date"), as.Date))

  set.seed(seed)
  split_date <- as.Date("2014-01-01")
  persons_to_split <- person_obs_periods |>
    dplyr::filter(.data$observation_period_start_date < split_date & .data$observation_period_end_date > split_date) |>
    dplyr::sample_frac(0.5)

  persons_continuous_plan <- person_obs_periods |>
    dplyr::anti_join(persons_to_split, by = "person_id")

  plans_part_1 <- persons_to_split |>
    dplyr::mutate(
      plan_start_date = .data$observation_period_start_date,
      plan_end_date = split_date - 1,
      plan_name = "PPO-Low-Deductible-500"
    )

  plans_part_2 <- persons_to_split |>
    dplyr::mutate(
      plan_start_date = split_date,
      plan_end_date = .data$observation_period_end_date,
      plan_name = "PPO-High-Deductible-2500"
    )

  continuous_plans <- persons_continuous_plan |>
    dplyr::mutate(
      plan_start_date = .data$observation_period_start_date,
      plan_end_date = .data$observation_period_end_date,
      plan_name = "PPO-Low-Deductible-500"
    )

  payer_plans_df <- dplyr::bind_rows(continuous_plans, plans_part_1, plans_part_2) |>
    dplyr::arrange(.data$person_id, .data$plan_start_date) |>
    dplyr::mutate(
      payerPlanPeriodId = dplyr::row_number(),
      payerSourceValue = "PPO",
      planSourceValue = .data$plan_name
    ) |>
    dplyr::select(.data$payerPlanPeriodId,
                  .data$person_id,
      payerPlanPeriodStartDate = .data$plan_start_date,
      payerPlanPeriodEndDate = .data$plan_end_date,
      .data$payerSourceValue,
      .data$planSourceValue
    )

  # --- Define Cost Structure and Fetch Event Data ---
  message("- Step 2: Fetching clinical events and mapping to plan periods.")
  costStructure <- list(
    "Procedure" = c("procedure_occurrence", "procedure_occurrence_id", "person_id", "procedure_date"),
    "Measurement" = c("measurement", "measurement_id", "person_id", "measurement_date"),
    "Visit" = c("visit_occurrence", "visit_occurrence_id", "person_id", "visit_start_date"),
    "Device" = c("device_exposure", "device_exposure_id", "person_id", "device_exposure_start_date"),
    "Drug" = c("drug_exposure", "drug_exposure_id", "person_id", "drug_exposure_start_date"),
    "Observation" = c("observation", "observation_id", "person_id", "observation_date"),
    "Condition" = c("condition_occurrence", "condition_occurrence_id", "person_id", "condition_start_date")
  )

  eventTable <- purrr::map2_df(
    names(costStructure), costStructure, ~ {
      sql <- glue::glue("SELECT t1.{.y[[2]]} as event_id, t1.{.y[[3]]} as person_id, t1.{.y[[4]]} as event_date FROM {cdmDatabaseSchema}.{.y[[1]]} t1")
      domain_table <- DatabaseConnector::querySql(connection, sql) |>
        dplyr::rename_with(tolower) |>
        dplyr::mutate(domain_id = .x, event_date = as.Date(.data$event_date))

      domain_table |>
        dplyr::left_join(payer_plans_df, by = "person_id", relationship = "many-to-many") |>
        dplyr::filter(.data$event_date >= .data$payerPlanPeriodStartDate & .data$event_date <= .data$payerPlanPeriodEndDate)
    }
  )

  # --- Generate and Structure the Cost Records (Wide Format) ---
  message("- Step 3: Generating synthetic cost values.")
  n_records <- nrow(eventTable)
  set.seed(seed)
  base_cost <- stats::runif(n_records, 23, 1500)
  total_charge <- round(base_cost * stats::runif(n_records, 1.1, 1.5), 2)
  total_cost <- round(base_cost * stats::runif(n_records, 0.7, 0.9), 2)
  insurance_coverage <- stats::runif(n_records, 0.54, 1)
  paid_by_payer <- round(total_cost * insurance_coverage, 2)
  paid_by_patient <- round(total_cost - paid_by_payer, 2)

  cost_records_df <- dplyr::tibble(
    costId = 1:n_records,
    costEventId = eventTable$event_id,
    costDomainId = eventTable$domain_id,
    costTypeConceptId = 5032,
    currencyConceptId = 44818668,
    totalCharge = total_charge,
    totalCost = total_cost,
    totalPaid = paid_by_payer + paid_by_patient,
    paidByPayer = paid_by_payer,
    paidByPatient = paid_by_patient,
    paidPatientCopay = pmin(paid_by_patient, 50),
    paidPatientCoinsurance = round(paid_by_patient * 0.7, 2),
    paidPatientDeductible = round(paid_by_patient * 0.3, 2),
    paidByPrimary = paid_by_payer,
    paidIngredientCost = NA_real_,
    paidDispensingFee = NA_real_,
    payerPlanPeriodId = eventTable$payerPlanPeriodId,
    amountAllowed = total_cost,
    revenueCodeConceptId = 0,
    revenueCodeSourceValue = NA_character_,
    drgConceptId = 0,
    drgSourceValue = NA_character_
  )

  # --- Insert Tables into the Database ---
  message("- Step 4: Inserting new tables into the database.")
  fullPayerPlanTableName <- glue::glue("{cdmDatabaseSchema}.payer_plan_period")
  fullCostTableName <- glue::glue("{cdmDatabaseSchema}.cost")

  DatabaseConnector::insertTable(
    connection = connection,
    tableName = fullPayerPlanTableName,
    data = payer_plans_df,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )

  DatabaseConnector::insertTable(
    connection = connection,
    tableName = fullCostTableName,
    data = cost_records_df,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )

  message("Successfully injected synthetic payer_plan_period and wide-format cost tables.")
  return(invisible(TRUE))
}


#' Transform a Wide Cost Table to OMOP CDM v5.4+ Long Format
#'
#' @description
#' Reads a wide-format `cost` table (typical of CDM v5.3 and earlier) from the database,
#' transforms it into the modern long format, backs up the original table, and inserts the
#' new long-format table.
#'
#' @details
#' This function is designed to work on a database that already contains a wide-format `cost` table,
#' such as one generated by `injectCostData()`. It performs a "wide-to-long" pivot on cost columns
#' (`total_charge`, `total_cost`, `paid_by_payer`, etc.), mapping each to a corresponding `cost_concept_id`.
#' It also populates new fields required by CDM v5.4+, such as `person_id` and `incurred_date`.
#'
#' The original `cost` table will be renamed to `cost_v5_3_backup` before the new table is created.
#'
#' @param connectionDetails       An R object of type `ConnectionDetails` created using the
#'                                `DatabaseConnector` package.
#' @param cdmDatabaseSchema       The name of the database schema that contains the OMOP CDM instance.
#'                                For Eunomia, this is typically "main".
#' @param sourceCostTable         The name of the wide-format cost table to transform. Defaults to "cost".
#'
#' @return
#' Invisibly returns `TRUE` upon successful completion. The function is called for its side effect of modifying the database.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' connectionDetails <- Eunomia::getEunomiaConnectionDetails()
#' # First, ensure wide-format cost data exists:
#' injectCostData(connectionDetails = connectionDetails)
#'
#' # Now, transform it to the modern long format:
#' transformCostToCdmV5_5(connectionDetails = connectionDetails)
#' }
transformCostToCdmV5_5 <- function(connectionDetails,
                                   cdmDatabaseSchema = "main",
                                   sourceCostTable = "cost") {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  message(glue::glue("Starting transformation of wide '{sourceCostTable}' table to long format."))

  # --- Pre-flight Checks ---
  tablesInDb <- tolower(DatabaseConnector::getTableNames(connection, cdmDatabaseSchema))
  if (!tolower(sourceCostTable) %in% tablesInDb) {
    stop(glue::glue("Source cost table '{cdmDatabaseSchema}.{sourceCostTable}' not found. Please run injectCostData() first."))
  }
  if (!"payer_plan_period" %in% tablesInDb) {
    stop(glue::glue("Required table '{cdmDatabaseSchema}.payer_plan_period' not found. Please run injectCostData() first."))
  }

  # --- Load Data and Add Person ID ---
  message("- Step 1: Loading source cost and payer plan data.")
  cost_v5_3_df <- DatabaseConnector::querySql(connection, glue::glue("SELECT * FROM {cdmDatabaseSchema}.{sourceCostTable}")) |>
    dplyr::rename_with(tolower)

  payer_plan_period_df <- DatabaseConnector::querySql(connection, glue::glue("SELECT person_id, payer_plan_period_id FROM {cdmDatabaseSchema}.payer_plan_period")) |>
    dplyr::rename_with(tolower)

  cost_v5_3_df <- cost_v5_3_df |>
    dplyr::left_join(payer_plan_period_df, by = "payer_plan_period_id")

  if (!"person_id" %in% names(cost_v5_3_df)) {
    stop("Could not add person_id to the cost data. Aborting.")
  }

  # --- Create Event-to-Visit Mapping ---
  message("- Step 2: Mapping clinical events to visit and date information.")
  costStructure <- list(
    "Procedure" = c("procedure_occurrence", "procedure_occurrence_id", "person_id", "procedure_date"),
    "Measurement" = c("measurement", "measurement_id", "person_id", "measurement_date"),
    "Visit" = c("visit_occurrence", "visit_occurrence_id", "person_id", "visit_start_date"),
    "Device" = c("device_exposure", "device_exposure_id", "person_id", "device_exposure_start_date"),
    "Drug" = c("drug_exposure", "drug_exposure_id", "person_id", "drug_exposure_start_date"),
    "Observation" = c("observation", "observation_id", "person_id", "observation_date"),
    "Condition" = c("condition_occurrence", "condition_occurrence_id", "person_id", "condition_start_date")
  )

  event_to_visit_map <- purrr::map2_df(
    names(costStructure), costStructure, ~ {
      sql <- glue::glue("SELECT t1.{.y[[2]]} AS event_id, t1.{.y[[3]]} AS event_date, t1.visit_occurrence_id FROM {cdmDatabaseSchema}.{.y[[1]]} t1")
      DatabaseConnector::querySql(connection, sql) |>
        dplyr::rename_with(tolower) |>
        dplyr::mutate(domain_id = .x, event_date = as.Date(.data$event_date))
    }
  ) |>
    dplyr::distinct(.data$event_id, .data$domain_id, .keep_all = TRUE)

  # --- Perform Wide-to-Long Transformation ---
  message("- Step 3: Performing wide-to-long data transformation.")
  cost_concept_map <- dplyr::tribble(
    ~cost_source_value, ~cost_concept_id,
    "total_charge", 31973,
    "total_cost", 31985,
    "total_paid", 31980,
    "paid_by_payer", 31980,
    "paid_by_primary", 31980,
    "paid_by_patient", 31981,
    "paid_patient_copay", 31974,
    "paid_patient_coinsurance", 31975,
    "paid_patient_deductible", 31976,
    "amount_allowed", 31979
  )

  cost_with_visit_info <- cost_v5_3_df |>
    dplyr::left_join(event_to_visit_map, by = c("costeventid" = "event_id", "cost_domainid" = "domain_id"))

  cost_v5_5_long <- cost_with_visit_info |>
    tidyr::pivot_longer(
      cols = dplyr::any_of(cost_concept_map$cost_source_value),
      names_to = "cost_source_value",
      values_to = "cost",
      values_drop_na = TRUE
    ) |>
    dplyr::left_join(cost_concept_map, by = "cost_source_value") |>
    dplyr::filter(!is.na(.data$cost_concept_id), .data$cost > 0) # Only keep valid, non-zero costs

  # --- Structure Final v5.4+ DataFrame ---
  message("- Step 4: Structuring final CDM v5.4+ cost table.")
  cost_final_df <- cost_v5_5_long |>
    dplyr::mutate(
      incurred_date = .data$event_date,
      billed_date = as.Date(NA),
      paid_date = as.Date(NA),
      cost_type_concept_id = 32817, # From a Claim
      revenue_code_concept_id = dplyr::coalesce(.data$revenuecodeconceptid, 0L),
      drg_concept_id = dplyr::coalesce(.data$drgconceptid, 0L)
    ) |>
    dplyr::arrange(.data$person_id, .data$incurred_date, .data$costeventid) |>
    dplyr::mutate(cost_id = dplyr::row_number()) |>
    dplyr::select(
      .data$cost_id,
      cost_event_id = .data$costeventid,
      cost_domain_id = .data$costdomainid,
      cost_concept_id = .data$cost_concept_id,
      cost_type_concept_id = .data$cost_type_concept_id,
      currency_concept_id = .data$currencyconceptid,
      cost = .data$cost,
      .data$incurred_date,
      .data$billed_date,
      .data$paid_date,
      payer_plan_period_id = .data$payerplanperiodid,
      amount_allowed = .data$amountallowed,
      .data$revenue_code_concept_id,
      revenue_code_source_value = .data$revenuecodesourcevalue,
      .data$drg_concept_id,
      drg_source_value = .data$drgsourcevalue
    )

  # --- Update the Database ---
  message("- Step 5: Backing up old table and inserting new long-format table.")
  backupTableName <- glue::glue("{cdmDatabaseSchema}.cost_v5_3_backup")
  fullCostTableName <- glue::glue("{cdmDatabaseSchema}.{sourceCostTable}")

  # Drop backup if it exists, then rename current table
  if (tolower("cost_v5_3_backup") %in% tablesInDb) {
    DatabaseConnector::executeSql(connection, glue::glue("DROP TABLE {backupTableName};"))
  }
  DatabaseConnector::executeSql(connection, glue::glue("ALTER TABLE {fullCostTableName} RENAME TO cost_v5_3_backup;"))
  message(glue::glue("Original '{sourceCostTable}' table renamed to 'cost_v5_3_backup'."))

  DatabaseConnector::insertTable(
    connection = connection,
    tableName = fullCostTableName,
    data = cost_final_df,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )
  message(glue::glue("Successfully created and inserted new long-format '{sourceCostTable}' table."))
  return(invisible(TRUE))
}
