#' Inject Synthetic Payer Plan and Wide-Format Cost Data into an OMOP CDM (Eunomia) Instance
#' @return Invisibly returns `TRUE` upon successful database insertion. Called for its side effect.
injectCostData <- function(connection) {
  seed <- 123
  
  cdmDatabaseSchema <-  'main'
  message("Generating synthetic payer_plan_period and wide-format cost data.")

  message("- Step 1: Creating realistic payer_plan_period table.")
  
  # Fetch observation periods and calculate start/end years
  person_obs_periods <- DatabaseConnector::querySql(connection, glue::glue("SELECT person_id, observation_period_start_date, observation_period_end_date FROM {cdmDatabaseSchema}.observation_period")) |>
    dplyr::rename_with(tolower) |>
    dplyr::mutate(dplyr::across(dplyr::ends_with("_date"), as.Date)) |>
    # Determine the calendar years covered by the observation period
    dplyr::mutate(start_year = as.integer(format(observation_period_start_date, "%Y")),
                  end_year = as.integer(format(observation_period_end_date, "%Y")))
  
  set.seed(seed)
  
  # Define plan options for simulation
  plan_options <- c("PPO-Low-Deductible-500", "HMO-Standard-1500", "PPO-High-Deductible-2500", "EPO-Basic-3000")
  
  # Helper function to generate randomized plan periods for a single person
  # This simulates plan changes happening on January 1st, with a bias towards continuous enrollment.
  generate_random_plans <- function(pid, start_date, end_date, start_year, end_year) {
    # Years where a change could potentially occur (Jan 1st of the years following the start year)
    possible_change_years <- (start_year + 1):end_year
    n_possible_changes <- length(possible_change_years)
    
    if (n_possible_changes == 0) {
      # Observation period is within a single calendar year
      n_segments <- 1
      change_dates <- c()
    } else {
      # Determine number of changes (0, 1, or 2 max), biased towards 0
      max_changes <- min(n_possible_changes, 2)
      # Probabilities: 60% chance of 0 changes, 30% of 1, 10% of 2 (if possible)
      probs <- c(0.6, 0.3, 0.1)
      # Normalize probabilities based on available slots
      current_probs <- probs[1:(max_changes + 1)]
      current_probs <- current_probs / sum(current_probs)
      num_changes <- sample(0:max_changes, 1, prob = current_probs)
      
      if (num_changes == 0) {
        n_segments <- 1
        change_dates <- c()
      } else {
        n_segments <- num_changes + 1
        # Select the specific years the changes happen
        change_years <- sort(sample(possible_change_years, num_changes))
        change_dates <- as.Date(paste0(change_years, "-01-01"))
      }
    }
    
    # Create start and end dates for the segments
    starts <- c(start_date, change_dates)
    # End dates are the day before the next change, or the final end date
    ends <- c(if (length(change_dates) > 0) change_dates - 1 else NULL, end_date)
    
    # Randomly assign plans for the segments
    assigned_plans <- sample(plan_options, n_segments, replace = TRUE)
    
    return(data.frame(
      person_id = pid,
      plan_start_date = starts,
      plan_end_date = start_date,
      plan_name = assigned_plans
    ))
  }
  
  # Apply the function to each person using dplyr rowwise operation
  # This approach provides realistic simulation while being reasonably performant for typical simulation sizes.
  payer_plans_df_long <- purrr::pmap_dfr(
    person_obs_periods,  ~ generate_random_plans(..1,..2,..3,..4, ..5))

  
  # Final formatting of the payer_plan_period table
  payer_plans_df <- payer_plans_df_long |>
    dplyr::arrange(.data$person_id, .data$plan_start_date) |>
    dplyr::mutate(
      payerPlanPeriodId = dplyr::row_number(),
      # Derive payer type (Source Value) from the simulated plan name
      payerSourceValue = dplyr::case_when(
        grepl("PPO", plan_name) ~ "Commercial PPO",
        grepl("HMO", plan_name) ~ "Managed Care HMO",
        grepl("EPO", plan_name) ~ "Exclusive Provider Organization",
        TRUE ~ "Other Insurance"
      ),
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
      
      # Join events to the generated payer plans to assign the correct payer_plan_period_id
      domain_table |>
        dplyr::left_join(payer_plans_df, by = "person_id", relationship = "many-to-many") |>
        # Filter to ensure the event date falls within the specific payer plan period
        dplyr::filter(.data$event_date >= .data$payerPlanPeriodStartDate & .data$event_date <= .data$payerPlanPeriodEndDate)
    }
  )
  
  # --- Generate and Structure the Cost Records (Wide Format) ---
  message("- Step 3: Generating synthetic cost values.")
  n_records <- nrow(eventTable)
  
  if (n_records > 0) {
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
      costTypeConceptId = 5032, # Administrative cost record
      currencyConceptId = 44818668, # USD
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
  } else {
    message("No clinical events found. Skipping cost generation.")
    cost_records_df <- data.frame()
  }
  
  # --- Insert Tables into the Database ---
  message("- Step 4: Inserting new tables into the database.")
  fullPayerPlanTableName <- "payer_plan_period"
  fullCostTableName <- "cost"
  
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = fullPayerPlanTableName,
    data = payer_plans_df,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )
  
  if (nrow(cost_records_df) > 0) {
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = fullCostTableName,
      data = cost_records_df,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      camelCaseToSnakeCase = TRUE
    )
  }
  message("Successfully injected synthetic payer_plan_period and wide-format cost tables.")
  return(connection)
}


#' Transform Wide-Format Cost Table to OMOP CDM v5.5 Long Format
#'
#' @description
#' Transforms a wide-format `cost` table (common in OMOP CDM v5.3 and earlier) into the standard long format required in CDM v5.4+.
#' This includes backing up the original table and inserting a new, long-format version.
#'
#' @details
#' This function performs the following:
#' 1. Loads the wide-format `cost` table (e.g., from `injectCostData()`), as well as associated `payer_plan_period` information.
#' 2. Maps cost event records to their corresponding domain-specific visit and date information.
#' 3. Transforms wide columns (e.g., `total_cost`, `paid_by_payer`, etc.) into long-form rows using standard `cost_concept_id` mappings.
#' 4. Appends newly required CDM v5.4 fields (e.g., `incurred_date`, `cost_type_concept_id`, etc.).
#' 5. Backs up the original `cost` table as `cost_v5_3_backup` and overwrites it with the new long-format version.
#'
#' Only non-zero and valid cost entries are included in the final result.
#'
#' @param connectionDetails An object of class `ConnectionDetails` as created by the DatabaseConnector package.
#' @param cdmDatabaseSchema Name of the database schema containing the OMOP CDM instance. Defaults to `"main"`.
#' @param sourceCostTable Name of the wide-format cost table to transform. Defaults to `"cost"`.
#'
#' @return Invisibly returns the database connection after successful execution.
#'
#' @export


transformCostToCdmV5dot5 <- function(
    connectionDetails,
    cdmDatabaseSchema = "main",
    sourceCostTable = "cost"
    ) {
  connection <- DatabaseConnector::connect(connectionDetails)
  connection <- injectCostData(connection)
  message(glue::glue("Starting transformation of wide '{sourceCostTable}' table to long format using SQL."))
    
  # --- Pre-flight Checks ---
  tablesInDb <- tolower(DatabaseConnector::getTableNames(connection, cdmDatabaseSchema))
  if (!tolower(sourceCostTable) %in% tablesInDb) {
      stop(glue::glue("Source cost table '{cdmDatabaseSchema}.{sourceCostTable}' not found. Please run injectCostData() first."))
    }
    
    # --- Backup existing table ---
    message("- Step 1: Backing up existing cost table.")
    backupTableName <- glue::glue("{cdmDatabaseSchema}.cost_v5_3_backup")
    fullCostTableName <- glue::glue("{cdmDatabaseSchema}.{sourceCostTable}")
    
    # Drop backup if it exists, then rename current table
    if (tolower("cost_v5_3_backup") %in% tablesInDb) {
      DatabaseConnector::executeSql(connection, glue::glue("DROP TABLE {backupTableName};"))
    }
    DatabaseConnector::executeSql(connection, glue::glue("ALTER TABLE {fullCostTableName} RENAME TO cost_v5_3_backup;"))
    message(glue::glue("Original '{sourceCostTable}' table renamed to 'cost_v5_3_backup'."))
    
    # --- Create new cost table with CDM v5.5 structure ---
    message("- Step 2: Creating new cost table with CDM v5.5 structure.")
    createTableSql <- glue::glue("
  CREATE TABLE {cdmDatabaseSchema}.{sourceCostTable} (
    cost_id BIGINT PRIMARY KEY,
    person_id BIGINT NOT NULL,
    cost_event_id BIGINT,
    visit_occurrence_id BIGINT,
    visit_detail_id BIGINT,
    cost_domain_id VARCHAR(50) NULL,
    effective_date DATE NOT NULL,
    cost_event_field_concept_id INT DEFAULT 1147332,
    cost_concept_id INT NOT NULL,
    cost_type_concept_id INT DEFAULT 31968,
    cost_source_concept_id INT,
    cost_source_value VARCHAR(50),
    currency_concept_id INT DEFAULT 44818668,
    total_charge FLOAT,
    total_cost FLOAT,
    total_paid FLOAT,
    paid_by_payer FLOAT,
    paid_by_patient FLOAT,
    paid_patient_copay FLOAT,
    paid_patient_coinsurance FLOAT,
    paid_patient_deductible FLOAT,
    paid_by_primary FLOAT,
    paid_ingredient_cost FLOAT,
    paid_dispensing_fee FLOAT,
    payer_plan_period_id BIGINT,
    amount_allowed FLOAT,
    cost FLOAT,
    incurred_date DATE,
    billed_date DATE,
    paid_date DATE
  );")
    
    DatabaseConnector::executeSql(connection, createTableSql)
    
    # --- Populate the new table using SQL transformation ---
    message("- Step 3: Transforming and inserting data using SQL.")
    
    # First, we need to get the event-to-visit mapping
    eventToVisitSql <- glue::glue("
  CREATE TEMPORARY TABLE event_to_visit_map AS
  WITH all_events AS (
    SELECT 
      po.procedure_occurrence_id AS event_id, 
      'Procedure' AS domain_id, 
      po.person_id, 
      po.procedure_date AS event_date, 
      po.visit_occurrence_id, 
      po.procedure_concept_id AS base_concept_id
    FROM {cdmDatabaseSchema}.procedure_occurrence po

    UNION ALL

    SELECT 
      m.measurement_id, 
      'Measurement', 
      m.person_id, 
      m.measurement_date, 
      m.visit_occurrence_id, 
      m.measurement_concept_id
    FROM {cdmDatabaseSchema}.measurement m

    UNION ALL

    SELECT 
      vo.visit_occurrence_id, 
      'Visit', 
      vo.person_id, 
      vo.visit_start_date, 
      vo.visit_occurrence_id, 
      vo.visit_concept_id
    FROM {cdmDatabaseSchema}.visit_occurrence vo

    UNION ALL 

    SELECT 
      vd.visit_detail_id, 
      'Visit Detail', 
      vd.person_id, 
      vd.visit_detail_start_date, 
      vd.visit_occurrence_id, 
      vd.visit_detail_concept_id
    FROM {cdmDatabaseSchema}.visit_detail vd

    UNION ALL

    SELECT 
      de.device_exposure_id, 
      'Device', 
      de.person_id, 
      de.device_exposure_start_date, 
      de.visit_occurrence_id, 
      de.device_concept_id
    FROM {cdmDatabaseSchema}.device_exposure de

    UNION ALL

    SELECT 
      d.drug_exposure_id, 
      'Drug', 
      d.person_id, 
      d.drug_exposure_start_date, 
      d.visit_occurrence_id, 
      d.drug_concept_id
    FROM {cdmDatabaseSchema}.drug_exposure d

    UNION ALL

    SELECT 
      o.observation_id, 
      'Observation', 
      o.person_id, 
      o.observation_date, 
      o.visit_occurrence_id, 
      o.observation_concept_id
    FROM {cdmDatabaseSchema}.observation o

    UNION ALL

    SELECT 
      co.condition_occurrence_id, 
      'Condition', 
      co.person_id, 
      co.condition_start_date, 
      co.visit_occurrence_id, 
      co.condition_concept_id
    FROM {cdmDatabaseSchema}.condition_occurrence co
  )
  SELECT DISTINCT * FROM all_events;
")
    
    DatabaseConnector::executeSql(connection, eventToVisitSql)
    
    # Now run the main transformation
    transformSql <- glue::glue("
  WITH wide_cost AS (
    SELECT DISTINCT
        etv.visit_occurrence_id,
        NULL as visit_detail_id,
        pp.person_id,
        c.cost_domain_id,
        COALESCE(etv.event_date, CURRENT_DATE) AS effective_date,
        1147332 AS cost_event_field_concept_id,
        COALESCE(base_concept_id, 0) AS base_cost_concept_id,
        COALESCE(c.cost_type_concept_id, 31968) AS cost_type_concept_id,
        COALESCE(c.currency_concept_id, 44818668) AS currency_concept_id,
        0 AS cost_source_concept_id,
        'Unknown' AS base_cost_source_value,
        c.total_charge,
        c.total_cost,
        c.total_paid,
        c.paid_by_payer,
        c.paid_by_patient,
        c.paid_patient_copay,
        c.paid_patient_coinsurance,
        c.paid_patient_deductible,
        c.paid_by_primary,
        c.paid_ingredient_cost,
        c.paid_dispensing_fee,
        c.payer_plan_period_id,
        c.amount_allowed,
        etv.event_date as incurred_date,
        NULL as billed_date,
        NULL as paid_date
    FROM {cdmDatabaseSchema}.cost_v5_3_backup c
    INNER JOIN {cdmDatabaseSchema}.payer_plan_period pp 
        ON c.payer_plan_period_id = pp.payer_plan_period_id
    LEFT JOIN event_to_visit_map etv 
        ON c.cost_event_id = etv.event_id AND c.cost_domain_id = etv.domain_id
  ),
  cost_long AS (
    SELECT person_id, visit_occurrence_id, visit_detail_id, cost_domain_id, effective_date,
           cost_event_field_concept_id, cost_type_concept_id, 31973 AS cost_concept_id,
           'total_charge' AS cost_source_value, currency_concept_id, cost_source_concept_id,
           total_charge AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
    FROM wide_cost WHERE total_charge IS NOT NULL 
    
    UNION ALL
    
    SELECT person_id, visit_occurrence_id, visit_detail_id, cost_domain_id, effective_date,
           cost_event_field_concept_id, cost_type_concept_id, 31985 AS cost_concept_id,
           'total_cost' AS cost_source_value, currency_concept_id, cost_source_concept_id,
           total_cost AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
    FROM wide_cost WHERE total_cost IS NOT NULL 
    
    UNION ALL
    
    SELECT person_id, visit_occurrence_id, visit_detail_id, cost_domain_id, effective_date,
           cost_event_field_concept_id, cost_type_concept_id, 31980 AS cost_concept_id,
           'total_paid' AS cost_source_value, currency_concept_id, cost_source_concept_id,
           total_paid AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
    FROM wide_cost WHERE total_paid IS NOT NULL 
    
    UNION ALL
    
    SELECT person_id, visit_occurrence_id, visit_detail_id, cost_domain_id, effective_date,
           cost_event_field_concept_id, cost_type_concept_id, 31980 AS cost_concept_id,
           'paid_by_payer' AS cost_source_value, currency_concept_id, cost_source_concept_id,
           paid_by_payer AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
    FROM wide_cost WHERE paid_by_payer IS NOT NULL 
    
    UNION ALL
    
    SELECT person_id, visit_occurrence_id, visit_detail_id, cost_domain_id, effective_date,
           cost_event_field_concept_id, cost_type_concept_id, 31981 AS cost_concept_id,
           'paid_by_patient' AS cost_source_value, currency_concept_id, cost_source_concept_id,
           paid_by_patient AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
    FROM wide_cost WHERE paid_by_patient IS NOT NULL 
    
    UNION ALL
    
    SELECT person_id, visit_occurrence_id, visit_detail_id, cost_domain_id, effective_date,
           cost_event_field_concept_id, cost_type_concept_id, 31974 AS cost_concept_id,
           'paid_patient_copay' AS cost_source_value, currency_concept_id, cost_source_concept_id,
           paid_patient_copay AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
    FROM wide_cost WHERE paid_patient_copay IS NOT NULL 
    
    UNION ALL
    
    SELECT person_id, visit_occurrence_id, visit_detail_id, cost_domain_id, effective_date,
           cost_event_field_concept_id, cost_type_concept_id, 31975 AS cost_concept_id,
           'paid_patient_coinsurance' AS cost_source_value, currency_concept_id, cost_source_concept_id,
           paid_patient_coinsurance AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
    FROM wide_cost WHERE paid_patient_coinsurance IS NOT NULL 
    
    UNION ALL
    
    SELECT person_id, visit_occurrence_id, visit_detail_id, cost_domain_id, effective_date,
           cost_event_field_concept_id, cost_type_concept_id, 31976 AS cost_concept_id,
           'paid_patient_deductible' AS cost_source_value, currency_concept_id, cost_source_concept_id,
           paid_patient_deductible AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
    FROM wide_cost WHERE paid_patient_deductible IS NOT NULL 
    
    UNION ALL
    
    SELECT person_id, visit_occurrence_id, visit_detail_id, cost_domain_id, effective_date,
           cost_event_field_concept_id, cost_type_concept_id, 31979 AS cost_concept_id,
           'amount_allowed' AS cost_source_value, currency_concept_id, cost_source_concept_id,
           amount_allowed AS cost, payer_plan_period_id, incurred_date, billed_date, paid_date
    FROM wide_cost WHERE amount_allowed IS NOT NULL 
  )
  INSERT INTO {cdmDatabaseSchema}.{sourceCostTable} (
    cost_id, person_id, visit_occurrence_id, visit_detail_id,
    cost_domain_id, effective_date, cost_event_field_concept_id, cost_type_concept_id,
    cost_concept_id, cost_source_value, currency_concept_id, cost_source_concept_id, 
    cost, payer_plan_period_id, incurred_date, billed_date, paid_date
  )
  SELECT
    ROW_NUMBER() OVER (ORDER BY person_id, visit_occurrence_id, 
                        CASE WHEN visit_detail_id IS NULL THEN 1 ELSE 0 END,
                        visit_detail_id) AS cost_id,
    person_id, visit_occurrence_id, visit_detail_id, cost_domain_id, effective_date,
    cost_event_field_concept_id, cost_type_concept_id, cost_concept_id, cost_source_value,
    currency_concept_id, cost_source_concept_id, cost, payer_plan_period_id,
    incurred_date, billed_date, paid_date
  FROM cost_long;")
    
    DatabaseConnector::executeSql(connection, transformSql)
    
    # Clean up temporary table
    DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS event_to_visit_map;")
    
    # --- Create indexes ---
    if (createIndexes) {
      message("- Step 4: Creating indexes on the new cost table.")
      
      indexStatements <- c(
        "CREATE INDEX idx_cost_person_id ON {cdmDatabaseSchema}.{sourceCostTable} (person_id);",
        "CREATE INDEX idx_cost_visit_occurrence_id ON {cdmDatabaseSchema}.{sourceCostTable} (visit_occurrence_id);",
        "CREATE INDEX idx_cost_visit_detail_id ON {cdmDatabaseSchema}.{sourceCostTable} (visit_detail_id);",
        "CREATE INDEX idx_cost_payer_plan_period_id ON {cdmDatabaseSchema}.{sourceCostTable} (payer_plan_period_id);",
        "CREATE INDEX idx_cost_cost_event_id ON {cdmDatabaseSchema}.{sourceCostTable} (cost_event_id);",
        "CREATE INDEX idx_cost_effective_date ON {cdmDatabaseSchema}.{sourceCostTable} (effective_date);",
        "CREATE INDEX idx_cost_incurred_date ON {cdmDatabaseSchema}.{sourceCostTable} (incurred_date);",
        "CREATE INDEX idx_cost_billed_date ON {cdmDatabaseSchema}.{sourceCostTable} (billed_date);",
        "CREATE INDEX idx_cost_paid_date ON {cdmDatabaseSchema}.{sourceCostTable} (paid_date);",
        "CREATE INDEX idx_cost_cost_concept_id ON {cdmDatabaseSchema}.{sourceCostTable} (cost_concept_id);",
        "CREATE INDEX idx_cost_cost_type_concept_id ON {cdmDatabaseSchema}.{sourceCostTable} (cost_type_concept_id);",
        "CREATE INDEX idx_cost_person_date ON {cdmDatabaseSchema}.{sourceCostTable} (person_id, effective_date);",
        "CREATE INDEX idx_cost_visit_concept ON {cdmDatabaseSchema}.{sourceCostTable} (visit_occurrence_id, cost_concept_id);",
        "CREATE INDEX idx_cost_domain_id ON {cdmDatabaseSchema}.{sourceCostTable} (cost_domain_id);"
      )
      
      for (idx in indexStatements) {
        tryCatch({
          DatabaseConnector::executeSql(connection, glue::glue(idx))
        }, error = function(e) {
          warning(glue::glue("Failed to create index: {e$message}"))
        })
      }
    }
    
    message(glue::glue("Successfully transformed '{sourceCostTable}' table to CDM v5.5 long format."))

  return(connection)
}
