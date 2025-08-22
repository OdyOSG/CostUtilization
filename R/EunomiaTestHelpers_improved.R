#' Inject Synthetic Payer Plan and Wide-Format Cost Data into an OMOP CDM (Eunomia) Instance
#' 
#' @description
#' Creates synthetic payer plan periods and cost records for testing purposes.
#' This function generates realistic healthcare cost data that can be used for
#' development and testing of cost analysis functions.
#' 
#' @param connection A DatabaseConnector connection object
#' @param seed Random seed for reproducibility (default: 123)
#' @param cdmDatabaseSchema Schema name containing CDM tables (default: "main")
#' 
#' @return Invisibly returns the connection object
#' @export
#' 
#' @examples
#' \dontrun{
#' connection <- DatabaseConnector::connect(connectionDetails)
#' injectCostData(connection, seed = 42)
#' }
injectCostData <- function(connection, seed = 123, cdmDatabaseSchema = "main") {
  # Input validation using rlang
  rlang::check_required(connection)
  checkmate::assert_class(connection, "DatabaseConnectorConnection")
  checkmate::assert_int(seed)
  checkmate::assert_string(cdmDatabaseSchema)
  
  cli::cli_alert_info("Generating synthetic payer_plan_period and wide-format cost data")
  
  # Step 1: Create payer plan periods
  cli::cli_alert_info("Step 1: Creating realistic payer_plan_period table")
  
  # Fetch observation periods with error handling
  obs_periods_sql <- glue::glue(
    "SELECT person_id, observation_period_start_date, observation_period_end_date 
     FROM {cdmDatabaseSchema}.observation_period"
  )
  
  person_obs_periods <- tryCatch({
    DatabaseConnector::querySql(connection, obs_periods_sql) |>
      dplyr::rename_with(tolower) |>
      dplyr::mutate(dplyr::across(dplyr::ends_with("_date"), as.Date)) |>
      dplyr::mutate(
        start_year = as.integer(format(observation_period_start_date, "%Y")),
        end_year = as.integer(format(observation_period_end_date, "%Y"))
      )
  }, error = function(e) {
    rlang::abort(
      "Failed to fetch observation periods",
      parent = e,
      cdmDatabaseSchema = cdmDatabaseSchema
    )
  })
  
  if (nrow(person_obs_periods) == 0) {
    rlang::warn("No observation periods found in the database")
    return(invisible(connection))
  }
  
  set.seed(seed)
  
  # Define plan options
  plan_options <- c(
    "PPO-Low-Deductible-500", 
    "HMO-Standard-1500", 
    "PPO-High-Deductible-2500", 
    "EPO-Basic-3000"
  )
  
  # Generate payer plans using purrr
  payer_plans_df <- person_obs_periods |>
    purrr::pmap_dfr(generate_random_plans_for_person, plan_options = plan_options) |>
    dplyr::arrange(.data$person_id, .data$plan_start_date) |>
    dplyr::mutate(
      payerPlanPeriodId = dplyr::row_number(),
      payerSourceValue = dplyr::case_when(
        grepl("PPO", plan_name) ~ "Commercial PPO",
        grepl("HMO", plan_name) ~ "Managed Care HMO",
        grepl("EPO", plan_name) ~ "Exclusive Provider Organization",
        TRUE ~ "Other Insurance"
      ),
      planSourceValue = .data$plan_name
    ) |>
    dplyr::select(
      payerPlanPeriodId,
      person_id,
      payerPlanPeriodStartDate = plan_start_date,
      payerPlanPeriodEndDate = plan_end_date,
      payerSourceValue,
      planSourceValue
    )
  
  # Step 2: Fetch clinical events
  cli::cli_alert_info("Step 2: Fetching clinical events and mapping to plan periods")
  
  cost_structure <- list(
    "Procedure" = c("procedure_occurrence", "procedure_occurrence_id", "person_id", "procedure_date"),
    "Measurement" = c("measurement", "measurement_id", "person_id", "measurement_date"),
    "Visit" = c("visit_occurrence", "visit_occurrence_id", "person_id", "visit_start_date"),
    "Device" = c("device_exposure", "device_exposure_id", "person_id", "device_exposure_start_date"),
    "Drug" = c("drug_exposure", "drug_exposure_id", "person_id", "drug_exposure_start_date"),
    "Observation" = c("observation", "observation_id", "person_id", "observation_date"),
    "Condition" = c("condition_occurrence", "condition_occurrence_id", "person_id", "condition_start_date")
  )
  
  # Fetch events using purrr::map2_dfr with error handling
  event_table <- purrr::map2_dfr(
    names(cost_structure), 
    cost_structure, 
    fetch_domain_events,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    payer_plans_df = payer_plans_df
  )
  
  # Step 3: Generate cost records
  cli::cli_alert_info("Step 3: Generating synthetic cost values")
  
  n_records <- nrow(event_table)
  
  if (n_records > 0) {
    set.seed(seed)
    cost_records_df <- generate_cost_records(event_table, n_records)
  } else {
    cli::cli_alert_info("No clinical events found. Skipping cost generation")
    cost_records_df <- data.frame()
  }
  
  # Step 4: Insert tables into database
  cli::cli_alert_info("Step 4: Inserting new tables into the database")
  
  insert_table_safe(
    connection = connection,
    tableName = "payer_plan_period",
    data = payer_plans_df,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    camelCaseToSnakeCase = TRUE
  )
  
  if (nrow(cost_records_df) > 0) {
    insert_table_safe(
      connection = connection,
      tableName = "cost",
      data = cost_records_df,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      camelCaseToSnakeCase = TRUE
    )
  }
  
  cli::cli_alert_success("Successfully injected synthetic payer_plan_period and wide-format cost tables")
  return(invisible(connection))
}

#' Transform Wide-Format Cost Table to OMOP CDM v5.5 Long Format
#'
#' @description
#' Transforms a wide-format cost table (common in OMOP CDM v5.3 and earlier) 
#' into the standard long format required in CDM v5.4+.
#'
#' @param connectionDetails DatabaseConnector connectionDetails object
#' @param cdmDatabaseSchema Schema name containing CDM tables (default: "main")
#' @param sourceCostTable Name of the wide-format cost table (default: "cost")
#' @param createIndexes Whether to create indexes on the new table (default: TRUE)
#'
#' @return Invisibly returns the database connection after successful execution
#' @export
#'
#' @examples
#' \dontrun{
#' con <- transformCostToCdmV5dot5(Eunomia::getEunomiaConnectionDetails())
#' }
transformCostToCdmV5dot5 <- function(
    connectionDetails,
    cdmDatabaseSchema = "main",
    sourceCostTable = "cost",
    createIndexes = TRUE) {
  
  # Input validation
  rlang::check_required(connectionDetails)
  checkmate::assert_class(connectionDetails, "ConnectionDetails")
  checkmate::assert_string(cdmDatabaseSchema)
  checkmate::assert_string(sourceCostTable)
  checkmate::assert_flag(createIndexes)
  
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Ensure cost data exists
  connection <- injectCostData(connection)
  
  cli::cli_alert_info(glue::glue(
    "Starting transformation of wide '{sourceCostTable}' table to long format"
  ))
  
  # Pre-flight checks
  tables_in_db <- tolower(DatabaseConnector::getTableNames(connection, cdmDatabaseSchema))
  
  if (!tolower(sourceCostTable) %in% tables_in_db) {
    rlang::abort(glue::glue(
      "Source cost table '{cdmDatabaseSchema}.{sourceCostTable}' not found"
    ))
  }
  
  # Step 1: Backup existing table
  cli::cli_alert_info("Step 1: Backing up existing cost table")
  backup_cost_table(connection, cdmDatabaseSchema, sourceCostTable, tables_in_db)
  
  # Step 2: Create new cost table with CDM v5.5 structure
  cli::cli_alert_info("Step 2: Creating new cost table with CDM v5.5 structure")
  create_cdm_v55_cost_table(connection, cdmDatabaseSchema, sourceCostTable)
  
  # Step 3: Transform and insert data
  cli::cli_alert_info("Step 3: Transforming and inserting data using SQL")
  transform_cost_data(connection, cdmDatabaseSchema, sourceCostTable)
  
  # Step 4: Create indexes
  if (createIndexes) {
    cli::cli_alert_info("Step 4: Creating indexes on the new cost table")
    create_cost_indexes(connection, cdmDatabaseSchema, sourceCostTable)
  }
  
  cli::cli_alert_success(glue::glue(
    "Successfully transformed '{sourceCostTable}' table to CDM v5.5 long format"
  ))
  
  return(invisible(connection))
}

# Helper functions ----

#' Generate random plan periods for a person
#' @noRd
generate_random_plans_for_person <- function(person_id, observation_period_start_date, 
                                            observation_period_end_date, start_year, 
                                            end_year, plan_options, ...) {
  possible_change_years <- (start_year + 1):end_year
  n_possible_changes <- length(possible_change_years)
  
  if (n_possible_changes == 0) {
    n_segments <- 1
    change_dates <- as.Date(character())
  } else {
    max_changes <- min(n_possible_changes, 2)
    probs <- c(0.6, 0.3, 0.1)[1:(max_changes + 1)]
    probs <- probs / sum(probs)
    num_changes <- sample(0:max_changes, 1, prob = probs)
    
    if (num_changes == 0) {
      n_segments <- 1
      change_dates <- as.Date(character())
    } else {
      n_segments <- num_changes + 1
      change_years <- sort(sample(possible_change_years, num_changes))
      change_dates <- as.Date(paste0(change_years, "-01-01"))
    }
  }
  
  starts <- c(observation_period_start_date, change_dates)
  ends <- c(if (length(change_dates) > 0) change_dates - 1 else NULL, 
            observation_period_end_date)
  
  assigned_plans <- sample(plan_options, n_segments, replace = TRUE)
  
  data.frame(
    person_id = person_id,
    plan_start_date = starts,
    plan_end_date = ends,
    plan_name = assigned_plans
  )
}

#' Fetch domain events
#' @noRd
fetch_domain_events <- function(domain_name, domain_info, connection, 
                               cdmDatabaseSchema, payer_plans_df) {
  sql <- glue::glue(
    "SELECT t1.{domain_info[[2]]} as event_id, 
            t1.{domain_info[[3]]} as person_id, 
            t1.{domain_info[[4]]} as event_date 
     FROM {cdmDatabaseSchema}.{domain_info[[1]]} t1"
  )
  
  tryCatch({
    domain_table <- DatabaseConnector::querySql(connection, sql) |>
      dplyr::rename_with(tolower) |>
      dplyr::mutate(
        domain_id = domain_name, 
        event_date = as.Date(.data$event_date)
      )
    
    domain_table |>
      dplyr::left_join(payer_plans_df, by = "person_id", relationship = "many-to-many") |>
      dplyr::filter(
        .data$event_date >= .data$payerPlanPeriodStartDate & 
        .data$event_date <= .data$payerPlanPeriodEndDate
      )
  }, error = function(e) {
    rlang::warn(glue::glue("Failed to fetch {domain_name} events: {e$message}"))
    data.frame()
  })
}

#' Generate cost records
#' @noRd
generate_cost_records <- function(event_table, n_records) {
  base_cost <- stats::runif(n_records, 23, 1500)
  total_charge <- round(base_cost * stats::runif(n_records, 1.1, 1.5), 2)
  total_cost <- round(base_cost * stats::runif(n_records, 0.7, 0.9), 2)
  insurance_coverage <- stats::runif(n_records, 0.54, 1)
  paid_by_payer <- round(total_cost * insurance_coverage, 2)
  paid_by_patient <- round(total_cost - paid_by_payer, 2)
  
  dplyr::tibble(
    costId = 1:n_records,
    costEventId = event_table$event_id,
    costDomainId = event_table$domain_id,
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
    payerPlanPeriodId = event_table$payerPlanPeriodId,
    amountAllowed = total_cost,
    revenueCodeConceptId = 0,
    revenueCodeSourceValue = NA_character_,
    drgConceptId = 0,
    drgSourceValue = NA_character_
  )
}

#' Safely insert table with error handling
#' @noRd
insert_table_safe <- function(connection, tableName, data, ...) {
  tryCatch({
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = tableName,
      data = data,
      ...
    )
  }, error = function(e) {
    rlang::abort(
      glue::glue("Failed to insert table '{tableName}'"),
      parent = e
    )
  })
}

#' Backup cost table
#' @noRd
backup_cost_table <- function(connection, cdmDatabaseSchema, sourceCostTable, tables_in_db) {
  backup_table_name <- glue::glue("{cdmDatabaseSchema}.cost_v5_3_backup")
  full_cost_table_name <- glue::glue("{cdmDatabaseSchema}.{sourceCostTable}")
  
  if ("cost_v5_3_backup" %in% tables_in_db) {
    DatabaseConnector::executeSql(connection, glue::glue("DROP TABLE {backup_table_name};"))
  }
  
  DatabaseConnector::executeSql(connection, glue::glue(
    "ALTER TABLE {full_cost_table_name} RENAME TO cost_v5_3_backup;"
  ))
  
  cli::cli_alert_info(glue::glue(
    "Original '{sourceCostTable}' table renamed to 'cost_v5_3_backup'"
  ))
}

#' Create CDM v5.5 cost table structure
#' @noRd
create_cdm_v55_cost_table <- function(connection, cdmDatabaseSchema, sourceCostTable) {
  create_table_sql <- glue::glue("
  CREATE TABLE {cdmDatabaseSchema}.{sourceCostTable} (
    cost_id BIGINT PRIMARY KEY,
    person_id BIGINT NOT NULL,
    cost_event_id BIGINT,
    cost_event_field_concept_id INT DEFAULT 1147332,
    visit_occurrence_id BIGINT,
    visit_detail_id BIGINT,
    cost_domain_id VARCHAR(50) NULL,
    cost_type_concept_id INT DEFAULT 31968,
    cost_concept_id INT NOT NULL,
    cost_source_concept_id INT,
    cost_source_value VARCHAR(50),
    currency_concept_id INT DEFAULT 44818668,
    cost FLOAT,
    incurred_date DATE,
    billed_date DATE,
    paid_date DATE,
    revenue_code_concept_id INT,
    drg_concept_id INT,
    revenue_code_source_value VARCHAR(50),
    drg_source_value VARCHAR(50),
    payer_plan_period_id BIGINT
  );")
  
  DatabaseConnector::executeSql(connection, create_table_sql)
}

#' Transform cost data to long format
#' @noRd
transform_cost_data <- function(connection, cdmDatabaseSchema, sourceCostTable) {
  # Create event-to-visit mapping
  event_to_visit_sql <- generate_event_to_visit_sql(cdmDatabaseSchema)
  DatabaseConnector::executeSql(connection, event_to_visit_sql)
  
  # Transform to long format
  transform_sql <- generate_transform_sql(cdmDatabaseSchema, sourceCostTable)
  DatabaseConnector::executeSql(connection, transform_sql)
  
  # Clean up temporary table
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS event_to_visit_map;")
}

#' Generate event-to-visit mapping SQL
#' @noRd
generate_event_to_visit_sql <- function(cdmDatabaseSchema) {
  glue::glue("
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
}

#' Generate transformation SQL
#' @noRd
generate_transform_sql <- function(cdmDatabaseSchema, sourceCostTable) {
  glue::glue("
  WITH wide_cost AS (
    SELECT DISTINCT
        c.cost_event_id,
        etv.visit_occurrence_id,
        NULL as visit_detail_id,
        pp.person_id,
        c.cost_domain_id,
        COALESCE(etv.event_date, CURRENT_DATE) AS incurred_date,
        1147332 AS cost_event_field_concept_id,
        COALESCE(c.cost_type_concept_id, 31968) AS cost_type_concept_id,
        COALESCE(c.currency_concept_id, 44818668) AS currency_concept_id,
        0 AS cost_source_concept_id,
        c.total_charge,
        c.total_cost,
        c.total_paid,
        c.paid_by_payer,
        c.paid_by_patient,
        c.paid_patient_copay,
        c.paid_patient_coinsurance,
        c.paid_patient_deductible,
        c.paid_by_primary,
        c.amount_allowed,
        c.payer_plan_period_id,
        c.revenue_code_concept_id,
        c.drg_concept_id,
        c.revenue_code_source_value,
        c.drg_source_value
    FROM {cdmDatabaseSchema}.cost_v5_3_backup c
    INNER JOIN {cdmDatabaseSchema}.payer_plan_period pp
        ON c.payer_plan_period_id = pp.payer_plan_period_id
    LEFT JOIN event_to_visit_map etv
        ON c.cost_event_id = etv.event_id AND c.cost_domain_id = etv.domain_id
  ),
  cost_long AS (
    SELECT person_id, cost_event_id, visit_occurrence_id, visit_detail_id, 
           cost_domain_id, incurred_date, cost_event_field_concept_id, 
           cost_type_concept_id, 31973 AS cost_concept_id,
           'total_charge' AS cost_source_value, currency_concept_id, 
           cost_source_concept_id, total_charge AS cost, payer_plan_period_id,
           revenue_code_concept_id, drg_concept_id, revenue_code_source_value, 
           drg_source_value, incurred_date as billed_date, NULL as paid_date
    FROM wide_cost WHERE total_charge IS NOT NULL

    UNION ALL

    SELECT person_id, cost_event_id, visit_occurrence_id, visit_detail_id,
           cost_domain_id, incurred_date, cost_event_field_concept_id,
           cost_type_concept_id, 31985 AS cost_concept_id,
           'total_cost' AS cost_source_value, currency_concept_id,
           cost_source_concept_id, total_cost AS cost, payer_plan_period_id,
           revenue_code_concept_id, drg_concept_id, revenue_code_source_value,
           drg_source_value, incurred_date as billed_date, NULL as paid_date
    FROM wide_cost WHERE total_cost IS NOT NULL

    UNION ALL

    SELECT person_id, cost_event_id, visit_occurrence_id, visit_detail_id,
           cost_domain_id, incurred_date, cost_event_field_concept_id,
           cost_type_concept_id, 31980 AS cost_concept_id,
           'paid_by_payer' AS cost_source_value, currency_concept_id,
           cost_source_concept_id, paid_by_payer AS cost, payer_plan_period_id,
           revenue_code_concept_id, drg_concept_id, revenue_code_source_value,
           drg_source_value, NULL as billed_date, incurred_date as paid_date
    FROM wide_cost WHERE paid_by_payer IS NOT NULL

    UNION ALL

    SELECT person_id, cost_event_id, visit_occurrence_id, visit_detail_id,
           cost_domain_id, incurred_date, cost_event_field_concept_id,
           cost_type_concept_id, 31981 AS cost_concept_id,
           'paid_by_patient' AS cost_source_value, currency_concept_id,
           cost_source_concept_id, paid_by_patient AS cost, payer_plan_period_id,
           revenue_code_concept_id, drg_concept_id, revenue_code_source_value,
           drg_source_value, NULL as billed_date, incurred_date as paid_date
    FROM wide_cost WHERE paid_by_patient IS NOT NULL

    UNION ALL

    SELECT person_id, cost_event_id, visit_occurrence_id, visit_detail_id,
           cost_domain_id, incurred_date, cost_event_field_concept_id,
           cost_type_concept_id, 31979 AS cost_concept_id,
           'amount_allowed' AS cost_source_value, currency_concept_id,
           cost_source_concept_id, amount_allowed AS cost, payer_plan_period_id,
           revenue_code_concept_id, drg_concept_id, revenue_code_source_value,
           drg_source_value, incurred_date as billed_date, NULL as paid_date
    FROM wide_cost WHERE amount_allowed IS NOT NULL
  )
  INSERT INTO {cdmDatabaseSchema}.{sourceCostTable} (
    cost_id, person_id, cost_event_id, cost_event_field_concept_id,
    visit_occurrence_id, visit_detail_id, cost_domain_id, cost_type_concept_id,
    cost_concept_id, cost_source_value, currency_concept_id, cost_source_concept_id,
    cost, incurred_date, billed_date, paid_date, payer_plan_period_id,
    revenue_code_concept_id, drg_concept_id, revenue_code_source_value, drg_source_value
  )
  SELECT
    ROW_NUMBER() OVER (ORDER BY person_id, visit_occurrence_id, cost_event_id) AS cost_id,
    person_id, cost_event_id, cost_event_field_concept_id, visit_occurrence_id,
    visit_detail_id, cost_domain_id, cost_type_concept_id, cost_concept_id,
    cost_source_value, currency_concept_id, cost_source_concept_id, cost,
    incurred_date, billed_date, paid_date, payer_plan_period_id,
    revenue_code_concept_id, drg_concept_id, revenue_code_source_value, drg_source_value
  FROM cost_long;")
}

#' Create indexes on cost table
#' @noRd
create_cost_indexes <- function(connection, cdmDatabaseSchema, sourceCostTable) {
  index_statements <- c(
    "CREATE INDEX idx_cost_person_id ON {cdmDatabaseSchema}.{sourceCostTable} (person_id);",
    "CREATE INDEX idx_cost_visit_occurrence_id ON {cdmDatabaseSchema}.{sourceCostTable} (visit_occurrence_id);",
    "CREATE INDEX idx_cost_visit_detail_id ON {cdmDatabaseSchema}.{sourceCostTable} (visit_detail_id);",
    "CREATE INDEX idx_cost_payer_plan_period_id ON {cdmDatabaseSchema}.{sourceCostTable} (payer_plan_period_id);",
    "CREATE INDEX idx_cost_cost_event_id ON {cdmDatabaseSchema}.{sourceCostTable} (cost_event_id);",
    "CREATE INDEX idx_cost_incurred_date ON {cdmDatabaseSchema}.{sourceCostTable} (incurred_date);",
    "CREATE INDEX idx_cost_billed_date ON {cdmDatabaseSchema}.{sourceCostTable} (billed_date);",
    "CREATE INDEX idx_cost_paid_date ON {cdmDatabaseSchema}.{sourceCostTable} (paid_date);",
    "CREATE INDEX idx_cost_cost_concept_id ON {cdmDatabaseSchema}.{sourceCostTable} (cost_concept_id);",
    "CREATE INDEX idx_cost_cost_type_concept_id ON {cdmDatabaseSchema}.{sourceCostTable} (cost_type_concept_id);",
    "CREATE INDEX idx_cost_person_date ON {cdmDatabaseSchema}.{sourceCostTable} (person_id, incurred_date);",
    "CREATE INDEX idx_cost_visit_concept ON {cdmDatabaseSchema}.{sourceCostTable} (visit_occurrence_id, cost_concept_id);",
    "CREATE INDEX idx_cost_domain_id ON {cdmDatabaseSchema}.{sourceCostTable} (cost_domain_id);"
  )
  
  purrr::walk(index_statements, function(idx) {
    tryCatch({
      DatabaseConnector::executeSql(connection, glue::glue(idx))
    }, error = function(e) {
      # Fixed bug: was {e$cli::cli_alert_info}, now {e$message}
      rlang::warn(glue::glue("Failed to create index: {e$message}"))
    })
  })
}