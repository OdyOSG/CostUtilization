#' Inject Synthetic Payer Plan and Wide-Format Cost Data into an OMOP CDM
#'
#' Creates synthetic `payer_plan_period` and wide-format `cost` tables for
#' testing, primarily for use with Eunomia datasets.
#'
#' @param connection A database connection from `DatabaseConnector::connect()`.
#' @param cdmDatabaseSchema The name of the CDM database schema.
#' @param costDomains A character vector of domains for which to generate costs.
#' @param seed An integer seed for reproducible data generation.
#'
#' @return Invisibly returns the database connection.
#' @export
#'
#' @examples
#' \dontrun{
#' connectionDetails <- Eunomia::getEunomiaConnectionDetails()
#' connection <- DatabaseConnector::connect(connectionDetails)
#' injectCostData(connection)
#' DatabaseConnector::disconnect(connection)
#' }
injectCostData <- function(connection,
                           cdmDatabaseSchema = "main",
                           costDomains = c("Procedure", "Measurement", "Visit",
                                           "Device", "Drug", "Observation", "Condition"),
                           seed = 123) {
  # Input validation
  checkmate::assertClass(connection, "DatabaseConnectorConnection")
  checkmate::assertString(cdmDatabaseSchema)
  checkmate::assertCharacter(costDomains, min.len = 1)
  checkmate::assertInt(seed)
  
  cli::cli_h2("Generating synthetic payer_plan_period and cost data")
  
  # Step 1: Create payer_plan_period table
  cli::cli_alert_info("Creating payer_plan_period table")
  
  obsPeriodsSql <- glue::glue("
    SELECT
      person_id,
      observation_period_start_date,
      observation_period_end_date
    FROM {cdmDatabaseSchema}.observation_period
  ")
  
  personObsPeriods <- DatabaseConnector::querySql(connection, obsPeriodsSql) |>
    dplyr::rename_with(tolower) |>
    dplyr::mutate(
      dplyr::across(dplyr::ends_with("_date"), as.Date)
    )
  
  if (nrow(personObsPeriods) == 0) {
    cli::cli_alert_warning("No observation periods found. Skipping cost data injection.")
    return(invisible(connection))
  }
  
  payerPlansDf <- generatePayerPlans(personObsPeriods, seed = seed)
  
  # Step 2: Fetch clinical events
  cli::cli_alert_info("Fetching clinical events and mapping to plan periods")
  
  eventTable <- fetchClinicalEvents(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    costDomains = costDomains,
    payerPlansDf = payerPlansDf
  )
  
  # Step 3: Generate cost records
  cli::cli_alert_info("Generating synthetic cost values")
  costRecordsDf <- generateCostRecords(eventTable, seed = seed)
  
  # Step 4: Insert tables
  cli::cli_alert_info("Inserting tables into database")
  insertTablesSafely(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    payerPlansDf = payerPlansDf,
    costRecordsDf = costRecordsDf
  )
  
  cli::cli_alert_success("Successfully injected synthetic payer_plan_period and cost tables.")
  invisible(connection)
}

#' Transform Wide-Format Cost Table to OMOP CDM v5.4+ Long Format
#'
#' Transforms a wide-format cost table into the standard long format, backing
#' up the original table and creating a new version with proper indexes.
#'
#' @param connectionDetails Connection details from `DatabaseConnector`.
#' @param cdmDatabaseSchema Name of the CDM database schema.
#' @param sourceCostTable Name of the wide-format cost table to transform.
#' @param createIndexes Logical; if TRUE, create indexes on the new table.
#'
#' @return Invisibly returns the database connection.
#' @export
#'
#' @examples
#' \dontrun{
#' connectionDetails <- Eunomia::getEunomiaConnectionDetails()
#' transformCostToCdmV5p4(connectionDetails)
#' }
transformCostToCdmV5p4 <- function(connectionDetails,
                                   cdmDatabaseSchema = "main",
                                   sourceCostTable = "cost",
                                   createIndexes = TRUE) {
  # Input validation
  checkmate::assertClass(connectionDetails, "connectionDetails")
  checkmate::assertString(cdmDatabaseSchema)
  checkmate::assertString(sourceCostTable)
  checkmate::assertFlag(createIndexes)
  
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  # Ensure cost data exists before attempting transformation
  tablesInDb <- tolower(DatabaseConnector::getTableNames(connection, cdmDatabaseSchema))
  if (!tolower(sourceCostTable) %in% tablesInDb) {
    cli::cli_alert_info("Source cost table not found. Running `injectCostData()` first.")
    injectCostData(connection, cdmDatabaseSchema = cdmDatabaseSchema)
    # Re-check after injection
    tablesInDb <- tolower(DatabaseConnector::getTableNames(connection, cdmDatabaseSchema))
    if (!tolower(sourceCostTable) %in% tablesInDb) {
      rlang::abort(glue::glue(
        "Source cost table '{cdmDatabaseSchema}.{sourceCostTable}' still not found after injection."
      ))
    }
  }
  
  cli::cli_h2("Transforming wide cost table to CDM v5.4+ long format")
  
  # Step 1: Backup existing table
  cli::cli_alert_info("Backing up existing cost table")
  backupCostTable(connection, cdmDatabaseSchema, sourceCostTable, tablesInDb)
  
  # Step 2: Create new table structure
  cli::cli_alert_info("Creating new cost table with CDM v5.4+ structure")
  createV5p4CostTable(connection, cdmDatabaseSchema, sourceCostTable)
  
  # Step 3: Transform and insert data
  cli::cli_alert_info("Transforming and inserting data")
  transformAndInsertCostData(connection, cdmDatabaseSchema, sourceCostTable)
  
  # Step 4: Create indexes
  if (createIndexes) {
    cli::cli_alert_info("Creating indexes on the new cost table")
    createCostIndexes(connection, cdmDatabaseSchema, sourceCostTable)
  }
  
  cli::cli_alert_success("Successfully transformed cost table to CDM v5.4+ long format.")
  invisible(connection)
}


# Helper Functions --------------------------------------------------------

#' Generate Payer Plans (Vectorized)
#' @noRd
generatePayerPlans <- function(personObsPeriods, seed = 123) {
  withr::with_seed(seed, {
    planOptions <- c(
      "PPO-Low-Deductible-500", "HMO-Standard-1500", "PPO-High-Deductible-2500",
      "EPO-Basic-3000", "POS-Premium-1000", "HDHP-HSA-3500"
    )
    
    personPlans <- personObsPeriods |>
      dplyr::mutate(
        startYear = lubridate::year(observation_period_start_date),
        endYear = lubridate::year(observation_period_end_date),
        yearsSpanned = endYear - startYear,
        initialPlan = sample(planOptions, dplyr::n(), replace = TRUE)
      ) |>
      dplyr::filter(yearsSpanned > 0) |>
      dplyr::mutate(
        possibleYears = purrr::map2(startYear, endYear, ~ seq(.x + 1, .y)),
        maxChanges = pmin(yearsSpanned, 3)
      ) |>
      dplyr::mutate(
        nChanges = purrr::map_int(maxChanges, ~ sample(0:.x, 1, prob = c(0.6, 0.25, 0.1, 0.05)[1:(.x + 1)]))
      ) |>
      dplyr::filter(nChanges > 0) |>
      dplyr::mutate(
        changeYears = purrr::map2(possibleYears, nChanges, sample, replace = FALSE),
        changeDates = purrr::map(changeYears, ~ as.Date(paste0(sort(.x), "-01-01")))
      ) |>
      dplyr::select(person_id, observation_period_start_date, observation_period_end_date, initialPlan, changeDates) |>
      tidyr::unnest(changeDates) |>
      dplyr::group_by(person_id) |>
      dplyr::arrange(changeDates) |>
      dplyr::mutate(
        planStartDate = dplyr::coalesce(dplyr::lag(changeDates), observation_period_start_date),
        planEndDate = changeDates - 1
      ) |>
      dplyr::ungroup()
    
    # Combine periods with changes and those without
    finalPlans <- personObsPeriods |>
      dplyr::mutate(initialPlan = sample(planOptions, dplyr::n(), replace = TRUE)) |>
      dplyr::anti_join(personPlans, by = "person_id") |>
      dplyr::select(
        person_id,
        planStartDate = observation_period_start_date,
        planEndDate = observation_period_end_date,
        initialPlan
      ) |>
      dplyr::bind_rows(
        personPlans |>
          dplyr::select(person_id, planStartDate, planEndDate, initialPlan)
      ) |>
      # Add final period for those with changes
      dplyr::bind_rows(
        personPlans |>
          dplyr::group_by(person_id) |>
          dplyr::summarise(
            planStartDate = max(changeDates),
            planEndDate = max(observation_period_end_date),
            initialPlan = dplyr::first(initialPlan)
          )
      ) |>
      dplyr::arrange(person_id, planStartDate) |>
      dplyr::group_by(person_id) |>
      # Assign plan names with some continuity
      dplyr::mutate(
        planName = purrr::accumulate(
          .x = dplyr::row_number()[-1],
          .f = ~ if (runif(1) < 0.3) .x else sample(setdiff(planOptions, .x), 1),
          .init = dplyr::first(initialPlan)
        )
      ) |>
      dplyr::ungroup() |>
      dplyr::mutate(
        payerPlanPeriodId = dplyr::row_number(),
        payerSourceValue = dplyr::case_when(
          grepl("PPO", planName) ~ "Commercial PPO",
          grepl("HMO", planName) ~ "Managed Care HMO",
          grepl("EPO", planName) ~ "Exclusive Provider Organization",
          grepl("POS", planName) ~ "Point of Service",
          grepl("HDHP", planName) ~ "High Deductible Health Plan",
          TRUE ~ "Other Insurance"
        ),
        planSourceValue = planName
      ) |>
      dplyr::select(
        payerPlanPeriodId,
        person_id,
        payerPlanPeriodStartDate = planStartDate,
        payerPlanPeriodEndDate = planEndDate,
        payerSourceValue,
        planSourceValue
      )
  })
  return(finalPlans)
}


#' Fetch Clinical Events
#' @noRd
fetchClinicalEvents <- function(connection, cdmDatabaseSchema, costDomains, payerPlansDf) {
  costStructure <- list(
    Procedure = c("procedure_occurrence", "procedure_occurrence_id", "person_id", "procedure_date"),
    Measurement = c("measurement", "measurement_id", "person_id", "measurement_date"),
    Visit = c("visit_occurrence", "visit_occurrence_id", "person_id", "visit_start_date"),
    Device = c("device_exposure", "device_exposure_id", "person_id", "device_exposure_start_date"),
    Drug = c("drug_exposure", "drug_exposure_id", "person_id", "drug_exposure_start_date"),
    Observation = c("observation", "observation_id", "person_id", "observation_date"),
    Condition = c("condition_occurrence", "condition_occurrence_id", "person_id", "condition_start_date")
  )
  costStructure <- costStructure[names(costStructure) %in% costDomains]
  
  unionQueries <- purrr::imap_chr(costStructure, function(cols, domain) {
    glue::glue("
      SELECT
        {cols[2]} AS event_id,
        '{domain}' AS domain_id,
        {cols[3]} AS person_id,
        {cols[4]} AS event_date
      FROM {cdmDatabaseSchema}.{cols[1]}
    ")
  })
  fullQuery <- paste(unionQueries, collapse = "\nUNION ALL\n")
  
  eventTable <- DatabaseConnector::querySql(connection, fullQuery) |>
    dplyr::rename_with(tolower) |>
    dplyr::mutate(event_date = as.Date(event_date))
  
  eventTableWithPlans <- eventTable |>
    dplyr::inner_join(payerPlansDf, by = "person_id", relationship = "many-to-many") |>
    dplyr::filter(
      event_date >= payerPlanPeriodStartDate,
      event_date <= payerPlanPeriodEndDate
    )
  
  return(eventTableWithPlans)
}

#' Generate Cost Records
#' @noRd
generateCostRecords <- function(eventTable, seed = 123) {
  nRecords <- nrow(eventTable)
  if (nRecords == 0) {
    cli::cli_alert_warning("No clinical events found. Returning empty cost records.")
    return(dplyr::tibble())
  }
  
  withr::with_seed(seed, {
    domainMultipliers <- c(
      Procedure = 5.0, Visit = 3.0, Drug = 2.0, Device = 4.0,
      Measurement = 1.5, Observation = 1.0, Condition = 2.5
    )
    domainMult <- domainMultipliers[eventTable$domain_id]
    domainMult[is.na(domainMult)] <- 1.0
    
    baseCost <- stats::rlnorm(nRecords, meanlog = 4, sdlog = 1.5) * domainMult
    baseCost <- pmin(baseCost, 50000)
    
    totalCharge <- round(baseCost * stats::runif(nRecords, 1.2, 2.5), 2)
    totalCost <- round(baseCost * stats::runif(nRecords, 0.6, 1.0), 2)
    
    planCoverage <- dplyr::case_when(
      grepl("PPO-Low", eventTable$planSourceValue) ~ stats::runif(nRecords, 0.80, 0.95),
      grepl("HMO", eventTable$planSourceValue) ~ stats::runif(nRecords, 0.75, 0.90),
      grepl("High-Deductible", eventTable$planSourceValue) ~ stats::runif(nRecords, 0.60, 0.80),
      grepl("HDHP", eventTable$planSourceValue) ~ stats::runif(nRecords, 0.50, 0.75),
      TRUE ~ stats::runif(nRecords, 0.65, 0.85)
    )
    
    paidByPayer <- round(totalCost * planCoverage, 2)
    paidByPatient <- round(totalCost - paidByPayer, 2)
    
    copayAmount <- dplyr::case_when(
      eventTable$domain_id == "Visit" ~ sample(c(25, 35, 50), nRecords, replace = TRUE),
      eventTable$domain_id == "Drug" ~ sample(c(10, 25, 40), nRecords, replace = TRUE),
      TRUE ~ 0
    )
    
    paidPatientCopay <- pmin(paidByPatient, copayAmount)
    remainingPatient <- paidByPatient - paidPatientCopay
    paidPatientCoinsurance <- round(remainingPatient * 0.8, 2)
    paidPatientDeductible <- round(remainingPatient * 0.2, 2)
    
    costRecordsDf <- dplyr::tibble(
      costId = seq_len(nRecords),
      costEventId = eventTable$event_id,
      costDomainId = eventTable$domain_id,
      costTypeConceptId = 5032, # Administrative cost record
      currencyConceptId = 44818668, # USD
      totalCharge = totalCharge,
      totalCost = totalCost,
      totalPaid = paidByPayer + paidByPatient,
      paidByPayer = paidByPayer,
      paidByPatient = paidByPatient,
      paidPatientCopay = paidPatientCopay,
      paidPatientCoinsurance = paidPatientCoinsurance,
      paidPatientDeductible = paidPatientDeductible,
      paidByPrimary = paidByPayer,
      payerPlanPeriodId = eventTable$payerPlanPeriodId,
      amountAllowed = totalCost,
      revenueCodeConceptId = 0,
      drgConceptId = 0
    )
  })
  return(costRecordsDf)
}

#' Safely Insert Tables
#' @noRd
insertTablesSafely <- function(connection, cdmDatabaseSchema, payerPlansDf, costRecordsDf) {
  tryCatch({
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = cdmDatabaseSchema,
      tableName = "payer_plan_period",
      data = payerPlansDf,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      camelCaseToSnakeCase = TRUE
    )
  }, error = function(e) {
    rlang::abort("Failed to insert payer_plan_period table.", parent = e)
  })
  
  if (nrow(costRecordsDf) > 0) {
    tryCatch({
      DatabaseConnector::insertTable(
        connection = connection,
        databaseSchema = cdmDatabaseSchema,
        tableName = "cost",
        data = costRecordsDf,
        dropTableIfExists = TRUE,
        createTable = TRUE,
        camelCaseToSnakeCase = TRUE
      )
    }, error = function(e) {
      rlang::abort("Failed to insert cost table.", parent = e)
    })
  }
}

#' Backup Cost Table
#' @noRd
backupCostTable <- function(connection, cdmDatabaseSchema, sourceTable, tablesInDb) {
  backupName <- "cost_v5_3_backup"
  if (tolower(backupName) %in% tablesInDb) {
    sql <- "DROP TABLE @cdm_schema.@backup_table;"
    DatabaseConnector::renderTranslateExecuteSql(connection, sql,
                                                 cdm_schema = cdmDatabaseSchema, backup_table = backupName)
  }
  
  sql <- "ALTER TABLE @cdm_schema.@source_table RENAME TO @backup_table;"
  tryCatch({
    DatabaseConnector::renderTranslateExecuteSql(connection, sql,
                                                 cdm_schema = cdmDatabaseSchema, source_table = sourceTable, backup_table = backupName)
  }, error = function(e) {
    rlang::abort(glue::glue("Failed to backup cost table '{sourceTable}'."), parent = e)
  })
}

#' Create CDM v5.4+ Cost Table
#' @noRd
createV5p4CostTable <- function(connection, cdmDatabaseSchema, tableName) {
  sql <- "
    CREATE TABLE @cdm_schema.@table_name (
      cost_id BIGINT NOT NULL,
      person_id BIGINT NOT NULL,
      cost_event_id BIGINT NOT NULL,
      cost_domain_id VARCHAR(20) NOT NULL,
      cost_type_concept_id INT NOT NULL,
      cost_concept_id INT NOT NULL,
      currency_concept_id INT,
      cost FLOAT,
      incurred_date DATE,
      billed_date DATE,
      paid_date DATE,
      revenue_code_concept_id INT,
      drg_concept_id INT,
      cost_source_value VARCHAR(50),
      cost_source_concept_id INT,
      revenue_code_source_value VARCHAR(50),
      drg_source_value VARCHAR(50),
      payer_plan_period_id BIGINT
    );"
  tryCatch({
    DatabaseConnector::renderTranslateExecuteSql(connection, sql,
                                                 cdm_schema = cdmDatabaseSchema, table_name = tableName)
  }, error = function(e) {
    rlang::abort(glue::glue("Failed to create new cost table '{tableName}'."), parent = e)
  })
}

#' Transform and Insert Cost Data
#' @noRd
transformAndInsertCostData <- function(connection, cdmDatabaseSchema, tableName) {
  # This SQL is complex and dialect-specific features might be needed.
  # Using SqlRender is critical.
  sql <- "
    WITH cost_unioned AS (
      SELECT cost_event_id, cost_domain_id, payer_plan_period_id, cost_type_concept_id, currency_concept_id,
             'total_charge' AS cost_source_value, 31973 AS cost_concept_id, total_charge AS cost
      FROM @cdm_schema.cost_v5_3_backup WHERE total_charge IS NOT NULL AND total_charge != 0
      UNION ALL
      SELECT cost_event_id, cost_domain_id, payer_plan_period_id, cost_type_concept_id, currency_concept_id,
             'total_cost' AS cost_source_value, 31985 AS cost_concept_id, total_cost AS cost
      FROM @cdm_schema.cost_v5_3_backup WHERE total_cost IS NOT NULL AND total_cost != 0
      UNION ALL
      SELECT cost_event_id, cost_domain_id, payer_plan_period_id, cost_type_concept_id, currency_concept_id,
             'paid_by_payer' AS cost_source_value, 31980 AS cost_concept_id, paid_by_payer AS cost
      FROM @cdm_schema.cost_v5_3_backup WHERE paid_by_payer IS NOT NULL AND paid_by_payer != 0
      UNION ALL
      SELECT cost_event_id, cost_domain_id, payer_plan_period_id, cost_type_concept_id, currency_concept_id,
             'paid_by_patient' AS cost_source_value, 31981 AS cost_concept_id, paid_by_patient AS cost
      FROM @cdm_schema.cost_v5_3_backup WHERE paid_by_patient IS NOT NULL AND paid_by_patient != 0
      UNION ALL
      SELECT cost_event_id, cost_domain_id, payer_plan_period_id, cost_type_concept_id, currency_concept_id,
             'amount_allowed' AS cost_source_value, 31979 AS cost_concept_id, amount_allowed AS cost
      FROM @cdm_schema.cost_v5_3_backup WHERE amount_allowed IS NOT NULL AND amount_allowed != 0
    )
    INSERT INTO @cdm_schema.@table_name (
      cost_id, person_id, cost_event_id, cost_domain_id, cost_type_concept_id,
      cost_concept_id, currency_concept_id, cost, incurred_date,
      cost_source_value, payer_plan_period_id
    )
    SELECT
      ROW_NUMBER() OVER (ORDER BY ppp.person_id, c.cost_event_id, c.cost_source_value) AS cost_id,
      ppp.person_id,
      c.cost_event_id,
      c.cost_domain_id,
      COALESCE(c.cost_type_concept_id, 0) AS cost_type_concept_id,
      c.cost_concept_id,
      c.currency_concept_id,
      c.cost,
      ppp.payer_plan_period_start_date AS incurred_date, -- Best guess for date
      c.cost_source_value,
      c.payer_plan_period_id
    FROM cost_unioned c
    INNER JOIN @cdm_schema.payer_plan_period ppp
      ON c.payer_plan_period_id = ppp.payer_plan_period_id;
  "
  tryCatch({
    DatabaseConnector::renderTranslateExecuteSql(connection, sql,
                                                 cdm_schema = cdmDatabaseSchema, table_name = tableName)
  }, error = function(e) {
    rlang::abort("Failed to transform and insert cost data.", parent = e)
  })
}

#' Create Indexes on Cost Table
#' @noRd
createCostIndexes <- function(connection, cdmDatabaseSchema, tableName) {
  indexes <- list(
    idx_cost_person_id = "person_id",
    idx_cost_concept_id = "cost_concept_id",
    idx_cost_event_id = "cost_event_id",
    idx_cost_domain_id = "cost_domain_id",
    idx_cost_incurred_date = "incurred_date"
  )
  
  purrr::iwalk(indexes, ~{
    sql <- "CREATE INDEX @index_name ON @cdm_schema.@table_name (@columns);"
    tryCatch({
      DatabaseConnector::renderTranslateExecuteSql(connection, sql,
                                                   index_name = .y, cdm_schema = cdmDatabaseSchema,
                                                   table_name = tableName, columns = .x)
    }, error = function(e) {
      cli::cli_alert_warning("Failed to create index {.y}: {e$message}")
    })
  })
}