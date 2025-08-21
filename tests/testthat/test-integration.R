# Integration tests for the complete workflow

test_that("complete cost analysis workflow works end-to-end", {
  # Setup connection
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  # Ensure cost data is available (from setup-eunomia.R)

  # Create a comprehensive test cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE integration_cohort AS
    SELECT
      1 AS cohort_definition_id,
      co.person_id AS subject_id,
      MIN(co.condition_start_date) AS cohort_start_date,
      DATE(MIN(co.condition_start_date), '+1 year') AS cohort_end_date
    FROM main.condition_occurrence co
    INNER JOIN main.observation_period op
      ON co.person_id = op.person_id
      AND co.condition_start_date BETWEEN op.observation_period_start_date
                                      AND op.observation_period_end_date
    WHERE co.condition_concept_id IN (201820, 201826)  -- Diabetes
    GROUP BY co.person_id
    HAVING COUNT(DISTINCT co.condition_occurrence_id) >= 2
  ")

  cohortSize <- DatabaseConnector::querySql(
    connection,
    "SELECT COUNT(DISTINCT subject_id) as n FROM integration_cohort"
  )

  skip_if(cohortSize$N == 0, "No suitable cohort members found")

  # Test 1: Basic analysis
  basicSettings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = -30,
    endOffsetDays = 365
  )

  basicResults <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "integration_cohort",
    cohortId = 1,
    costOfCareSettings = basicSettings,
    verbose = FALSE
  )

  expect_type(basicResults, "list")
  expect_true(nrow(basicResults$results) > 0)
  expect_true(nrow(basicResults$diagnostics) > 0)
  expect_true(all(c("totalCost", "costPppm") %in% names(basicResults$results)))

  # Test 2: Analysis with visit restrictions
  visitSettings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 90,
    restrictVisitConceptIds = c(9201, 9203, 9202) # IP, ER, OP
  )

  visitResults <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "integration_cohort",
    cohortId = 1,
    costOfCareSettings = visitSettings,
    verbose = FALSE
  )

  expect_type(visitResults, "list")

  # Test 3: Analysis with event filters
  diabetesFilters <- list(
    list(
      name = "Diabetes Conditions",
      domain = "Condition",
      conceptIds = c(201820, 201826, 443238, 442793)
    ),
    list(
      name = "Diabetes Drugs",
      domain = "Drug",
      conceptIds = c(1503297, 1502826, 1502855, 1529331)
    ),
    list(
      name = "Diabetes Labs",
      domain = "Measurement",
      conceptIds = c(3004501, 3003309, 3005673)
    )
  )

  eventSettings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = -180,
    endOffsetDays = 180,
    eventFilters = diabetesFilters
  )

  eventResults <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "integration_cohort",
    cohortId = 1,
    costOfCareSettings = eventSettings,
    verbose = FALSE
  )

  expect_type(eventResults, "list")

  # Test 4: Different cost concepts
  costTypeSettings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365,
    costConceptId = 31980L, # Total cost instead of charge
    currencyConceptId = 44818668L # USD
  )

  costTypeResults <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "integration_cohort",
    cohortId = 1,
    costOfCareSettings = costTypeSettings,
    verbose = FALSE
  )

  expect_type(costTypeResults, "list")

  # Test 5: Compare time windows
  preSettings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = -365,
    endOffsetDays = -1
  )

  postSettings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 365
  )

  preResults <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "integration_cohort",
    cohortId = 1,
    costOfCareSettings = preSettings,
    verbose = FALSE
  )

  postResults <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "integration_cohort",
    cohortId = 1,
    costOfCareSettings = postSettings,
    verbose = FALSE
  )

  # Both should return results
  expect_type(preResults, "list")
  expect_type(postResults, "list")

  # Clean up
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS integration_cohort")
})

test_that("micro-costing workflow works correctly", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  # Ensure visit_detail exists
  DatabaseConnector::executeSql(connection, "
    CREATE TABLE IF NOT EXISTS main.visit_detail AS
    SELECT
      ROW_NUMBER() OVER () AS visit_detail_id,
      person_id,
      visit_occurrence_id,
      9201 AS visit_detail_concept_id,
      visit_start_date AS visit_detail_start_date,
      visit_end_date AS visit_detail_end_date
    FROM main.visit_occurrence
    WHERE visit_concept_id = 9201
    LIMIT 1000
  ")

  # Create cohort
  DatabaseConnector::executeSql(connection, "
    CREATE TEMPORARY TABLE micro_cohort AS
    SELECT
      1 AS cohort_definition_id,
      person_id AS subject_id,
      observation_period_start_date AS cohort_start_date,
      observation_period_end_date AS cohort_end_date
    FROM main.observation_period
    WHERE person_id IN (
      SELECT DISTINCT person_id
      FROM main.drug_exposure
      WHERE drug_concept_id IN (1503297, 1502826)
    )
    LIMIT 20
  ")

  # Define filters for micro-costing
  microFilters <- list(
    list(
      name = "Target Drugs",
      domain = "Drug",
      conceptIds = c(1503297, 1502826)
    ),
    list(
      name = "Related Procedures",
      domain = "Procedure",
      conceptIds = c(2514435, 2514436)
    )
  )

  microSettings <- createCostOfCareSettings(
    anchorCol = "cohort_start_date",
    startOffsetDays = 0,
    endOffsetDays = 90,
    eventFilters = microFilters,
    microCosting = TRUE,
    primaryEventFilterName = "Target Drugs"
  )

  microResults <- calculateCostOfCare(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "micro_cohort",
    cohortId = 1,
    costOfCareSettings = microSettings,
    verbose = FALSE
  )

  expect_type(microResults, "list")
  expect_true("distinctVisitDetails" %in% names(microResults$results) ||
    "visitDetailsPerThousandPy" %in% names(microResults$results))

  # Clean up
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS main.visit_detail")
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS micro_cohort")
})
