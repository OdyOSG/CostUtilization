# The `setup-eunomia.R` file runs first, creating a connection and populating
# the database with the new v5.5 COST and PAYER_PLAN_PERIOD tables.

test_that("Compute function calculates total 'Allowed' cost correctly", {
  settings <- createCostCovariateSettings(
    covariateId = 2001,
    costConceptId = c(31978),      # Target "Allowed" amount
    costTypeConceptId = c(32817)  # From a "Claim"
  )
  
  result <- getCostCovariateData(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    costCovariateSettings = settings
  )
  
  # Calculate the expected value directly from the generated data
  cost_table <- dbGetQuery(connection, "SELECT * FROM main.cost")
  cohort_table <- dbGetQuery(connection, "SELECT * FROM main.cohort")
  events_table <- dbGetQuery(connection, "SELECT 'Procedure' AS cost_event_table, procedure_occurrence_id AS cost_event_id, person_id FROM main.procedure_occurrence UNION ALL SELECT 'Drug' AS cost_event_table, drug_exposure_id AS cost_event_id, person_id FROM main.drug_exposure UNION ALL SELECT 'Visit' AS cost_event_table, visit_occurrence_id AS cost_event_id, person_id FROM main.visit_occurrence")
  
  expected_costs <- cost_table %>%
    filter(cost_concept_id == 31978, cost_type_concept_id == 32817) %>%
    inner_join(events_table, by = c("cost_event_id", "cost_event_table")) %>%
    inner_join(cohort_table, by = c("person_id" = "subject_id")) %>%
    group_by(person_id) %>%
    summarise(total_cost = sum(cost, na.rm = TRUE))
  
  # Merge expected results with the full cohort to include zeros
  full_expected <- cohort_table %>%
    left_join(expected_costs, by = c("subject_id" = "person_id")) %>%
    mutate(total_cost = ifelse(is.na(total_cost), 0, total_cost))
  
  # Compare the results
  comparison_df <- result %>%
    arrange(subjectId) %>%
    rename(result_cost = covariateValue) %>%
    left_join(
      full_expected %>%
        arrange(subject_id) %>%
        select(subject_id, expected_cost = total_cost),
      by = c("subjectId" = "subject_id")
    )
  
  expect_equal(comparison_df$result_cost, comparison_df$expected_cost)
  expect_equal(nrow(result), nrow(cohort_table))
})

test_that("Patients with no costs return a covariate value of 0", {
  # Use a concept ID that we know doesn't exist in our generated data
  settings <- createCostCovariateSettings(
    covariateId = 2002,
    costConceptId = c(999999), # Fake concept ID
    costTypeConceptId = c(32817)
  )
  
  result <- getCostCovariateData(
    connection = connection,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortId = 1,
    costCovariateSettings = settings
  )
  
  # Everyone's cost should be 0
  expect_true(all(result$covariateValue == 0))
  expect_equal(nrow(result), nrow(dbGetQuery(connection, "SELECT * FROM main.cohort")))
})

# Disconnect from the database after tests are complete
withr::defer({
  DatabaseConnector::disconnect(connection)
}, testthat::teardown_env())