library(testthat)
library(DBI)
library(duckdb)
library(dplyr)
library(CostUtilization)

# ===============================================================================
# Test Suite for calculateLos()
# ===============================================================================

describe("calculateLos", {
  db_file <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), db_file)

  # Create a predictable cohort for testing various scenarios
  test_cohort_data <- dplyr::tribble(
    ~cohort_definition_id, ~subject_id, ~cohort_start_date, ~cohort_end_date,
    1L, 2L, "2010-03-01", "2011-01-01",
    1L, 3L, "2010-09-15", "2012-01-01",
    # This person is specifically for testing censoring
    1L, 101L, "2012-01-01", "2012-01-15"
  ) |>
    mutate(across(ends_with("_date"), as.Date))

  DBI::dbWriteTable(con, "test_los_cohort", test_cohort_data, overwrite = TRUE)

  visit_for_censoring_test <- dplyr::tibble(
    visit_occurrence_id = 9999L,
    person_id = 101L,
    visit_concept_id = 9201L, # Inpatient Visit
    visit_start_date = as.Date("2012-01-10"),
    visit_end_date = as.Date("2012-01-20"),
    visit_type_concept_id = 44818517L # Visit derived from EHR record
  )

  # Append this record to the existing visit_occurrence table in the database.
  DBI::dbWriteTable(
    conn = con,
    name = DBI::Id(schema = "main", table = "visit_occurrence"),
    value = visit_for_censoring_test,
    append = TRUE
  )

  # Ensure the connection is closed and the temp file is deleted when tests are done
  on.exit(
    {
      DBI::dbDisconnect(con, shutdown = TRUE)
      unlink(db_file, recursive = T)
    },
    add = TRUE
  )

  # --- Happy Path Tests ---

  it("should run with default settings and return a valid tibble", {
    results <- calculateLos(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "test_los_cohort",
      cohortId = 1,
      verbose = FALSE
    )

    expect_s3_class(results, "tbl_df")
    expect_gt(nrow(results), 0)
  })

  # --- Parameter Tests ---

  it("should correctly apply visitConceptIds restriction", {
    inpatient_visit_id <- 9201L
    results <- calculateLos(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "test_los_cohort",
      cohortId = 1,
      visitConceptIds = inpatient_visit_id,
      verbose = FALSE
    )
    # The only visit concepts should be the one we specified, plus the summary '0'
    expect_true(all(unique(results$visitConceptId) %in% c(0, inpatient_visit_id)))
  })

  it("should correctly use anchorCol = 'cohort_end_date'", {
    # This test just ensures the query runs without error with the alternate anchor
    expect_no_error({
      calculateLos(
        connection = con,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "test_los_cohort",
        cohortId = 1,
        anchorCol = "cohort_end_date",
        startOffsetDays = -365,
        endOffsetDays = 0,
        verbose = FALSE
      )
    })
  })

  it("should correctly censor LOS at cohort_end_date", {
    # Test person 101 has a visit from 2012-01-10 to 2012-01-20 (11 days)
    # Their cohort ends on 2012-01-15.
    # Uncensored LOS should be 11. Censored LOS should be 6 (Jan 10 to Jan 15).

    # Run UNCENSORED
    uncensored_results <- calculateLos(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "test_los_cohort",
      cohortId = 1,
      censorOnCohortEndDate = FALSE,
      verbose = FALSE
    )
    uncensored_los <- uncensored_results |>
      filter(personId == 101, visitConceptId == 9201) |>
      pull(totalLosDays)

    # Run CENSORED
    censored_results <- calculateLos(
      connection = con,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "test_los_cohort",
      cohortId = 1,
      censorOnCohortEndDate = TRUE,
      verbose = FALSE
    )
    censored_los <- censored_results |>
      filter(personId == 101, visitConceptId == 9201) |>
      pull(totalLosDays)

    expect_equal(uncensored_los, 11)
    expect_equal(censored_los, 6)
    expect_lt(censored_los, uncensored_los)
  })

  # --- Sad Path / Error Handling Tests ---

  it("should throw an error for an invalid time window", {
    expect_error(
      calculateLos(
        connection = con,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "test_los_cohort",
        cohortId = 1,
        startOffsetDays = 100,
        endOffsetDays = 50 # Invalid
      )
    )
  })

  it("should throw an error for an invalid anchorCol", {
    expect_error(
      calculateLos(
        connection = con,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "test_los_cohort",
        cohortId = 1,
        anchorCol = "invalid_column_name"
      )
    )
  })

  it("should throw an error for an invalid visitTable", {
    expect_error(
      calculateLos(
        connection = con,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "test_los_cohort",
        cohortId = 1,
        visitTable = "procedure_occurrence" # Not a valid choice
      )
    )
  })
})
