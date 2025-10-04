library(testthat)
library(DBI)
library(duckdb)

# ===============================================================================
# Test Suite for getEunomiaDuckDb()
# ===============================================================================

describe("getEunomiaDuckDb", {
  # --- Test Environment Setup ---
  # Create a temporary directory to act as the EUNOMIA_DATA_FOLDER
  temp_data_dir <- tempfile("eunomia_data_")
  dir.create(temp_data_dir)

  # Clean up the directory and any created files when tests are done
  on.exit(unlink(temp_data_dir, recursive = TRUE), add = TRUE)

  #-----------------------------------------------------------------------------
  # Happy Path Tests
  #-----------------------------------------------------------------------------

  it("should download and create a valid Eunomia DuckDB database", {
    # Run the function, which should trigger a download
    db_file <- getEunomiaDuckDb(pathToData = temp_data_dir)

    # 1. Check if the database file was created
    expect_true(file.exists(db_file))

    # 2. Connect to the database and verify its contents
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

    all_tables <- DBI::dbListTables(con)

    # Check for the presence of key OMOP CDM tables
    expected_tables <- c("person", "observation_period", "visit_occurrence", "condition_occurrence", "drug_exposure")
    expect_true(all(expected_tables %in% tolower(all_tables)))

    # Check if a table has data
    person_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM person;")$n
    expect_gt(person_count, 0)
  })

  it("should use an existing archive if it is already downloaded", {
    # The first test should have already downloaded the archive.
    # We'll run it again and check that no "Downloading..." message appears.

    # Capture the output to check for the absence of the download message
    output <- capture.output(
      {
        getEunomiaDuckDb(pathToData = temp_data_dir)
      },
      type = "message"
    )

    # The message "Downloading GiBleed" should NOT be present
    expect_false(any(grepl("Downloading", output, ignore.case = TRUE)))
    # The message "Creating database" SHOULD be present
    expect_true(any(grepl("Creating database", output, ignore.case = TRUE)))
  })

  #-----------------------------------------------------------------------------
  # Sad Path Tests
  #-----------------------------------------------------------------------------

  it("should throw an error if pathToData is not specified", {
    # Temporarily unset the environment variable to ensure the check fails
    withr::with_envvar(c(EUNOMIA_DATA_FOLDER = ""), {
      expect_error(
        getEunomiaDuckDb(pathToData = ""),
        regexp = "The pathToData argument must be specified"
      )
    })
  })
})
