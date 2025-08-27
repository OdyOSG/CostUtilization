library(testthat)
library(DBI)
library(duckdb)
library(dplyr)

#===============================================================================
# Test Suite for transformCostToCdmV5dot5()
#===============================================================================

describe("transformCostToCdmV5dot5", {
  
  # --- Test Environment Setup ---
  # A fresh database is created for each test to ensure isolation.
  setup_db <- function() {
    dbFile <- tempfile(fileext = ".duckdb")
    con <- DBI::dbConnect(duckdb::duckdb(dbFile))
    getEunomiaDuckDb(databaseFile = dbFile)
    
    # Return both connection and file path for cleanup
    list(con = con, dbFile = dbFile)
  }
  
  #-----------------------------------------------------------------------------
  # Happy Path Tests
  #-----------------------------------------------------------------------------
  
  it("should correctly transform a wide cost table to the long format", {
    env <- setup_db()
    con <- env$con
    on.exit({
      DBI::dbDisconnect(con, shutdown = TRUE)
      unlink(env$dbFile)
    }, add = TRUE)
    
    # The function under test first injects wide data, then transforms it.
    transformCostToCdmV5dot5(connection = con, cdmDatabaseSchema = "main")
    
    # 1. Verify table structure changes
    all_tables <- tolower(DBI::dbListTables(con))
    expect_true("cost" %in% all_tables)
    expect_true("cost_v5_3_backup" %in% all_tables)
    
    # 2. Verify the new 'cost' table has the correct long-format schema
    new_cost_cols <- tolower(names(DBI::dbGetQuery(con, "SELECT * FROM cost LIMIT 0;")))
    expected_cols <- c("cost_id", "person_id", "cost_concept_id", "cost", "cost_source_value")
    expect_true(all(expected_cols %in% new_cost_cols))
    
    # 3. Verify the data was pivoted
    wide_cost_rows <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM cost_v5_3_backup;")$n
    long_cost_rows <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM cost;")$n
    expect_gt(long_cost_rows, wide_cost_rows)
    
    # 4. Verify multiple cost concepts exist, confirming the pivot logic
    distinct_concepts <- DBI::dbGetQuery(con, "SELECT COUNT(DISTINCT cost_concept_id) AS n FROM cost;")$n
    expect_gt(distinct_concepts, 1)
  })
  
  it("should produce a consistent result when run multiple times", {
    env <- setup_db()
    con <- env$con
    on.exit({
      DBI::dbDisconnect(con, shutdown = TRUE)
      unlink(env$dbFile)
    }, add = TRUE)
    
    # Run transformation the first time
    transformCostToCdmV5dot5(connection = con, cdmDatabaseSchema = "main")
    result1 <- DBI::dbGetQuery(con, "SELECT * FROM cost ORDER BY cost_id;")
    
    # Run transformation the second time
    # The function should handle the existing backup table and re-transform
    transformCostToCdmV5dot5(connection = con, cdmDatabaseSchema = "main")
    result2 <- DBI::dbGetQuery(con, "SELECT * FROM cost ORDER BY cost_id;")
    
    # The final state should be identical
    expect_identical(result1, result2)
  })
  
  #-----------------------------------------------------------------------------
  # Sad Path Tests
  #-----------------------------------------------------------------------------
  
  it("should throw an error if the source cost table does not exist", {
    env <- setup_db()
    con <- env$con
    on.exit({
      DBI::dbDisconnect(con, shutdown = TRUE)
      unlink(env$dbFile)
    }, add = TRUE)
    
    # Manually drop the 'cost' table before running the transform
    # Note: injectCostData is called inside the function, so we can't drop it before.
    # Instead, we test for the error message that should be thrown if injectCostData fails
    # or if the table is missing for any other reason. The current implementation stops
    # if the source table is not found after the initial injection attempt.
    
    # To simulate this, we can rename the table after injection but before the check
    injectCostData(connection = con)
    DBI::dbExecute(con, "ALTER TABLE cost RENAME TO another_name;")
    
    expect_error(
      transformCostToCdmV5dot5(connection = con, cdmDatabaseSchema = "main", sourceCostTable = "cost"),
      regexp = "Source cost table .* not found"
    )
  })
})
