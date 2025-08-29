library(testthat)
library(DBI)
library(duckdb)
library(dplyr)

#===============================================================================
# Test Suite for transformCostToCdmV5dot5()
#===============================================================================

describe("transformCostToCdmV5dot5", {

  it("should correctly transform a wide cost table to the long format", {
    
    # Create a temporary DuckDB file for the Eunomia dataset
    databaseFile <- getEunomiaDuckDb(pathToData = 'testing_data')
    con <- DBI::dbConnect(duckdb::duckdb(databaseFile))
    
    # Run the transformation
    con <- transformCostToCdmV5dot5(con)
    on.exit(DBI::dbDisconnect(con))
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


})
