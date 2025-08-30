library(testthat)
library(DBI)
library(duckdb)
library(dplyr)
library(purrr)
library(rlang)

#===============================================================================
# Enhanced Test Suite for Helper Functions - CDM v5.5 Support
#===============================================================================

describe("Helper Functions - Enhanced Tests", {
  
  # Setup test database for helper function testing
  setup_helper_test_db <- function() {
    db_file <- tempfile(fileext = ".duckdb")
    con <- DBI::dbConnect(duckdb::duckdb(db_file))
    
    # Create some test tables
    DBI::dbExecute(con, "CREATE TABLE test_table1 (id INTEGER, name VARCHAR);")
    DBI::dbExecute(con, "CREATE TABLE test_table2 (id INTEGER, value DOUBLE);")
    DBI::dbExecute(con, "INSERT INTO test_table1 VALUES (1, 'test'), (2, 'data');")
    DBI::dbExecute(con, "INSERT INTO test_table2 VALUES (1, 100.5), (2, 200.7);")
    
    return(list(connection = con, db_file = db_file))
  }
  
  cleanup_helper_test_db <- function(test_env) {
    if (!is.null(test_env$connection) && DBI::dbIsValid(test_env$connection)) {
      DBI::dbDisconnect(test_env$connection, shutdown = TRUE)
    }
    if (!is.null(test_env$db_file) && file.exists(test_env$db_file)) {
      unlink(test_env$db_file)
    }
  }
  
  #-----------------------------------------------------------------------------
  # 1. cleanupTempTables Function Tests
  #-----------------------------------------------------------------------------
  
  it("should clean up temporary tables correctly", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # Create temporary tables
    DBI::dbExecute(con, "CREATE TEMPORARY TABLE temp_table1 (id INTEGER);")
    DBI::dbExecute(con, "CREATE TEMPORARY TABLE temp_table2 (id INTEGER);")
    
    # Verify tables exist
    initial_tables <- DBI::dbListTables(con)
    expect_true("temp_table1" %in% initial_tables)
    expect_true("temp_table2" %in% initial_tables)
    
    # Clean up specific tables
    cleanupTempTables(con, "temp_table1", "temp_table2")
    
    # Verify tables are removed
    final_tables <- DBI::dbListTables(con)
    expect_false("temp_table1" %in% final_tables)
    expect_false("temp_table2" %in% final_tables)
  })
  
  it("should handle non-existent tables gracefully", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # Should not error when trying to clean up non-existent tables
    expect_no_error({
      cleanupTempTables(con, "non_existent_table1", "non_existent_table2")
    })
  })
  
  it("should work with schema-qualified table names", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # Create table in main schema
    DBI::dbExecute(con, "CREATE TABLE main.schema_test_table (id INTEGER);")
    
    # Verify table exists
    expect_true("schema_test_table" %in% DBI::dbListTables(con))
    
    # Clean up with schema specification
    expect_no_error({
      cleanupTempTables(con, schema = "main", "schema_test_table")
    })
  })
  
  #-----------------------------------------------------------------------------
  # 2. logMessage Function Tests
  #-----------------------------------------------------------------------------
  
  it("should handle different log levels correctly", {
    # Test different log levels
    log_levels <- c("INFO", "WARNING", "ERROR", "DEBUG", "SUCCESS")
    
    log_tests <- log_levels |>
      purrr::map_lgl(~ {
        # Capture output to verify logging works
        output <- capture.output({
          logMessage("Test message", verbose = TRUE, level = .x)
        }, type = "message")
        
        # Should produce some output for each level
        length(output) > 0
      })
    
    expect_true(all(log_tests))
  })
  
  it("should respect verbose flag", {
    # With verbose = FALSE, should produce no output
    output_false <- capture.output({
      logMessage("Test message", verbose = FALSE, level = "INFO")
    }, type = "message")
    
    expect_equal(length(output_false), 0)
    
    # With verbose = TRUE, should produce output
    output_true <- capture.output({
      logMessage("Test message", verbose = TRUE, level = "INFO")
    }, type = "message")
    
    expect_gt(length(output_true), 0)
  })
  
  #-----------------------------------------------------------------------------
  # 3. executeSqlStatements Function Tests
  #-----------------------------------------------------------------------------
  
  it("should execute multiple SQL statements correctly", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    sql_statements <- c(
      "CREATE TEMPORARY TABLE exec_test1 (id INTEGER);",
      "INSERT INTO exec_test1 VALUES (1), (2), (3);",
      "CREATE TEMPORARY TABLE exec_test2 AS SELECT * FROM exec_test1;"
    )
    
    # Execute statements
    expect_no_error({
      executeSqlStatements(con, sql_statements, verbose = FALSE)
    })
    
    # Verify results
    result <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM exec_test2;")
    expect_equal(result$n, 3)
  })
  
  it("should handle empty and NULL statements", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # Should handle empty vector
    expect_no_error({
      executeSqlStatements(con, character(0), verbose = FALSE)
    })
    
    # Should handle NULL and empty strings
    mixed_statements <- c(
      "CREATE TEMPORARY TABLE null_test (id INTEGER);",
      NULL,
      "",
      "   ",  # Whitespace only
      "INSERT INTO null_test VALUES (1);"
    )
    
    expect_no_error({
      executeSqlStatements(con, mixed_statements, verbose = FALSE)
    })
    
    # Verify valid statements were executed
    result <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM null_test;")
    expect_equal(result$n, 1)
  })
  
  it("should provide informative error messages for SQL failures", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # Invalid SQL should produce informative error
    invalid_statements <- c(
      "CREATE TEMPORARY TABLE error_test (id INTEGER);",
      "INVALID SQL STATEMENT HERE;",  # This will fail
      "INSERT INTO error_test VALUES (1);"
    )
    
    expect_error({
      executeSqlStatements(con, invalid_statements, verbose = FALSE)
    }, regexp = "Error executing SQL statement")
  })
  
  #-----------------------------------------------------------------------------
  # 4. formatDuration Function Tests
  #-----------------------------------------------------------------------------
  
  it("should format durations correctly", {
    # Test different time scales
    duration_tests <- list(
      list(seconds = 30, expected_pattern = "seconds"),
      list(seconds = 90, expected_pattern = "minutes"),
      list(seconds = 3700, expected_pattern = "hours")
    )
    
    duration_results <- duration_tests |>
      purrr::map_lgl(~ {
        formatted <- formatDuration(.x$seconds)
        grepl(.x$expected_pattern, formatted)
      })
    
    expect_true(all(duration_results))
  })
  
  #-----------------------------------------------------------------------------
  # 5. insertTableDBI Function Tests
  #-----------------------------------------------------------------------------
  
  it("should insert data correctly with various options", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # Test data
    test_data <- tibble::tibble(
      testId = 1:3,
      testName = c("A", "B", "C"),
      testValue = c(10.1, 20.2, 30.3)
    )
    
    # Insert with camelCase conversion
    expect_no_error({
      insertTableDBI(
        connection = con,
        tableName = "insert_test",
        data = test_data,
        camelCaseToSnakeCase = TRUE
      )
    })
    
    # Verify data was inserted with snake_case columns
    result <- DBI::dbGetQuery(con, "SELECT * FROM insert_test ORDER BY test_id;")
    expect_equal(nrow(result), 3)
    expect_true("test_id" %in% names(result))
    expect_true("test_name" %in% names(result))
    expect_true("test_value" %in% names(result))
  })
  
  it("should handle temporary table creation", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    test_data <- tibble::tibble(id = 1:2, value = c("X", "Y"))
    
    # Create temporary table
    expect_no_error({
      insertTableDBI(
        connection = con,
        tableName = "temp_insert_test",
        data = test_data,
        tempTable = TRUE
      )
    })
    
    # Verify temporary table exists and has data
    result <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM temp_insert_test;")
    expect_equal(result$n, 2)
  })
  
  #-----------------------------------------------------------------------------
  # 6. Utility Function Tests (.int_flag, .qualify, executeOne)
  #-----------------------------------------------------------------------------
  
  it("should convert logical values to integer flags correctly", {
    expect_equal(.int_flag(TRUE), 1L)
    expect_equal(.int_flag(FALSE), 0L)
    expect_equal(.int_flag(NULL), 0L)
    expect_equal(.int_flag(NA), 0L)
    expect_equal(.int_flag("true"), 0L)  # Only TRUE should be 1
  })
  
  it("should qualify table names correctly", {
    # With schema
    expect_equal(.qualify("table_name", "schema_name"), "schema_name.table_name")
    
    # Without schema (NULL)
    expect_equal(.qualify("table_name", NULL), "table_name")
    
    # Without schema (empty string)
    expect_equal(.qualify("table_name", ""), "table_name")
    
    # Edge cases
    expect_equal(.qualify(NULL, "schema"), NULL)
    expect_equal(.qualify("", "schema"), "")
  })
  
  it("should execute single SQL statements with executeOne", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # executeOne should return a list with result, messages, warnings, error
    result <- executeOne(con, "CREATE TEMPORARY TABLE execute_one_test (id INTEGER);")
    
    expect_type(result, "list")
    expect_true("result" %in% names(result))
    expect_null(result$error)
    
    # Verify table was created
    tables <- DBI::dbListTables(con)
    expect_true("execute_one_test" %in% tables)
  })
  
  #-----------------------------------------------------------------------------
  # 7. Integration Tests with CDM v5.5 Patterns
  #-----------------------------------------------------------------------------
  
  it("should support CDM v5.5 cost table operations", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # Create CDM v5.5 style cost table
    cost_table_sql <- "
      CREATE TABLE cost_v55_test (
        cost_id BIGINT,
        person_id BIGINT,
        cost_concept_id INTEGER,
        cost_source_value VARCHAR(50),
        cost NUMERIC(19,4),
        effective_date DATE
      );"
    
    executeOne(con, cost_table_sql)
    
    # Insert test data using helper
    cost_data <- tibble::tibble(
      costId = 1:3,
      personId = c(101L, 102L, 103L),
      costConceptId = c(31973L, 31985L, 31980L),
      costSourceValue = c("total_charge", "total_cost", "paid_by_payer"),
      cost = c(1000.50, 800.25, 600.75),
      effectiveDate = as.Date(c("2023-01-15", "2023-01-16", "2023-01-17"))
    )
    
    insertTableDBI(
      connection = con,
      tableName = "cost_v55_test",
      data = cost_data,
      camelCaseToSnakeCase = TRUE
    )
    
    # Verify CDM v5.5 cost concepts are properly stored
    concept_check <- DBI::dbGetQuery(con, "
      SELECT cost_concept_id, cost_source_value, COUNT(*) as n
      FROM cost_v55_test 
      GROUP BY cost_concept_id, cost_source_value
      ORDER BY cost_concept_id;")
    
    expect_equal(nrow(concept_check), 3)
    expect_true(31973L %in% concept_check$cost_concept_id)
    expect_true("total_charge" %in% concept_check$cost_source_value)
  })
  
  it("should handle complex cleanup scenarios", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # Create multiple temporary tables with CDM-style names
    temp_tables <- c(
      "tmp_cohort_costs",
      "tmp_visit_restrictions", 
      "tmp_event_filters",
      "tmp_cpi_adjustments"
    )
    
    # Create all tables
    purrr::walk(temp_tables, ~ {
      sql <- glue::glue("CREATE TEMPORARY TABLE {.x} (id INTEGER);")
      executeOne(con, sql)
    })
    
    # Verify all exist
    existing_tables <- DBI::dbListTables(con)
    expect_true(all(temp_tables %in% existing_tables))
    
    # Clean up using functional approach
    temp_tables |>
      purrr::walk(~ cleanupTempTables(con, .x))
    
    # Verify all are cleaned up
    final_tables <- DBI::dbListTables(con)
    expect_false(any(temp_tables %in% final_tables))
  })
  
  #-----------------------------------------------------------------------------
  # 8. Performance and Error Handling Tests
  #-----------------------------------------------------------------------------
  
  it("should handle large SQL statement batches efficiently", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # Create a large batch of SQL statements
    large_batch <- 1:50 |>
      purrr::map_chr(~ {
        glue::glue("INSERT INTO test_table1 VALUES ({.x + 100}, 'batch_{.x}');")
      })
    
    # Should complete within reasonable time
    start_time <- Sys.time()
    
    expect_no_error({
      executeSqlStatements(con, large_batch, verbose = FALSE)
    })
    
    end_time <- Sys.time()
    execution_time <- as.numeric(difftime(end_time, start_time, units = "secs"))
    
    # Should complete quickly for 50 simple statements
    expect_lt(execution_time, 10)
    
    # Verify all statements executed
    result <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM test_table1 WHERE id > 100;")
    expect_equal(result$n, 50)
  })
  
  it("should provide robust error handling", {
    test_env <- setup_helper_test_db()
    on.exit(cleanup_helper_test_db(test_env))
    
    con <- test_env$connection
    
    # Test error handling with invalid connection
    invalid_con <- structure(list(), class = "DBIConnection")
    
    expect_error({
      cleanupTempTables(invalid_con, "test_table")
    })
    
    # Test with NULL inputs
    expect_no_error({
      cleanupTempTables(con, NULL, "", "   ")
    })
    
    # Test logMessage with invalid level
    expect_no_error({
      logMessage("Test", verbose = TRUE, level = "INVALID_LEVEL")
    })
  })
})