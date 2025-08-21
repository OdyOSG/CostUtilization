# Test utility functions

test_that("logMessage outputs correct message types", {
  # Test different log levels
  expect_silent(logMessage("Test", verbose = FALSE))

  # Capture output for different levels
  expect_message(logMessage("Info test", verbose = TRUE, level = "INFO"), "Info test")
  expect_message(logMessage("Warning test", verbose = TRUE, level = "WARNING"), "Warning test")
  expect_message(logMessage("Error test", verbose = TRUE, level = "ERROR"), "Error test")
  expect_message(logMessage("Success test", verbose = TRUE, level = "SUCCESS"), "Success test")
  expect_message(logMessage("Debug test", verbose = TRUE, level = "DEBUG"), "Debug test")
  expect_message(logMessage("Other test", verbose = TRUE, level = "OTHER"), "Other test")
})

test_that("cleanupTempTables removes tables correctly", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  # Create test tables
  DatabaseConnector::executeSql(connection, "CREATE TEMPORARY TABLE temp_test1 (id INT)")
  DatabaseConnector::executeSql(connection, "CREATE TEMPORARY TABLE temp_test2 (id INT)")

  # Clean up tables
  cleanupTempTables(connection, NULL, "temp_test1", "temp_test2")

  # Verify tables are dropped
  tables <- DatabaseConnector::getTableNames(connection)
  expect_false("temp_test1" %in% tolower(tables))
  expect_false("temp_test2" %in% tolower(tables))

  # Test with schema
  DatabaseConnector::executeSql(connection, "CREATE TEMPORARY TABLE temp_test3 (id INT)")
  cleanupTempTables(connection, "main", "temp_test3")

  # Test with NULL tables (should not error)
  expect_silent(cleanupTempTables(connection, NULL, NULL, "non_existent", NULL))
})

test_that("executeSqlStatements handles multiple statements correctly", {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  # Test with valid statements
  statements <- c(
    "CREATE TEMPORARY TABLE exec_test1 (id INT, value TEXT)",
    "INSERT INTO exec_test1 VALUES (1, 'test1')",
    "INSERT INTO exec_test1 VALUES (2, 'test2')",
    "" # Empty statement should be skipped
  )

  expect_silent(executeSqlStatements(connection, statements, verbose = FALSE))

  # Verify data was inserted
  result <- DatabaseConnector::querySql(connection, "SELECT COUNT(*) as n FROM exec_test1")
  expect_equal(result$N, 2)

  # Test with error handling
  badStatements <- c(
    "CREATE TEMPORARY TABLE exec_test2 (id INT)",
    "INVALID SQL STATEMENT" # This should cause an error
  )

  expect_error(
    executeSqlStatements(connection, badStatements, verbose = FALSE),
    "syntax error"
  )

  # Clean up
  DatabaseConnector::executeSql(connection, "DROP TABLE IF EXISTS exec_test1")
})

test_that("validateEventFilters validates structure correctly", {
  # Valid filters
  validFilters <- list(
    list(
      name = "Diabetes",
      domain = "Condition",
      conceptIds = c(201820, 201826)
    ),
    list(
      name = "Metformin",
      domain = "Drug",
      conceptIds = c(1503297)
    )
  )

  expect_silent(validateEventFilters(validFilters))

  # Not a list
  expect_error(
    validateEventFilters("not a list"),
    "eventFilters.*must be a list"
  )

  # Filter not a list
  invalidFilters1 <- list("not a list")
  expect_error(
    validateEventFilters(invalidFilters1),
    "Event filter 1 must be a list"
  )

  # Missing name
  invalidFilters2 <- list(
    list(
      domain = "Condition",
      conceptIds = c(201820)
    )
  )
  expect_error(
    validateEventFilters(invalidFilters2),
    "Event filter 1 must have a 'name' field"
  )

  # Invalid name type
  invalidFilters3 <- list(
    list(
      name = 123, # Should be character
      domain = "Condition",
      conceptIds = c(201820)
    )
  )
  expect_error(
    validateEventFilters(invalidFilters3),
    "Event filter 1 must have a 'name' field.*character"
  )

  # Missing domain
  invalidFilters4 <- list(
    list(
      name = "Test",
      conceptIds = c(201820)
    )
  )
  expect_error(
    validateEventFilters(invalidFilters4),
    "Event filter 1 must have a 'domain' field"
  )

  # Invalid domain
  invalidFilters5 <- list(
    list(
      name = "Test",
      domain = "InvalidDomain",
      conceptIds = c(201820)
    )
  )
  expect_error(
    validateEventFilters(invalidFilters5),
    "Event filter 1 has invalid domain: InvalidDomain"
  )

  # Missing conceptIds
  invalidFilters6 <- list(
    list(
      name = "Test",
      domain = "Condition"
    )
  )
  expect_error(
    validateEventFilters(invalidFilters6),
    "Event filter 1 must have a 'conceptIds' field"
  )

  # Empty conceptIds
  invalidFilters7 <- list(
    list(
      name = "Test",
      domain = "Condition",
      conceptIds = numeric(0)
    )
  )
  expect_error(
    validateEventFilters(invalidFilters7),
    "Event filter 1 must have a 'conceptIds' field.*at least one value"
  )

  # Non-numeric conceptIds
  invalidFilters8 <- list(
    list(
      name = "Test",
      domain = "Condition",
      conceptIds = c("not", "numeric")
    )
  )
  expect_error(
    validateEventFilters(invalidFilters8),
    "Event filter 1 must have a 'conceptIds' field.*numeric"
  )

  # Duplicate names
  invalidFilters9 <- list(
    list(
      name = "Duplicate",
      domain = "Condition",
      conceptIds = c(201820)
    ),
    list(
      name = "Duplicate",
      domain = "Drug",
      conceptIds = c(1503297)
    )
  )
  expect_error(
    validateEventFilters(invalidFilters9),
    "Event filter names must be unique"
  )

  # Test all valid domains
  validDomains <- c(
    "Drug", "Condition", "Procedure", "Measurement",
    "Observation", "Device", "Visit", "All"
  )

  for (domain in validDomains) {
    filter <- list(
      list(
        name = paste("Test", domain),
        domain = domain,
        conceptIds = c(1)
      )
    )
    expect_silent(validateEventFilters(filter))
  }
})

test_that("qualifyTableName handles schema correctly", {
  # Both provided
  expect_equal(qualifyTableName("table1", "schema1"), "schema1.table1")

  # No schema
  expect_equal(qualifyTableName("table1", NULL), "table1")

  # No table
  expect_null(qualifyTableName(NULL, "schema1"))

  # Neither provided
  expect_null(qualifyTableName(NULL, NULL))

  # Empty strings
  expect_equal(qualifyTableName("table1", ""), "table1")
  expect_equal(qualifyTableName("", "schema1"), "schema1.")
})
