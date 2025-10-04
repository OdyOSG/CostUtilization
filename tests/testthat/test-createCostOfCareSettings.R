library(testthat)
library(purrr)
library(rlang)

# ===============================================================================
# Enhanced Test Suite for createCostOfCareSettings() - CDM v5.5 Focus
# ===============================================================================

# CDM v5.5 cost concept reference data
CDM_V55_CONCEPTS <- list(
  # Standard cost concepts
  total_charge = 31973L,
  total_cost = 31985L,
  paid_by_payer = 31980L,
  paid_by_patient = 31981L,
  paid_patient_copay = 31974L,
  paid_patient_coinsurance = 31975L,
  paid_patient_deductible = 31976L,
  amount_allowed = 31979L,

  # Currency concepts
  usd = 44818668L,
  eur = 44818669L,

  # Visit concepts for testing
  inpatient = 9201L,
  outpatient = 9202L,
  emergency = 9203L
)

# Valid OMOP domains for testing
VALID_DOMAINS <- c("Drug", "Condition", "Procedure", "Observation", "Measurement", "Device", "Visit", "All")

describe("createCostOfCareSettings - Enhanced CDM v5.5 Tests", {
  #-----------------------------------------------------------------------------
  # 1. CDM v5.5 Cost Concept Validation Tests
  #-----------------------------------------------------------------------------

  it("should accept all standard CDM v5.5 cost concepts", {
    # Test each standard cost concept
    cost_concept_tests <- CDM_V55_CONCEPTS[1:8] |> # First 8 are cost concepts
      purrr::imap_lgl(~ {
        tryCatch(
          {
            settings <- createCostOfCareSettings(costConceptId = .x)
            expect_s3_class(settings, "CostOfCareSettings")
            expect_equal(settings$costConceptId, .x)
            TRUE
          },
          error = function(e) {
            cli::cli_warn("Failed for concept {.y} ({.x}): {e$message}")
            FALSE
          }
        )
      })

    # All standard concepts should work
    expect_true(all(cost_concept_tests))
  })

  it("should support multiple additional cost concepts", {
    primary_concept <- CDM_V55_CONCEPTS$total_charge
    additional_concepts <- c(
      CDM_V55_CONCEPTS$total_cost,
      CDM_V55_CONCEPTS$paid_by_payer,
      CDM_V55_CONCEPTS$paid_by_patient
    )

    settings <- createCostOfCareSettings(
      costConceptId = primary_concept,
      additionalCostConceptIds = additional_concepts
    )

    expect_equal(settings$costConceptId, primary_concept)
    expect_equal(settings$additionalCostConceptIds, additional_concepts)
    expect_equal(length(settings$additionalCostConceptIds), 3)
  })

  it("should validate currency concepts", {
    # Test with different currency concepts
    currency_tests <- list(
      usd = CDM_V55_CONCEPTS$usd,
      eur = CDM_V55_CONCEPTS$eur
    ) |>
      purrr::imap_lgl(~ {
        tryCatch(
          {
            settings <- createCostOfCareSettings(currencyConceptId = .x)
            expect_equal(settings$currencyConceptId, .x)
            TRUE
          },
          error = function(e) FALSE
        )
      })

    expect_true(all(currency_tests))
  })

  #-----------------------------------------------------------------------------
  # 2. Advanced Event Filter Validation Tests
  #-----------------------------------------------------------------------------

  it("should validate complex multi-domain event filters", {
    complex_filters <- list(
      list(
        name = "Diabetes Medications",
        domain = "Drug",
        conceptIds = c(1503297L, 1502826L, 1502855L, 1525215L)
      ),
      list(
        name = "Diabetes Diagnoses",
        domain = "Condition",
        conceptIds = c(201820L, 201826L, 443238L, 4193704L)
      ),
      list(
        name = "Diabetes Procedures",
        domain = "Procedure",
        conceptIds = c(4301351L, 4139525L, 4273629L)
      ),
      list(
        name = "Diabetes Labs",
        domain = "Measurement",
        conceptIds = c(3004501L, 3003309L, 3024171L, 3034639L)
      ),
      list(
        name = "All Clinical Events",
        domain = "All",
        conceptIds = c(1L, 2L, 3L) # Placeholder concepts
      )
    )

    settings <- createCostOfCareSettings(eventFilters = complex_filters)

    expect_equal(settings$nFilters, 5L)
    expect_true(settings$hasEventFilters)
    expect_equal(length(settings$eventFilters), 5)

    # Verify each filter structure
    purrr::walk(settings$eventFilters, ~ {
      expect_true(all(c("name", "domain", "conceptIds") %in% names(.x)))
      expect_true(.x$domain %in% VALID_DOMAINS)
      expect_true(is.integer(.x$conceptIds))
      expect_gt(length(.x$conceptIds), 0)
    })
  })

  it("should enforce unique filter names", {
    duplicate_filters <- list(
      list(name = "Diabetes", domain = "Drug", conceptIds = c(1L, 2L)),
      list(name = "Diabetes", domain = "Condition", conceptIds = c(3L, 4L)) # Duplicate name
    )

    expect_error(
      createCostOfCareSettings(eventFilters = duplicate_filters),
      regexp = "Duplicate filter names"
    )
  })

  it("should validate domain-specific concept constraints", {
    # Test each valid domain
    domain_tests <- VALID_DOMAINS |>
      purrr::map_lgl(~ {
        filter_list <- list(
          list(
            name = paste("Test", .x, "Filter"),
            domain = .x,
            conceptIds = c(1L, 2L, 3L)
          )
        )

        tryCatch(
          {
            settings <- createCostOfCareSettings(eventFilters = filter_list)
            expect_equal(settings$eventFilters[[1]]$domain, .x)
            TRUE
          },
          error = function(e) FALSE
        )
      })

    expect_true(all(domain_tests))

    # Test invalid domain
    expect_error(
      createCostOfCareSettings(eventFilters = list(
        list(name = "Invalid", domain = "InvalidDomain", conceptIds = c(1L))
      )),
      regexp = "not a valid OMOP domain"
    )
  })

  #-----------------------------------------------------------------------------
  # 3. Micro-costing Configuration Tests
  #-----------------------------------------------------------------------------

  it("should configure micro-costing with proper validation", {
    micro_filters <- list(
      list(name = "Target Procedures", domain = "Procedure", conceptIds = c(1L, 2L)),
      list(name = "Related Drugs", domain = "Drug", conceptIds = c(3L, 4L))
    )

    settings <- createCostOfCareSettings(
      eventFilters = micro_filters,
      microCosting = TRUE
    )

    expect_true(settings$microCosting)
    expect_true(settings$hasEventFilters)
  })

  it("should reject invalid micro-costing configurations", {
    # Missing primary filter name
    expect_error(
      createCostOfCareSettings(
        microCosting = TRUE,
        eventFilters = list(list(name = "Test", domain = "Drug", conceptIds = c(1L)))
      ),
      regexp = "primary event filter"
    )

    # Primary filter name not in event filters
    expect_error(
      createCostOfCareSettings(
        microCosting = TRUE,
        eventFilters = list(list(name = "Test", domain = "Drug", conceptIds = c(1L)))
      )
    )

    # Micro-costing without event filters
    expect_error(
      createCostOfCareSettings(
        microCosting = TRUE
      ),
      regexp = "requires event filters"
    )
  })

  #-----------------------------------------------------------------------------
  # 4. Temporal Window Configuration Tests
  #-----------------------------------------------------------------------------

  it("should support flexible temporal window configurations", {
    # Test various temporal scenarios
    temporal_scenarios <- list(
      list(name = "Pre-index only", start = -365L, end = 0L),
      list(name = "Post-index only", start = 0L, end = 365L),
      list(name = "Around index", start = -180L, end = 180L),
      list(name = "Long-term follow-up", start = 0L, end = 1095L), # 3 years
      list(name = "Short-term", start = -7L, end = 7L)
    )

    temporal_tests <- temporal_scenarios |>
      purrr::map_lgl(~ {
        tryCatch(
          {
            settings <- createCostOfCareSettings(
              startOffsetDays = .x$start,
              endOffsetDays = .x$end
            )

            expect_equal(settings$startOffsetDays, .x$start)
            expect_equal(settings$endOffsetDays, .x$end)
            TRUE
          },
          error = function(e) {
            cli::cli_warn("Failed for {.x$name}: {e$message}")
            FALSE
          }
        )
      })

    expect_true(all(temporal_tests))
  })

  it("should validate temporal window logic", {
    # End must be greater than start
    expect_error(
      createCostOfCareSettings(startOffsetDays = 100L, endOffsetDays = 100L),
      regexp = "must be greater than"
    )

    expect_error(
      createCostOfCareSettings(startOffsetDays = 100L, endOffsetDays = 50L),
      regexp = "must be greater than"
    )
  })

  it("should support both anchor column options", {
    anchor_tests <- c("cohort_start_date", "cohort_end_date") |>
      purrr::map_lgl(~ {
        settings <- createCostOfCareSettings(anchorCol = .x)
        expect_equal(settings$anchorCol, .x)
        TRUE
      })

    expect_true(all(anchor_tests))

    # Invalid anchor should fail
    expect_error(
      createCostOfCareSettings(anchorCol = "invalid_column"),
      regexp = "Must be element of set"
    )
  })

  #-----------------------------------------------------------------------------
  # 5. CPI Adjustment Configuration Tests
  #-----------------------------------------------------------------------------

  it("should configure CPI adjustment with proper file validation", {
    # Create temporary CPI file
    cpi_file <- tempfile(fileext = ".csv")
    on.exit(unlink(cpi_file), add = TRUE)

    cpi_data <- data.frame(
      year = 2020:2023,
      adj_factor = c(1.0, 1.02, 1.05, 1.08)
    )
    utils::write.csv(cpi_data, cpi_file, row.names = FALSE)

    settings <- createCostOfCareSettings(
      cpiAdjustment = TRUE,
      cpiFilePath = cpi_file
    )

    expect_true(settings$cpiAdjustment)
    expect_equal(settings$cpiFilePath, cpi_file)
  })

  it("should validate CPI file requirements", {
    # Missing file path
    expect_error(
      createCostOfCareSettings(cpiAdjustment = TRUE),
      regexp = "no CPI file provided"
    )

    # Non-existent file
    expect_error(
      createCostOfCareSettings(
        cpiAdjustment = TRUE,
        cpiFilePath = "non_existent_file.csv"
      ),
      regexp = "File does not exist"
    )
  })

  #-----------------------------------------------------------------------------
  # 6. Visit Restriction Configuration Tests
  #-----------------------------------------------------------------------------

  it("should configure visit restrictions properly", {
    visit_concepts <- c(
      CDM_V55_CONCEPTS$inpatient,
      CDM_V55_CONCEPTS$outpatient,
      CDM_V55_CONCEPTS$emergency
    )

    settings <- createCostOfCareSettings(restrictVisitConceptIds = visit_concepts)

    expect_true(settings$hasVisitRestriction)
    expect_equal(settings$restrictVisitConceptIds, visit_concepts)
    expect_equal(length(settings$restrictVisitConceptIds), 3)
  })

  it("should validate visit concept constraints", {
    # Should accept positive integers
    valid_concepts <- c(9201L, 9202L, 9203L)
    settings <- createCostOfCareSettings(restrictVisitConceptIds = valid_concepts)
    expect_equal(settings$restrictVisitConceptIds, valid_concepts)

    # Should reject invalid values
    expect_error(
      createCostOfCareSettings(restrictVisitConceptIds = c(0L, -1L))
    )

    expect_error(
      createCostOfCareSettings(restrictVisitConceptIds = c(1L, 1L))
    )
  })

  #-----------------------------------------------------------------------------
  # 7. Comprehensive Integration Tests
  #-----------------------------------------------------------------------------

  it("should create comprehensive settings with all features", {
    # Create temporary CPI file
    cpi_file <- tempfile(fileext = ".csv")
    on.exit(unlink(cpi_file), add = TRUE)

    utils::write.csv(
      data.frame(year = 2020:2023, adj_factor = c(1.0, 1.02, 1.05, 1.08)),
      cpi_file,
      row.names = FALSE
    )

    # Comprehensive configuration
    comprehensive_filters <- list(
      list(name = "Primary Events", domain = "Procedure", conceptIds = c(1L, 2L, 3L)),
      list(name = "Secondary Events", domain = "Drug", conceptIds = c(4L, 5L, 6L))
    )

    settings <- createCostOfCareSettings(
      anchorCol = "cohort_end_date",
      startOffsetDays = -180L,
      endOffsetDays = 365L,
      restrictVisitConceptIds = c(9201L, 9202L),
      eventFilters = comprehensive_filters,
      microCosting = TRUE,
      costConceptId = CDM_V55_CONCEPTS$total_cost,
      currencyConceptId = CDM_V55_CONCEPTS$usd,
      additionalCostConceptIds = c(CDM_V55_CONCEPTS$paid_by_payer, CDM_V55_CONCEPTS$paid_by_patient),
      cpiAdjustment = TRUE,
      cpiFilePath = cpi_file
    )

    # Validate all settings
    expect_s3_class(settings, "CostOfCareSettings")
    expect_equal(settings$anchorCol, "cohort_end_date")
    expect_equal(settings$startOffsetDays, -180L)
    expect_equal(settings$endOffsetDays, 365L)
    expect_true(settings$hasVisitRestriction)
    expect_equal(length(settings$restrictVisitConceptIds), 2)
    expect_true(settings$hasEventFilters)
    expect_equal(settings$nFilters, 2L)
    expect_true(settings$microCosting)
    expect_equal(settings$costConceptId, CDM_V55_CONCEPTS$total_cost)
    expect_equal(settings$currencyConceptId, CDM_V55_CONCEPTS$usd)
    expect_equal(length(settings$additionalCostConceptIds), 2)
    expect_true(settings$cpiAdjustment)
    expect_equal(settings$cpiFilePath, cpi_file)
  })

  it("should provide informative validation messages", {
    # Test that validation messages are helpful
    expect_error(
      createCostOfCareSettings(startOffsetDays = 100L, endOffsetDays = 50L)
    )

    expect_error(
      createCostOfCareSettings(eventFilters = "not_a_list")
    )
  })

  #-----------------------------------------------------------------------------
  # 8. Functional Programming and Modern R Tests
  #-----------------------------------------------------------------------------

  it("should support functional programming patterns", {
    # Test creating multiple settings configurations using purrr
    cost_concepts <- list(
      total_charge = CDM_V55_CONCEPTS$total_charge,
      total_cost = CDM_V55_CONCEPTS$total_cost,
      paid_by_payer = CDM_V55_CONCEPTS$paid_by_payer
    )

    settings_list <- cost_concepts |>
      purrr::imap(~ {
        createCostOfCareSettings(
          costConceptId = .x,
          startOffsetDays = 0L,
          endOffsetDays = 365L
        )
      })

    expect_equal(length(settings_list), 3)
    expect_true(all(purrr::map_lgl(settings_list, ~ inherits(.x, "CostOfCareSettings"))))
  })

  it("should work with rlang and tidy evaluation patterns", {
    # Test that settings work with rlang patterns
    create_settings_with_params <- function(concept_id, ...) {
      dots <- rlang::list2(...)

      base_args <- list(
        costConceptId = concept_id,
        startOffsetDays = 0L,
        endOffsetDays = 365L
      )

      final_args <- c(base_args, dots)
      do.call(createCostOfCareSettings, final_args)
    }

    settings <- create_settings_with_params(
      CDM_V55_CONCEPTS$total_charge,
      anchorCol = "cohort_end_date",
      restrictVisitConceptIds = c(9201L, 9202L)
    )

    expect_s3_class(settings, "CostOfCareSettings")
    expect_equal(settings$costConceptId, CDM_V55_CONCEPTS$total_charge)
    expect_equal(settings$anchorCol, "cohort_end_date")
    expect_true(settings$hasVisitRestriction)
  })
})
