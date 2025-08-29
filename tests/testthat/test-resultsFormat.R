testthat::test_that("asFeatureExtractionAggregated works with aggregated Andromeda results", {
  skip_if_not_installed("Andromeda")

  andr <- Andromeda::andromeda()
  on.exit(Andromeda::close(andr), add = TRUE)

  # Aggregated shape per spec: metric_type, metric_name, metric_value
  agg_df <- dplyr::tibble(
    metric_type  = c("visit_level","visit_level","visit_level","visit_level"),
    metric_name  = c("total_cost", "cost_pppm", "n_persons_with_cost", "visits_per_1000_py"),
    metric_value = c(10000, 123.45, 80, 250.0)
  )
  Andromeda::insertTable(andr, "results", agg_df)

  # Diagnostics to provide population size
  diag_df <- dplyr::tibble(
    step_name = c("00_initial_cohort","01_person_subset"),
    n_persons = c(100, 80),
    n_events  = c(NA_real_, NA_real_)
  )
  Andromeda::insertTable(andr, "diagnostics", diag_df)

  settings <- createCostOfCareSettings(costConceptId = 31985L)

  fe <- asFeatureExtractionAggregated(
    results = andr,
    diagnostics = andr,
    settings = settings,
    cohortId = 1,
    analysisId = 501L
  )

  # Structure checks
  expect_type(fe, "list")
  expect_true(all(c("aggregatedCovariates","aggregatedCovariateRef","analysisRef","metaData") %in% names(fe)))
  expect_s3_class(fe$aggregatedCovariates, "tbl_df")
  expect_s3_class(fe$aggregatedCovariateRef, "tbl_df")
  expect_s3_class(fe$analysisRef, "tbl_df")
  expect_true("AggregatedCovariateData" %in% class(fe))

  # Column checks (FeatureExtraction format)
  expect_true(all(c("covariateId","sumValue","averageValue","standardDeviation","minValue","p10Value","medianValue","p90Value","maxValue","countValue") %in% names(fe$aggregatedCovariates)))
  expect_true(all(fe$aggregatedCovariates$countValue == 80))
})


testthat::test_that("asFeatureExtractionAggregated works with person-level Andromeda results", {
  skip_if_not_installed("Andromeda")

  andr <- Andromeda::andromeda()
  on.exit(Andromeda::close(andr), add = TRUE)

  set.seed(42)
  n <- 50
  df_person <- dplyr::tibble(
    person_id = 1:n,
    cost = runif(n, min = 10, max = 1000) |> round(2),
    adjusted_cost = cost * 1.05 |> round(2)
  )
  Andromeda::insertTable(andr, "results", df_person)

  diag_df <- dplyr::tibble(
    step_name = c("01_person_subset"),
    n_persons = c(n),
    n_events  = c(NA_real_)
  )
  Andromeda::insertTable(andr, "diagnostics", diag_df)

  settings <- createCostOfCareSettings(costConceptId = 31980L)

  fe <- asFeatureExtractionAggregated(
    results = andr,
    diagnostics = andr,
    settings = settings,
    cohortId = 2,
    analysisId = 777L
  )

  # Expect two covariates (cost and adjusted_cost)
  expect_equal(nrow(fe$aggregatedCovariateRef), 2)
  expect_equal(unique(fe$aggregatedCovariates$countValue), n)

  # Basic invariants: averageValue equals mean of columns
  means <- c(
    mean(df_person$cost),
    mean(df_person$adjusted_cost)
  )
  # match order by join on covariateId
  ref <- fe$aggregatedCovariateRef |> dplyr::mutate(ord = dplyr::row_number())
  vals <- fe$aggregatedCovariates |> dplyr::inner_join(ref, by = "covariateId") |> dplyr::arrange(ord)
  expect_equal(round(vals$averageValue, 6), round(means, 6))
})
