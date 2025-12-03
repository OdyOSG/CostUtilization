-- Calculate person-time for normalization denominators

CREATE TABLE #person_time (
  person_id BIGINT NOT NULL,
  cohort_definition_id BIGINT NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  person_days INT NULL,
  person_years DECIMAL(18, 4) NULL,
  person_quarters DECIMAL(18, 4) NULL,
  person_months DECIMAL(18, 4) NULL
);

INSERT INTO #person_time (
  person_id,
  cohort_definition_id,
  start_date,
  end_date,
  person_days,
  person_years,
  person_quarters,
  person_months
)
SELECT
  person_id,
  cohort_definition_id,
  start_date,
  end_date,
  DATEDIFF(day, start_date, end_date) + 1 AS person_days,
  (DATEDIFF(day, start_date, end_date) + 1) / 365.25 AS person_years,
  (DATEDIFF(day, start_date, end_date) + 1) / 91.3125 AS person_quarters,
  (DATEDIFF(day, start_date, end_date) + 1) / 30.4375 AS person_months
FROM #analysis_window;
