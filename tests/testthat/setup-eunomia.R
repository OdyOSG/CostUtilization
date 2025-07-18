# This setup file is run once before all tests
library(Eunomia)
library(dplyr)
library(DBI)

# --- Connection and Helper Data ---
connectionDetails <- getEunomiaConnectionDetails()
connection <- connect(connectionDetails)

# Get existing persons and events from Eunomia
persons <- dbGetQuery(connection, "SELECT * FROM main.person")
events <- dbGetQuery(connection, "
  SELECT 'Procedure' AS domain_id, procedure_occurrence_id AS event_id, person_id, procedure_date AS event_date FROM main.procedure_occurrence
  UNION ALL
  SELECT 'Drug' AS domain_id, drug_exposure_id AS event_id, person_id, drug_exposure_start_date AS event_date FROM main.drug_exposure
  UNION ALL
  SELECT 'Visit' AS domain_id, visit_occurrence_id AS event_id, person_id, visit_start_date AS event_date FROM main.visit_occurrence
") %>%
  as_tibble() %>%
  mutate(event_date = as.Date(event_date))

# --- Create a Valid PAYER_PLAN_PERIOD Table ---
# Create one or two plan periods for each person
payer_plan_period <- persons %>%
  select(person_id) %>%
  group_by(person_id) %>%
  summarise(
    payer_plan_period_start_date = as.Date(c("2008-01-01", "2012-01-01")),
    payer_plan_period_end_date = as.Date(c("2011-12-31", "2025-12-31"))
  ) %>%
  ungroup() %>%
  mutate(payer_plan_period_id = row_number())

# --- Generate COST Table in "Long" Format for CDM v5.5 ---
set.seed(123)
cost_long <- events %>%
  # Take a sample of events to apply costs to
  sample_n(500) %>%
  # For each event, create multiple cost rows (Charged, Allowed, Paid)
  rowwise() %>%
  mutate(cost_data = list({
    base_cost <- runif(1, 50, 2000)
    charged_amt <- round(base_cost * runif(1, 1.5, 2.5), 2)
    allowed_amt <- round(charged_amt * runif(1, 0.4, 0.7), 2)
    paid_amt <- round(allowed_amt * runif(1, 0.8, 1.0), 2)
    
    tibble::tribble(
      ~cost_concept_id, ~cost_type_concept_id, ~cost,
      # --- Cost Components ---
      31973,              32817,               charged_amt,  # Charged, from a Claim
      31978,              32817,               allowed_amt,  # Allowed, from a Claim
      31980,              32817,               paid_amt,     # Paid, from a Claim
    )
  })) %>%
  ungroup() %>%
  tidyr::unnest(cost_data) %>%
  # Join to get a valid payer_plan_period_id for the person and date
  left_join(payer_plan_period, by = "person_id") %>%
  filter(event_date >= payer_plan_period_start_date & event_date <= payer_plan_period_end_date) %>%
  # Add final v5.5 columns
  mutate(
    cost_event_table = domain_id,
    currency_concept_id = 44818668, # USD
    cost_id = row_number(),
    # Create new date columns based on the event date [cite: 169]
    incurred_date = event_date,
    billed_date = event_date + sample(2:10, 1),
    paid_date = event_date + sample(20:45, 1),
    revenue_code_concept_id = 0,
    drg_concept_id = 0
  ) %>%
  # Select final columns for the COST table
  select(
    cost_id, cost_event_id = event_id, cost_event_table, cost_concept_id, cost_type_concept_id,
    payer_plan_period_id, currency_concept_id, cost, incurred_date, billed_date, paid_date,
    revenue_code_concept_id, drg_concept_id
  )

# --- Create Cohort Table ---
cohort <- dbGetQuery(connection,
                     "SELECT person_id FROM main.condition_occurrence WHERE condition_concept_id = 201826" # T2DM
) %>%
  distinct() %>%
  mutate(
    cohort_definition_id = 1,
    cohort_start_date = as.Date("2010-01-01"),
    cohort_end_date = as.Date("2014-12-31")
  ) %>%
  rename(subject_id = person_id)

# --- Load Tables into Eunomia ---
dbWriteTable(connection, "main.payer_plan_period", payer_plan_period, overwrite = TRUE)
dbWriteTable(connection, "main.cost", cost_long, overwrite = TRUE)
dbWriteTable(connection, "main.cohort", cohort, overwrite = TRUE)