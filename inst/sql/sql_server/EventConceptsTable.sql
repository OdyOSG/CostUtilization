CREATE TABLE #event_concepts (
  event_id             INT          NOT NULL,
  event_name           VARCHAR(255) NOT NULL,
  concept_id           BIGINT          NOT NULL,
  domain_id            VARCHAR(50)     NOT NULL
);