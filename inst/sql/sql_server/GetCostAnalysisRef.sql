-- Generates analysis reference table for cost covariates
{DEFAULT @covariate_settings = '{}'}

-- For now, we define a static set of analysis IDs.
-- A more dynamic approach could parse the settings object if needed.
SELECT
    CAST(analysis_id AS INT) AS analysis_id,
    CAST(analysis_name AS VARCHAR(255)) AS analysis_name,
    CAST(domain_id AS VARCHAR(255)) AS domain_id,
    CAST(start_day AS INT) AS start_day,
    CAST(end_day AS INT) AS end_day,
    CAST(is_binary AS CHAR(1)) as is_binary,
    CAST(missing_means_zero AS CHAR(1)) as missing_means_zero
FROM (
    VALUES
    (101, 'Total Cost',         NULL, -365, 365, 'N', 'Y'),
    (201, 'Cost By Domain',     'Drug;Visit;Procedure;Device;Measurement;Observation;Specimen', -365, 365, 'N', 'Y'),
    (301, 'Cost By Type',       NULL, -365, 365, 'N', 'Y'),
    (401, 'Utilization',        NULL, -365, 365, 'N', 'Y')
) AS t (analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero);