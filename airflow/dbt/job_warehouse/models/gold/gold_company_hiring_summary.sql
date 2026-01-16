{{ config(materialized='table') }}

SELECT
    company_name,
    COUNT(*) AS total_jobs,
    COUNT(DISTINCT location_name) AS total_locations
FROM {{ ref('gold_job_fact') }}
GROUP BY 1
