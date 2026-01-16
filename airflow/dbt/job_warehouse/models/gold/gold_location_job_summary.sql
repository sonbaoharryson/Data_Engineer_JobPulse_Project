{{ config(materialized='table') }}

SELECT
    location_name,
    COUNT(*) AS total_jobs
FROM {{ ref('gold_job_fact') }}
GROUP BY 1
