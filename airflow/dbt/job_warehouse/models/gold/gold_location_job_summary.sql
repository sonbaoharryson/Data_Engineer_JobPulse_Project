{{ config(materialized='table') }}

SELECT
    working_location,
    COUNT(*) AS total_jobs
FROM {{ ref('gold_job_fact') }}
GROUP BY 1
