{{ config(materialized='table') }}

SELECT
    least_level_of_education,
    COUNT(*) AS total_jobs
FROM {{ ref('gold_job_fact') }}
GROUP BY 1
