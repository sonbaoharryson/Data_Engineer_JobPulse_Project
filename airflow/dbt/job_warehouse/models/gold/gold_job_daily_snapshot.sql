{{ config(materialized='table') }}

SELECT
    posted_date,
    platform,
    COUNT(*) AS total_jobs
FROM {{ ref('gold_job_fact') }}
GROUP BY 1, 2
