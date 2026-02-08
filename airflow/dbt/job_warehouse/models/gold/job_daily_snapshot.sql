{{ config(materialized='table') }}

SELECT
    job_posted_date,
    source_platform,
    COUNT(*) AS total_jobs
FROM {{ ref('job_fact') }}
GROUP BY 1, 2
