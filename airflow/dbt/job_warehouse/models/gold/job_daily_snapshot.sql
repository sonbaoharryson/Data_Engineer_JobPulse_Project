{{ config(materialized='table') }}

SELECT
    dbt_load_timestamp,
    source_platform,
    COUNT(*) AS total_jobs
FROM {{ ref('job_fact') }}
GROUP BY 1, 2
