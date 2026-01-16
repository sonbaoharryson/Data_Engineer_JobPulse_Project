{{ config(materialized='table') }}

SELECT DISTINCT
    working_location
FROM {{ ref('silver_jobs_unified') }}
WHERE working_location IS NOT NULL