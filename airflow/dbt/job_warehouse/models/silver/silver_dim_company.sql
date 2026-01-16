{{ config(materialized='table') }}

SELECT company_name
FROM {{ ref('silver_jobs_unified') }}
WHERE company_name IS NOT NULL
GROUP BY company_name