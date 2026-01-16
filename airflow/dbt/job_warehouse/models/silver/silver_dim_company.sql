{{ config(materialized='table') }}

SELECT company
FROM {{ ref('silver_jobs_unified') }}
WHERE company IS NOT NULL
GROUP BY company