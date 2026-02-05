{{ config(
    materialized='table',
    tags=['silver_layer', 'dimension']
) }}

SELECT
    TO_HEX(MD5(TO_UTF8(work_model_normalized))) AS work_model_id,
    work_model_normalized,
    COUNT(DISTINCT url) AS job_count,
    COUNT(DISTINCT company_name) AS num_companies,
    COUNT(DISTINCT job_location) AS num_locations,
    ROUND(AVG(salary_avg_million), 1) AS avg_salary,
    ROUND(COUNT(DISTINCT url) * 100.0 / (SELECT COUNT(DISTINCT url) FROM {{ ref('fact_jobs') }}), 2) AS pct_of_total_jobs,
    COUNT(DISTINCT source_platform) AS num_platforms,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dbt_load_timestamp
FROM {{ ref('fact_jobs') }}
GROUP BY work_model_normalized
ORDER BY job_count DESC
