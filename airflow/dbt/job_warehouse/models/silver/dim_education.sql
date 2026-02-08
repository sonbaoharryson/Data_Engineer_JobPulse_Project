{{ config(
    materialized='table',
    tags=['silver_layer', 'dimension']
) }}

SELECT
    TO_HEX(MD5(TO_UTF8(education_requirement))) AS education_id,
    education_requirement,
    COUNT(*) AS job_count,
    COUNT(DISTINCT company_name) AS num_companies,
    COUNT(DISTINCT job_location) AS num_locations,
    ROUND(AVG(salary_avg_million), 1) AS avg_salary,
    ROUND(CAST(COUNT(*) AS INT) / 
        (SELECT COUNT(*) FROM {{ ref('fact_jobs') }}) * 100, 2) AS pct_of_total_jobs,
    ARRAY_JOIN(ARRAY_AGG(DISTINCT source_platform), ', ') AS source_platforms,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dbt_load_timestamp
FROM {{ ref('fact_jobs') }}
WHERE education_requirement != 'Not Specified'
    AND job_category IS NOT NULL
    AND job_category != 'Others'
GROUP BY education_requirement
