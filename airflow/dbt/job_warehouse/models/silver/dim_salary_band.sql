{{ config(
    materialized='table',
    tags=['silver_layer', 'dimension']
) }}

SELECT
    TO_HEX(MD5(TO_UTF8(salary_band))) AS salary_band_id,
    salary_band,
    COUNT(*) AS job_count,
    COUNT(DISTINCT company_name) AS num_companies,
    COUNT(DISTINCT job_location) AS num_locations,
    salary_min_million,
    salary_max_million,
    ROUND(AVG(salary_avg_million), 1) AS avg_salary_in_band,
    COUNT(DISTINCT source_platform) AS num_platforms,
    ROUND(CAST(COUNT(*) AS INT) / 
        (SELECT COUNT(*) FROM {{ ref('fact_jobs') }} WHERE salary_band != 'Not Specified') * 100, 2) AS pct_of_salaried_jobs,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dbt_load_timestamp
FROM {{ ref('fact_jobs') }}
WHERE salary_band != 'Not Specified'
    AND job_category IS NOT NULL
    AND job_category != 'Others'
GROUP BY salary_band, salary_min_million, salary_max_million
--ORDER BY salary_min_million DESC NULLS LAST
