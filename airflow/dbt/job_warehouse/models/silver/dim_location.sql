{{ config(
    materialized='table',
    tags=['silver_layer', 'dimension'],
    indexes=[
        {'columns': ['location_id'], 'type': 'btree'}
    ]
) }}

SELECT
    TO_HEX(MD5(TO_UTF8(job_location))) AS location_id,
    job_location,
    COUNT(DISTINCT url) AS total_jobs,
    COUNT(DISTINCT company_name) AS num_companies,
    COUNT(DISTINCT source_platform) AS num_platforms,
    ROUND(AVG(salary_avg_million), 1) AS avg_salary_by_location,
    MAX(salary_max_million) AS highest_salary,
    MIN(salary_min_million) AS lowest_salary,
    COUNT(DISTINCT CASE WHEN work_model_normalized = 'Remote' THEN url END) AS remote_jobs,
    COUNT(DISTINCT CASE WHEN work_model_normalized = 'Hybrid' THEN url END) AS hybrid_jobs,
    COUNT(DISTINCT CASE WHEN work_model_normalized = 'On-Site' THEN url END) AS onsite_jobs,
    COUNT(DISTINCT CASE WHEN work_model_normalized = 'Full-time' THEN url END) AS fulltime_jobs,
    ARRAY_JOIN(ARRAY_AGG(DISTINCT education_requirement), ', ') AS education_requirements,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dbt_load_timestamp
FROM {{ ref('fact_jobs') }}
GROUP BY job_location, TO_HEX(MD5(TO_UTF8(job_location)))
