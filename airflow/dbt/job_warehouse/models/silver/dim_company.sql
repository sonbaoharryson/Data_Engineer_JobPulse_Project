{{ config(
    materialized='table',
    tags=['silver_layer', 'dimension'],
    indexes=[
        {'columns': ['company_id'], 'type': 'btree'}
    ]
) }}

SELECT
    TO_HEX(MD5(TO_UTF8(CONCAT(source_platform, '|', company_name)))) AS company_id,
    company_name,
    source_platform,
    COUNT(DISTINCT url) AS total_job_postings,
    COUNT(DISTINCT job_location) AS num_locations,
    COUNT(DISTINCT CASE WHEN posted_to_discord THEN 1 END) AS discord_postings,
    ROUND(CAST(COUNT(DISTINCT CASE WHEN posted_to_discord THEN 1 END) AS INT) / COUNT(DISTINCT url) * 100, 2) AS pct_discord_posted,
    MIN(job_posted_date) AS first_posting_date,
    MAX(job_posted_date) AS last_posting_date,
    ROUND(AVG(salary_avg_million), 1) AS avg_salary_offered,
    ARRAY_JOIN(ARRAY_AGG(DISTINCT work_model_normalized), ', ') AS work_models_offered,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dbt_load_timestamp
FROM {{ ref('fact_jobs') }}
WHERE job_category IS NOT NULL
    AND job_category != 'Others'
GROUP BY source_platform, company_name
