{{ config(
    materialized='table',
    tags=['gold_layer', 'mart']
) }}

SELECT
    source_platform,
    COUNT(*) AS total_job_postings,
    COUNT(DISTINCT company_name) AS unique_companies,
    COUNT(DISTINCT job_location) AS unique_locations,
    COUNT(DISTINCT job_title) AS unique_job_titles,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN posted_to_discord THEN 1 END) / COUNT(*), 2) AS pct_discord_posted,
    COUNT(DISTINCT CASE WHEN work_model_normalized = 'Remote' THEN url END) AS remote_jobs_count,
    COUNT(DISTINCT CASE WHEN work_model_normalized = 'Hybrid' THEN url END) AS hybrid_jobs_count,
    COUNT(DISTINCT CASE WHEN work_model_normalized = 'Full-time' THEN url END) AS fulltime_jobs_count,
    COUNT(DISTINCT CASE WHEN work_model_normalized = 'On-Site' THEN url END) AS onsite_jobs_count,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN education_requirement = 'Bachelor+' THEN url END) / COUNT(DISTINCT url), 2) AS pct_bachelor_required,
    ROUND(AVG(salary_avg_million), 1) AS avg_salary,
    COUNT(DISTINCT CASE WHEN salary_avg_million IS NOT NULL THEN url END) AS salaried_positions_count,
    MIN(job_posted_date) AS earliest_job_date,
    MAX(job_posted_date) AS latest_job_date
FROM {{ ref('fact_jobs') }}
WHERE job_category IS NOT NULL AND job_category != 'Others'
GROUP BY source_platform
