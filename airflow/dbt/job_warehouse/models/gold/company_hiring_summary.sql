{{ config(
    materialized='table',
    tags=['gold_layer', 'mart']
) }}

SELECT
    c.company_id,
    ROW_NUMBER() OVER (ORDER BY c.total_job_postings DESC) AS company_rank,
    c.company_name,
    c.source_platform,
    c.total_job_postings,
    c.num_locations,
    c.discord_postings,
    c.pct_discord_posted,
    DATE_DIFF('day', CURRENT_DATE, c.first_posting_date) + 1 AS active_days_range,
    c.first_posting_date,
    c.last_posting_date,
    c.avg_salary_offered,
    COUNT(DISTINCT j.job_location) AS unique_locations_hiring,
    COUNT(DISTINCT j.job_title) AS unique_job_titles,
    c.work_models_offered
FROM {{ ref('dim_company') }} c
LEFT JOIN {{ ref('fact_jobs') }} j
    ON c.company_name = j.company_name
    AND c.source_platform = j.source_platform
WHERE j.job_category IS NOT NULL AND j.job_category != 'Others'
GROUP BY 
    c.company_id, c.company_name, c.source_platform, 
    c.total_job_postings, c.num_locations, c.discord_postings, 
    c.pct_discord_posted, c.first_posting_date, c.last_posting_date, 
    c.avg_salary_offered, c.work_models_offered
