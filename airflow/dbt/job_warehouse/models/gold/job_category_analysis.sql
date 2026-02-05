{{ config(
    materialized='table',
    tags=['gold_layer', 'mart']
) }}

SELECT
    TO_HEX(MD5(TO_UTF8(job_category))) AS job_category_id,
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS title_rank,
    job_category,
    COUNT(*) AS num_positions,
    COUNT(DISTINCT company_name) AS num_companies,
    COUNT(DISTINCT job_location) AS num_locations,
    ROUND(100.0 * COUNT(*) / 
        (SELECT COUNT(*) FROM {{ ref('fact_jobs') }}) * 100, 2) AS pct_of_all_jobs,
    ROUND(AVG(salary_avg_million), 1) AS avg_salary,
    MIN(salary_min_million) AS min_salary,
    MAX(salary_max_million) AS max_salary,
    COUNT(DISTINCT CASE WHEN work_model_normalized = 'Remote' THEN url END) AS remote_positions,
    COUNT(DISTINCT CASE WHEN work_model_normalized = 'Hybrid' THEN url END) AS hybrid_positions,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN education_requirement = 'Bachelor+' THEN url END) / COUNT(*), 2) AS pct_bachelor_required,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dbt_load_timestamp
FROM {{ ref('fact_jobs') }}
WHERE job_category IS NOT NULL AND job_category != 'Others'
GROUP BY job_category
ORDER BY title_rank
