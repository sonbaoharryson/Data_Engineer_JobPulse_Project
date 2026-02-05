{{ config(
    materialized='table',
    tags=['gold_layer', 'mart']
) }}

SELECT
    j.job_category,
    j.job_location,
    j.salary_band,
    COUNT(*) AS num_positions,
    COUNT(DISTINCT CASE WHEN j.salary_avg_million IS NOT NULL THEN j.url END) AS salaried_positions,
    ROUND(AVG(j.salary_avg_million), 1) AS avg_salary,
    approx_percentile(salary_avg_million, 0.25) AS salary_q1,
    approx_percentile(salary_avg_million, 0.50) AS salary_median,
    approx_percentile(salary_avg_million, 0.75) AS salary_q3,
    MIN(j.salary_min_million) AS min_salary,
    MAX(j.salary_max_million) AS max_salary,
    COUNT(DISTINCT CASE WHEN j.work_model_normalized = 'Remote' THEN j.url END) AS remote_positions,
    COUNT(DISTINCT CASE WHEN j.work_model_normalized = 'Hybrid' THEN j.url END) AS hybrid_positions,
    COUNT(DISTINCT CASE WHEN j.work_model_normalized = 'On-Site' THEN j.url END) AS on_site_positions,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dbt_load_timestamp
FROM {{ ref('fact_jobs') }} j
WHERE j.job_category IS NOT NULL AND j.job_category != 'Others'
GROUP BY j.job_category, j.job_location, j.salary_band
ORDER BY j.job_location, j.job_category, num_positions DESC
