{{ config(materialized='table', tags=['gold_layer', 'job_fact']) }}

SELECT
    TO_HEX(MD5(TO_UTF8(job_category))) AS job_category_id,
    TO_HEX(MD5(TO_UTF8(CONCAT(source_platform, '|', company_name)))) AS company_id,
    TO_HEX(MD5(TO_UTF8(CONCAT(source_platform, '|', url)))) AS job_id,
    TO_HEX(MD5(TO_UTF8(work_model_normalized))) AS work_model_id,
    TO_HEX(MD5(TO_UTF8(job_location))) AS location_id,
    TO_HEX(MD5(TO_UTF8(education_requirement))) AS education_id,
    TO_HEX(MD5(TO_UTF8(salary_band))) AS salary_band_id,
    source_platform,
    url,
    logo_id,
    job_title,
    year_of_experiences,
    experiences_level,
    work_arrangement_normalized,
    salary_avg_million,
    salary,
    tags,
    job_posted_date
FROM {{ ref('fact_jobs') }}
WHERE job_category IS NOT NULL AND job_category != 'Others'