{{ config(materialized='table', tags=['gold_layer', 'job_fact']) }}

SELECT
    TO_HEX(MD5(TO_UTF8(job_category))) AS job_category_id,
    TO_HEX(MD5(TO_UTF8(CONCAT(source_platform, '|', company_name)))) AS company_id,
    TO_HEX(MD5(TO_UTF8(CONCAT(source_platform, '|', url)))) AS job_id,
    TO_HEX(MD5(TO_UTF8(work_model_normalized))) AS work_model_id,
    TO_HEX(MD5(TO_UTF8(job_location))) AS location_id,
    TO_HEX(MD5(TO_UTF8(education_requirement))) AS education_id,
    source_platform,
    url,
    logo_id,
    job_title,
    work_arrangement_normalized,
    salary_band,
    salary,
    year_of_experiences,
    tags,
    job_posted_date,
    job_posted_timestamp,
    dbt_load_timestamp
FROM {{ ref('fact_jobs') }}