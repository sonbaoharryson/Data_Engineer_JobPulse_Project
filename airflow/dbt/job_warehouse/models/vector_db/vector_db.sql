{{ config(
    materialized='incremental',
    unique_key='job_id',
    incremental_strategy='merge',
    tags=['vector_db', 'embedding']
)}}

SELECT
    job_id,
    url,
    job_title,
    year_of_experiences,
    experiences_level,
    salary_avg_million,
    salary,
    tags,
    job_posted_date,
    company_name,
    job_location,
    CONCAT(
        'Job Title: ', COALESCE(job_title, ''), '. ',
        'Company: ', COALESCE(company_name, ''), '. ',
        'Location: ', COALESCE(job_location, ''), '. ',
        'Experience Level: ', COALESCE(experiences_level, ''), '. ',
        'Years of Experience: ', COALESCE(year_of_experiences, ''), '. ',
        'Work Type: ', COALESCE(work_arrangement_normalized, ''), '. ',
        'Requirements: ', COALESCE(requirements, ''), '. ',
        'Description: ', COALESCE(descriptions, ''), '. ',
        'Tags: ', COALESCE(tags, '')
    ) AS embedding_text

FROM {{ ref('fact_jobs') }}

{% if is_incremental() %}
WHERE job_posted_date >= (
    SELECT COALESCE(MAX(job_posted_date), DATE '1900-01-01')
    FROM {{ this }}
)
{% endif %}