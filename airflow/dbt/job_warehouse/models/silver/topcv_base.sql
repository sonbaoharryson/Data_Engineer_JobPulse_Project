{{ config(
    materialized='table',
    tags=['silver_layer', 'fact_table']
) }}

WITH topcv_jobs AS (
    SELECT
        'topcv' AS source_platform,
        b.url,
        b.logo_url,
        b.title,
        b.company,
        {{ location_normalization('b.working_location') }} AS working_location,
        b.work_model AS work_arrangement,
        'On-Site' AS work_model,
        b.salary,
        COALESCE(jcm.target_job_category, 'Others') AS job_category,
        {{ min_salary_offered('b.salary') }} AS salary_min_million,
		{{ max_salary_offered('b.salary') }} AS salary_max_million,
        {{ least_level_of_education('b.level_of_education') }} AS education_requirement,
        b.descriptions,
        b.requirements,
        {{ extract_experience('b.experiences') }} AS year_of_experiences,
        COALESCE(b.job_category, NULL) AS tags,
        b.posted_to_discord,
        CAST(b.created_at AS DATE) AS job_posted_date,
        b.created_at AS job_posted_timestamp
    FROM {{ ref('topcv_jobs_raw') }} b
    LEFT JOIN {{ ref('job_category_mapping') }} jcm
        ON REGEXP_LIKE(lower(b.title), jcm.pattern)
)
SELECT * FROM topcv_jobs