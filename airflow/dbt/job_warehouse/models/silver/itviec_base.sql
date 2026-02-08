{{ config(
    materialized='table',
    tags=['silver_layer', 'fact_table']
) }}

WITH itviec_jobs AS (
    SELECT
        'itviec' AS source_platform,
        b.url,
        b.logo_url,
        b.title,
        b.company,
        {{ location_normalization('b.working_location') }} AS working_location,
        'Full-time' AS work_arrangement,
        b.work_model,
        CAST(NULL AS VARCHAR(100)) AS salary,
        COALESCE(jcm.target_job_category, 'Others') AS job_category,
        CAST(NULL AS DECIMAL) AS salary_min_million,
        CAST(NULL AS DECIMAL) AS salary_max_million,
        {{ least_level_of_education('requirements_and_experiences') }} AS education_requirement,
        b.descriptions,
        b.requirements_and_experiences AS requirements,
        {{ extract_experience('b.requirements_and_experiences') }} AS year_of_experiences,
        b.tags,
        b.posted_to_discord,
        CAST(b.created_at AS DATE) AS job_posted_date,
        b.created_at AS job_posted_timestamp
    FROM {{ ref('itviec_jobs_raw') }} b
    LEFT JOIN {{ ref('job_category_mapping') }} jcm
        ON REGEXP_LIKE(lower(b.title), jcm.pattern)
)
SELECT * FROM itviec_jobs