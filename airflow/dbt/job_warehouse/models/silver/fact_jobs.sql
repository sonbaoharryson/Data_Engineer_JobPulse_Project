{{ config(
    materialized='table',
    tags=['silver_layer', 'fact_table'],
    indexes=[
        {'columns': ['source_platform', 'job_posted_date'], 'type': 'btree'},
        {'columns': ['company_name'], 'type': 'hash'},
        {'columns': ['job_location'], 'type': 'hash'}
    ]
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
),
itviec_jobs AS (
    SELECT
        'itviec' AS source_platform,
        b.url,
        b.logo_url,
        b.title,
        b.company,
        {{ location_normalization('b.working_location') }} AS working_location,
        'Full-time' AS work_arrangement,
        b.work_model,
        NULL AS salary,
        COALESCE(jcm.target_job_category, 'Others') AS job_category,
        NULL AS salary_min_million,
        NULL AS salary_max_million,
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
),
unified_jobs AS (
    SELECT * FROM topcv_jobs
    UNION ALL
    SELECT * FROM itviec_jobs
),
explode_jobs AS (
    SELECT
        source_platform,
        url,
        logo_url,
        title,
        company,
        work_model,
        work_arrangement,
        job_category,
        salary,
        salary_min_million,
        salary_max_million,
        education_requirement,
        descriptions,
        requirements,
        year_of_experiences,
        tags,
        posted_to_discord,
        job_posted_date,
        job_posted_timestamp,
        trim(job_loc_new) AS working_location
    FROM unified_jobs
    CROSS JOIN UNNEST(SPLIT(working_location, '-')) AS t(job_loc_new)
    WHERE job_loc_new NOT LIKE '%nơi khác%'
),
enriched AS (
    SELECT
        TO_HEX(MD5(TO_UTF8(CONCAT(source_platform, '|', url)))) AS job_id,
        source_platform,
        url,
        cl.logo_id AS logo_id,
        title AS job_title,
        company AS company_name,
        TRIM(vn_city_mapping.city_en) AS job_location,
        job_category,
        -- Normalize work model
        CASE 
            WHEN LOWER(TRIM(work_model)) IN ('hybrid', 'lai') THEN 'Hybrid'
            WHEN LOWER(TRIM(work_model)) IN ('remote', 'từ xa') THEN 'Remote'
            WHEN LOWER(TRIM(work_model)) IN ('at office', 'tại văn phòng', 'on-site', 'on site') THEN 'On-Site'
            ELSE TRIM(work_model)
        END AS work_model_normalized,
        CASE
            WHEN LOWER(TRIM(work_arrangement)) IN ('toàn thời gian', 'full-time', 'full time', 'full-time', 'fulltime') THEN 'Full-time'
            WHEN LOWER(TRIM(work_arrangement)) IN ('bán thời gian', 'part-time', 'part time') THEN 'Part-time'
            WHEN LOWER(TRIM(work_arrangement)) IN ('thực tập', 'internship', 'intern') THEN 'Internship'
            ELSE TRIM(work_arrangement)
        END AS work_arrangement_normalized,
        salary,
        salary_min_million,
        salary_max_million,
        {{ salary_value('salary_min_million', 'salary_max_million') }} AS salary_avg_million,
        {{ salary_bench(salary_value('salary_min_million', 'salary_max_million')) }} AS salary_band,
        education_requirement,
        descriptions,
        requirements,
        year_of_experiences,
        tags,
        posted_to_discord,
        job_posted_date,
        job_posted_timestamp,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dbt_load_timestamp
    FROM explode_jobs
    LEFT JOIN {{ ref('vn_city_mapping') }} vn_city_mapping
        ON REGEXP_LIKE(
            explode_jobs.working_location,
            vn_city_mapping.pattern
        )
    LEFT JOIN {{ source('job_raw', 'company_logos') }} cl
    ON explode_jobs.logo_url = cl.logo_url
)

SELECT * FROM enriched

{% if is_incremental() %}
    WHERE job_posted_date > (SELECT MAX(job_posted_date) FROM {{ this }})
{% endif %}
