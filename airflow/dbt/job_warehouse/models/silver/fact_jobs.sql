{{ config(
    materialized='table',
    tags=['silver_layer', 'fact_table', 'heavy_compute']
) }}

WITH unified_jobs AS (
    SELECT * FROM {{ ref('topcv_base') }}
    UNION ALL
    SELECT * FROM {{ ref('itviec_base') }}
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
        year_of_experiences year_of_experiences_raw,
        {{ yoe_normalized('year_of_experiences') }} AS year_of_experiences_normalized,
        {{ yoe_band(yoe_normalized('year_of_experiences')) }} AS year_of_experiences,
        {{ yoe_level(yoe_normalized('year_of_experiences')) }} AS experiences_level,
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
