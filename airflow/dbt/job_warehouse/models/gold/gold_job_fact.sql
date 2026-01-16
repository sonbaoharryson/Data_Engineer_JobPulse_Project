{{ config(materialized='table', tags=['gold_layer', 'job_fact']) }}

SELECT
    source,
    url,
    title,
    company_name,
    working_location,
    work_model,
    salary,
    least_level_of_education,
    experience_required,
    cast(created_at as date) AS t_rec_ins_dt
FROM {{ ref('silver_jobs_unified') }}