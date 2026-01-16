{{ config(materialized='table', tags=['silver_layer', 'unified']) }}

SELECT
    'itviec' AS source,
    url,
    title,
    company AS company_name,
    working_location,
    work_model,
    NULL AS salary,
    least_level_of_education,
    exp AS experience_required,
    created_at
FROM {{ ref('silver_ingest_itviec') }}

UNION ALL

SELECT
    'topcv' AS source,
    url,
    title,
    company AS company_name,
    working_location,
    work_model,
    salary,
    least_level_of_education,
    exp AS experience_required,
    created_at
FROM {{ ref('silver_ingest_topcv') }}
