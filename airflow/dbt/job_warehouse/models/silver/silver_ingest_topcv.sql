{{ config(materialized='table', tags=['silver_layer', 'enrich_education_column', 'topcv'])}}

SELECT
    *,
    {{ least_level_of_education('level_of_education') }} AS least_level_of_education,
    {{ extract_experience('experiences') }} AS exp
FROM {{ source('bronze_layer', 'bronze_ingest_topcv') }}