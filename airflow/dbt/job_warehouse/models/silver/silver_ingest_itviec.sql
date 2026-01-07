{{ config(materialized='table', tags=['silver_layer', 'enrich_education_column', 'itviec'])}}
with temp as(
SELECT
    *,
    {{ least_level_of_education('requirements_and_experiences') }} AS least_level_of_education,
    {{ extract_experience('requirements_and_experiences') }} AS exp
FROM {{ source('bronze_layer', 'bronze_ingest_itviec') }}
)
select * from temp where exp <> 'Unknown'