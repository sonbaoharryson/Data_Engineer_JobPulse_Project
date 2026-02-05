{{ config(
    materialized='table',
    schema='silver',
    alias='dim_company_logo'
) }}

SELECT
    DISTINCT logo_id,
        logo_path
FROM {{ source('job_raw', 'company_logos') }}
