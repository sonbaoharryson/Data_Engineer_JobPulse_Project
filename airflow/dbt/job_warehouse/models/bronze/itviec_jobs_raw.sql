{{ config(
    materialized='incremental',
    unique_key='url',
    incremental_strategy='merge',
    tags=['bronze_layer', 'basic_cleanse', 'itviec']
)}}

SELECT
    id,
    {{ initcap_and_trim('title') }} AS title,
    {{ upper_and_trim('company') }} AS company,
    TRIM(logo_url) AS logo_url,
    TRIM(url) AS url,
    job_category,
    {{ initcap_and_trim('working_location') }} AS working_location,
    {{ initcap_and_trim('work_model') }} AS work_model,
    {{ initcap_and_trim('tags') }} AS tags,
    {{ lower_and_trim('descriptions') }} AS descriptions,
    {{ lower_and_trim('requirements_and_experiences') }} AS requirements_and_experiences,
    posted_to_discord,
    created_at
FROM {{ source('job_raw', 'itviec_data_job') }}

{% if is_incremental() %}
    -- Only process new records since last run
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}