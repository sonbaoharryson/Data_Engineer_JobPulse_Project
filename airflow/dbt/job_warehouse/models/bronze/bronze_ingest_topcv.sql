{{ config(materialized='table', tags=['bronze_layer', 'basic_cleanse', 'topcv'])}}

SELECT
    id,
    {{ initcap_and_trim('title') }} AS title,
    {{ upper_and_trim('company') }} AS company,
    {{ lower_and_trim('logo_url') }} AS logo_url,
    {{ lower_and_trim('url') }} AS url,
    {{ initcap_and_trim('working_location') }} AS working_location,
    {{ initcap_and_trim('salary') }} AS salary,
    {{ lower_and_trim('descriptions') }} AS descriptions,
    {{ lower_and_trim('requirements') }} AS requirements,
    {{ lower_and_trim('experiences') }} AS experiences,
    {{ initcap_and_trim('level_of_education') }} AS level_of_education,
    {{ initcap_and_trim('work_model') }} AS work_model,
    posted_to_discord,
    created_at
from {{ source('job_raw', 'topcv_data_job') }}