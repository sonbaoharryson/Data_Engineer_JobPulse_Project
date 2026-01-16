{{ config(materialized='view') }}
select
    id,
    title,
    company,
    working_location,
    url,
    created_at
from {{ source('job_raw', 'itviec_data_job') }}
