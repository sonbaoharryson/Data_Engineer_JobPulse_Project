select
    id,
    title,
    company,
    working_location,
    salary,
    url,
    created_at
from {{ source('job_raw', 'topcv_data_job') }}
