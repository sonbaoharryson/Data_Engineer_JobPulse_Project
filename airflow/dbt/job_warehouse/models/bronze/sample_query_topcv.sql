select
    id,
    title,
    company,
    location,
    salary,
    url,
    created_at
from {{ source('job_raw', 'topcv_data_job') }}
