select
    id,
    title,
    company,
    location,
    url,
    created_at
from {{ source('job_raw', 'itviec_data_job') }}
