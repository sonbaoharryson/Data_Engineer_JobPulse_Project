{{ config(materialized='table') }}

SELECT
    t_rec_ins_dt,
    source,
    COUNT(*) AS total_jobs
FROM {{ ref('gold_job_fact') }}
GROUP BY 1, 2
