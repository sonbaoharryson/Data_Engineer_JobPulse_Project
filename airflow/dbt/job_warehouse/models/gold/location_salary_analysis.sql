{{ config(
    materialized='table',
    tags=['gold_layer', 'mart']
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY l.avg_salary_by_location DESC NULLS LAST) AS salary_rank,
    l.location_id,
    l.job_location,
    l.total_jobs,
    l.num_companies,
    l.num_platforms,
    ROUND(100.0 * l.total_jobs / 
        (SELECT COUNT(*) FROM {{ ref('fact_jobs') }}) * 100, 2) AS pct_of_total_jobs,
    l.avg_salary_by_location,
    l.highest_salary,
    l.lowest_salary,
    l.remote_jobs,
    l.hybrid_jobs,
    l.onsite_jobs,
    l.fulltime_jobs,
    ROUND(100.0 * l.remote_jobs / l.total_jobs, 2) AS pct_remote,
    ROUND(100.0 * l.hybrid_jobs / l.total_jobs, 2) AS pct_hybrid,
    l.education_requirements,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dbt_load_timestamp
FROM {{ ref('dim_location') }} l
ORDER BY salary_rank
