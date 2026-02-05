{{ config(
    materialized='table',
    tags=['gold_layer', 'mart']
) }}

SELECT
    e.education_id,
    e.education_requirement,
    e.job_count,
    e.num_companies,
    e.num_locations,
    e.avg_salary,
    e.pct_of_total_jobs,
    e.source_platforms,
    ROW_NUMBER() OVER (ORDER BY e.job_count DESC) AS demand_rank,
    ROUND(AVG(e.avg_salary) OVER (), 1) AS market_avg_salary,
    e.avg_salary - ROUND(AVG(e.avg_salary) OVER (), 1) AS salary_vs_market,
    ROUND((e.avg_salary - ROUND(AVG(e.avg_salary) OVER (), 1)) / 
        ROUND(AVG(e.avg_salary) OVER (), 1) * 100, 2) AS salary_vs_market_pct,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS dbt_load_timestamp
FROM {{ ref('dim_education') }} e
ORDER BY demand_rank
