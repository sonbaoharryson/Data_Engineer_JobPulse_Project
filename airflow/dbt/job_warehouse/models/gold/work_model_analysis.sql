{{ config(
    materialized='table',
    tags=['gold_layer', 'mart']
) }}

SELECT
    w.work_model_id,
    w.work_model_normalized,
    w.job_count,
    w.num_companies,
    w.num_locations,
    w.avg_salary,
    w.pct_of_total_jobs,
    w.num_platforms,
    ROW_NUMBER() OVER (ORDER BY w.job_count DESC) AS demand_rank,
    ROUND(AVG(w.avg_salary) OVER (), 1) AS market_avg_salary,
    ROUND(w.avg_salary - AVG(w.avg_salary) OVER (), 1) AS salary_vs_market,
    CASE 
        WHEN w.avg_salary > AVG(w.avg_salary) OVER () THEN 'Above Average'
        WHEN w.avg_salary < AVG(w.avg_salary) OVER () THEN 'Below Average'
        ELSE 'At Market'
    END AS salary_positioning
FROM {{ ref('dim_work_model') }} w
