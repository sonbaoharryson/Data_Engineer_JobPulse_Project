{{
    config(
        materialized='table',
        tags=['gold_layer', 'mart', 'master_date']
    )
}}

SELECT
    DISTINCT job_posted_date AS date,
    MONTH(job_posted_date) AS month,
    QUARTER(job_posted_date) AS quarter,
    YEAR(job_posted_date) AS year,
    EXTRACT(day FROM job_posted_date) AS day_of_month,
    EXTRACT(week FROM job_posted_date) AS week_of_year,
    EXTRACT(month FROM job_posted_date) AS month_of_year
FROM {{ ref('job_fact') }}