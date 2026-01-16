{{ config(materialized='table', tags=['summary', 'audit', 'log_tasks']) }}

SELECT
    task_name,
    DATE(execution_date) as execution_day,
    dag_status,
    COUNT(DISTINCT dag_run_id) as total_dag_runs,
    COUNT(DISTINCT task_id) as total_tasks,
    SUM(CASE WHEN task_status = 'success' THEN 1 ELSE 0 END) as successful_tasks,
    SUM(CASE WHEN task_status = 'failed' THEN 1 ELSE 0 END) as failed_tasks,
    SUM(rows_processed) as total_rows_processed,
    SUM(rows_inserted) as total_rows_inserted,
    SUM(rows_scraped) as total_rows_scraped,
    SUM(discord_posts_sent) as total_discord_posts_sent,
    SUM(discord_posts_failed) as total_discord_posts_failed,
    AVG(duration_seconds) as avg_dag_duration_seconds,
    MIN(start_date) as first_start_time,
    MAX(end_date) as last_end_time
FROM {{ source('audit_layer', 'master_job_elt_audit')}}
WHERE dag_id = 'master_job_elt'
GROUP BY task_name, DATE(execution_date), dag_status
ORDER BY execution_day DESC, dag_status