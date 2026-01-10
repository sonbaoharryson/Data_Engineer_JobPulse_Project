import os
import logging
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from tasks.process_tasks import post_job_to_discord
from tasks.tasks_group import itviec_pipeline, topcv_pipeline
from tasks.audit_tasks import dbt_task_callback, discord_task_callback, task_failure_callback, task_success_callback
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

PROJECT_DIR = '/opt/airflow/dbt/job_warehouse'
PROFILE_DIR = '/opt/airflow/dbt/job_warehouse'

default_args = {
    "owner": "sonbao",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "on_success_callback": task_success_callback,
    "on_failure_callback": task_failure_callback
}


@dag(
    default_args=default_args,
    dag_id="master_job_elt",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule='@daily',
    tags=[
        "master_dag",
        "itviec_process",
        "topcv_process",
        "upload_discord",
        "dbt_process",
    ],
)
def master_elt():

    @task.bash(
        on_success_callback=dbt_task_callback,
        on_failure_callback=task_failure_callback
    )
    def process_bronze_wh_layer():
        logger.info('Starting process data to bronze layer!!!')
        return f'dbt run --select bronze --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash(
        on_success_callback=dbt_task_callback,
        on_failure_callback=task_failure_callback
    )
    def process_silver_wh_layer():
        logger.info('Starting process data to silver layer!!!')
        return f'dbt run --select silver --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash(
        on_success_callback=dbt_task_callback,
        on_failure_callback=task_failure_callback
    )
    def process_gold_wh_layer():
        logger.info('Starting process data to gold layer!!!')
        return f'dbt run --select gold --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash
    def process_audit_wh_layer():
        logger.info('Starting process data to audit layer!!!')
        return f'dbt run --select audit --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task(
        on_success_callback=discord_task_callback,
        on_failure_callback=task_failure_callback
    )
    def job_post_itviec(crawl_source:str):
        return post_job_to_discord(crawl_source)

    @task(
        on_success_callback=discord_task_callback,
        on_failure_callback=task_failure_callback
    )
    def job_post_topcv(crawl_source:str):
        return post_job_to_discord(crawl_source)

    @task_group
    def dbt_wh_pipeline():
        bronze = process_bronze_wh_layer()
        silver = process_silver_wh_layer()
        gold = process_gold_wh_layer()
        audit = process_audit_wh_layer()
        bronze >> [
            job_post_itviec(crawl_source='itviec'),
            job_post_topcv(crawl_source='topcv'),
            silver
        ]
        silver >> gold >> audit

    [itviec_pipeline(), topcv_pipeline()] >> dbt_wh_pipeline()

dag = master_elt()