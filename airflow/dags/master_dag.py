import os
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag
from datetime import datetime, timedelta
from tasks.tasks_group import itviec_pipeline, topcv_pipeline, post_job_group, dbt_wh_pipeline
from tasks.audit_tasks import task_failure_callback, task_success_callback

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
        "itviec_pipeline",
        "topcv_pipeline",
        "upload_discord",
        "dbt_pipeline",
    ],
)
def master_elt():

    itviec_insert = itviec_pipeline()
    topcv_insert = topcv_pipeline()

    post_tasks = post_job_group()
    bronze_task = dbt_wh_pipeline()

    itviec_insert >> post_tasks["itviec"]
    topcv_insert >> post_tasks["topcv"]
    [itviec_insert, topcv_insert] >> bronze_task

dag = master_elt()