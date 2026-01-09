import os
import logging
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from tasks.tasks_group import itviec_pipeline, topcv_pipeline


logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

default_args = {
    "owner": "sonbao",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    default_args=default_args,
    dag_id="master_job_elt",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=[
        "master_dag",
        "itviec_process",
        "topcv_process",
        "upload_discord",
        "dbt_process",
    ],
)
def master_elt():

    itviec_process_pipeline = itviec_pipeline()
    topcv_process_pipeline = topcv_pipeline()

dag = master_elt()
