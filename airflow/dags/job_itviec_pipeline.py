import os
import logging
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from tasks.tasks_group import itviec_pipeline
from tasks.audit_tasks import task_failure_callback, task_success_callback

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

#Define DAG
default_args = {
    'owner': 'sonbao',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'on_success_callback': task_success_callback,
    'on_failure_callback': task_failure_callback
}

@dag(
    dag_id='itviec_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['itviec_pipeline']
)

def _itviec_pipeline():

    itviec_pipeline()

dag = _itviec_pipeline()