import os
import logging
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from tasks.tasks_group import topcv_pipeline
from tasks.audit_tasks import task_failure_callback, task_success_callback

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


#Define DAG
default_args = {
    'owner': 'sonbao',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

@dag(
    dag_id='topcv_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['topcv_pipeline']
)

def _topcv_pipeline():

    topcv_pipeline()
    # insert >> bronze
    # bronze >> [silver, discord] >> gold

dag = _topcv_pipeline()