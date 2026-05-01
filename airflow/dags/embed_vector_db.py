import os
import logging
import sys

sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag
from datetime import datetime, timedelta
from tasks.tasks_group import embedding_data_vector_db_group

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

# Define DAG
default_args = {
    "owner": "sonbao",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="embed_vector_db_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["embed_pipeline"],
)
def _embed_vector_db_group_task():

    embedding_data_vector_db_group()


dag = _embed_vector_db_group_task()
