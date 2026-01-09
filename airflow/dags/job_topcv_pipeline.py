import os
import logging
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from tasks.process_tasks import load_crawl_sources_url, scrape_source_job, insert_jobs_to_staging_layer, post_job_to_discord

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
    dag_id='topcv_data_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=15),
    catchup=False,
    tags=['topcv_pipeline']
)

def topcv_pipeline():

    @task
    def load_topcv_url():
        return load_crawl_sources_url(source_crawl='topcv')

    @task
    def scrape_topcv_job(sources: dict):
        return scrape_source_job(sources=sources, source_crawl='topcv')


    @task
    def insert_jobs_topcv(data):
        insert_jobs_to_staging_layer(data=data, source_crawl='topcv')


    @task
    def post_job_to_discord_topcv():
        post_job_to_discord(crawl_source='topcv')


    topcv_sources = load_topcv_url()
    job_data = scrape_topcv_job(topcv_sources)
    insert = insert_jobs_topcv(job_data)
    discord = post_job_to_discord_topcv()

    insert >> discord

    # insert >> bronze
    # bronze >> [silver, discord] >> gold

dag = topcv_pipeline()