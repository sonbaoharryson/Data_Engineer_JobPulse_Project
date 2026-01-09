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
    dag_id='itviec_data_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['itviec_pipeline']
)

def itviec_pipeline():

    @task
    def load_itviec_url():
        return load_crawl_sources_url(source_crawl='itviec')

    @task
    def scrape_itviec_job(sources: dict):
        return scrape_source_job(sources=sources, source_crawl='itviec')

    @task
    def insert_jobs_itviec(data):
        insert_jobs_to_staging_layer(data=data, source_crawl='itviec')

    @task
    def post_job_to_discord_itviec():
        post_job_to_discord(crawl_source='itviec')

    itviec_sources = load_itviec_url()
    job_data = scrape_itviec_job(itviec_sources)
    insert = insert_jobs_itviec(job_data)
    discord = post_job_to_discord_itviec()

    insert >> discord

    # insert >> bronze
    # bronze >> [silver, discord] >> gold

dag = itviec_pipeline()