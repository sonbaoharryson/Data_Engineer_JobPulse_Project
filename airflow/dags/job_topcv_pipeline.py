import os
import sys
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from scripts.crawl_scripts.crawl_job.crawler import Crawler
from scripts.utils.insert_data_bronze import insert_topcv_jobs
from scripts.utils.load_crawl_source import load_crawl_sources
from scripts.utils.sender import query_unposted_jobs, mark_jobs_as_posted, send_job_alerts
import logging

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
DISCORD_CHANNEL_ID = int(os.getenv('DISCORD_CHANNEL_ID'))

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
        data = load_crawl_sources(file_name='source_topcv.json')
        return data

    @task
    def scrape_topcv_job(sources: dict):
        crawler = Crawler("topcv")
        total_data_job_topcv = []
        for source, url in sources.items():
            print(f"Processing source: {source} with URL: {url}")
            try:
                topcv_jobs = crawler.crawler(url)
                if topcv_jobs:
                    print(f"Successfully scraped {len(topcv_jobs)} jobs from TopCV")
            except Exception as e:
                print(f"Error scraping TopCV: {e}")
            total_data_job_topcv += topcv_jobs
        return total_data_job_topcv if total_data_job_topcv else None

    @task
    def insert_jobs(data):
        if not data:
            print("No TopCV jobs to insert")
            return
        insert_topcv_jobs(data)

    @task
    def post_job_to_discord():
        query_data = query_unposted_jobs(table_name='topcv_data_job')
        jobs, urls = query_data
        logger.info(f"Found {len(jobs)} new TopCV jobs to post to Discord")
        if not jobs:
            logger.info('No new jobs from TopCV to post to Discord')
            return
        try:
            send_job_alerts(jobs, DISCORD_TOKEN, DISCORD_CHANNEL_ID)
            mark_jobs_as_posted(table_name='topcv_data_job', job_urls=urls)
        except Exception as e:
            logger.error(f"Error sending job alerts to Discord: {e}")

    topcv_sources=load_topcv_url()
    job_data=scrape_topcv_job(topcv_sources)
    insert_jobs(job_data) >> post_job_to_discord()

dag = topcv_pipeline()