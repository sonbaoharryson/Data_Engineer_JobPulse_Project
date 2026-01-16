import os
import sys
import logging
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict
from scripts.crawl_scripts.crawl_job.crawler import Crawler
from scripts.utils.load_crawl_source import load_crawl_sources
from scripts.utils.insert_data_staging import insert_itviec_jobs, insert_topcv_jobs
from scripts.utils.sender import query_unposted_jobs, mark_jobs_as_posted, send_job_alerts

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
DISCORD_CHANNEL_ID = int(os.getenv('DISCORD_CHANNEL_ID'))

def load_crawl_sources_url(source_crawl:str):
    if source_crawl=='itviec':
        data = load_crawl_sources(file_name='source_itviec.json')
    elif source_crawl=='topcv':
        data = load_crawl_sources(file_name='source_topcv.json')
    return data

def scrape_source_job(sources: dict, source_crawl:str):
    crawler = Crawler(source_crawl)
    total_data_job = []
    for source, url in sources.items():
        logger.info(f"Processing source: {source} with URL: {url}")
        try:
            dict_jobs = crawler.crawler(url)
            if dict_jobs:
                logger.info(f"Successfully scraped {len(dict_jobs)} jobs from {source_crawl}")
        except Exception as e:
            logger.error(f"Error scraping {source_crawl}: {e}")
        total_data_job += dict_jobs

    return_dict = {
            'rows_processed': 0,
            'rows_inserted': 0,
            'rows_scraped':total_data_job,
            'posts_sent': 0
        }
    return return_dict

def insert_jobs_to_staging_layer(data:List[Dict], source_crawl:str):
    if not data:
        logger.info(f"No {source_crawl} jobs to insert")
        return {}

    if source_crawl=='itviec':
        insert_itviec_jobs(data)
    elif source_crawl=='topcv':
        insert_topcv_jobs(data)
    return_dict = {
            'rows_processed': 0,
            'rows_inserted': len(data),
            'rows_scraped':0,
            'posts_sent': 0
        }
    return return_dict

def post_job_to_discord(crawl_source:str):
    query_data = query_unposted_jobs(table_name='itviec_data_job') if crawl_source=='itviec' else query_unposted_jobs(table_name='topcv_data_job')
    jobs, urls = query_data
    logger.info(f"Found {len(jobs)} new {crawl_source} jobs to post to Discord")
    if not jobs:
        logger.info(f'No new jobs from {crawl_source} to post to Discord')
        return {}
    try:
        posts_send, posts_failed_sent = send_job_alerts(jobs, DISCORD_TOKEN, DISCORD_CHANNEL_ID)
        mark_jobs_as_posted(table_name='itviec_data_job', job_urls=urls) if crawl_source=='itviec' else mark_jobs_as_posted(table_name='topcv_data_job', job_urls=urls)
        return_dict = {
                'posts_sent': posts_send,
                'posts_failed': posts_failed_sent
            }
        return return_dict
    except Exception as e:
        logger.error(f"Error sending job alerts to Discord: {e}")