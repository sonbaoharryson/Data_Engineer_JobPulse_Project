import os
import sys
import logging
import json
sys.path.insert(1, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from typing import List, Dict

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

def load_crawl_sources_url(source_crawl:str):
    from scripts.utils.load_crawl_source import load_crawl_sources

    if source_crawl=='itviec':
        data = load_crawl_sources(file_name='source_itviec.json')
    elif source_crawl=='topcv':
        data = load_crawl_sources(file_name='source_topcv.json')
    return data

def upload_crawl_data_to_minio(data:List[Dict], source_crawl:str, bucket_name:str="crawled-data"):
    import datetime
    if not data:
        logger.info(f"No {source_crawl} jobs to upload to MinIO")
        return {}

    from scripts.utils.minio_conn import MinIOConnection

    minio_conn = MinIOConnection()
    destination_file = f"{source_crawl}/{source_crawl}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_jobs.json"

    try:
        minio_conn.upload_data_object(bucket_name=bucket_name, destination_file=destination_file, data_object=data)
        logger.info(f"Uploaded {len(data)} {source_crawl} jobs to MinIO at {destination_file}")
        return destination_file
    except Exception as e:
        logger.error(f"Error uploading {source_crawl} jobs to MinIO: {e}")
        return None

def get_data_from_minio(source_crawl:str, file_path:str):
    from scripts.utils.minio_conn import MinIOConnection

    minio_conn = MinIOConnection()
    bucket_name = "crawled-data"
    _data = minio_conn.read_file(bucket_name=bucket_name, object_name=file_path)
    data = json.loads(_data) if _data else []
    logger.info(f"Retrieved {len(data)} {source_crawl} jobs from MinIO at {file_path}")
    return data

def deduplicate_jobs(jobs: list[dict], key: str = "url") -> list[dict]:
    seen = set()
    deduped = []

    for job in jobs:
        if not isinstance(job, dict):
            continue

        value = job.get(key)
        if not value:
            continue

        if value not in seen:
            seen.add(value)
            deduped.append(job)

    return deduped

def scrape_source_job(sources: dict, source_crawl:str):
    from scripts.crawl_scripts.crawl_job.crawler import Crawler
    from scripts.validation.ge_runner import run_ge_validation
    from scripts.validation.itviec import expectations as itviec_expectations
    from scripts.validation.topcv import expectations as topcv_expectations

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
    
    deduped_jobs = deduplicate_jobs(total_data_job)
    
    source_expectations = itviec_expectations if source_crawl=='itviec' else topcv_expectations
    
    run_ge_validation(
        records=deduped_jobs,
        expectation_fn=source_expectations,
        source_name=source_crawl
    )
    
    upload_file_path = upload_crawl_data_to_minio(data=deduped_jobs, source_crawl=source_crawl)
    return_dict = {
            'rows_processed': 0,
            'rows_inserted': 0,
            'rows_scraped':len(deduped_jobs),
            'posts_sent': 0,
            'uploaded_file_path': upload_file_path
        }
    return return_dict

def insert_jobs_to_staging_layer(data_file_path: str, source_crawl:str):
    from scripts.utils.insert_data_staging import (
        insert_itviec_jobs,
        insert_topcv_jobs
    )

    data = get_data_from_minio(source_crawl=source_crawl, file_path=data_file_path)
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
    from scripts.utils.sender import (
        query_unposted_jobs,
        mark_jobs_as_posted,
        send_job_alerts
    )
    import os

    DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
    DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))

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