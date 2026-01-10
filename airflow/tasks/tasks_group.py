from airflow.decorators import task, task_group
from tasks.process_tasks import load_crawl_sources_url, scrape_source_job, insert_jobs_to_staging_layer, post_job_to_discord

@task_group
def itviec_pipeline():
    @task
    def load_itviec_url():
        return load_crawl_sources_url(source_crawl="itviec")

    @task
    def scrape_itviec_job(sources: dict):
        return scrape_source_job(sources=sources, source_crawl="itviec")

    @task
    def insert_jobs_itviec(data):
        return insert_jobs_to_staging_layer(data=data['rows_scraped'], source_crawl="itviec")

    @task
    def post_job_to_discord_itviec():
        return post_job_to_discord(crawl_source="itviec")

    source_crawl = load_itviec_url()
    scrapped_data = scrape_itviec_job(source_crawl)
    insert_data = insert_jobs_itviec(scrapped_data)
    # discord_post_job = post_job_to_discord_itviec()
    # insert_data >> discord_post_job

@task_group
def topcv_pipeline():
    @task
    def load_topcv_url():
        return load_crawl_sources_url(source_crawl="topcv")

    @task
    def scrape_topcv_job(sources: dict):
        return scrape_source_job(sources=sources, source_crawl="topcv")

    @task
    def insert_jobs_topcv(data):
        return insert_jobs_to_staging_layer(data=data['rows_scraped'], source_crawl="topcv")

    @task
    def post_job_to_discord_topcv():
        return post_job_to_discord(crawl_source="topcv")

    source_crawl = load_topcv_url()
    scrapped_data = scrape_topcv_job(source_crawl)
    insert_data = insert_jobs_topcv(scrapped_data)
    # discord_post_job = post_job_to_discord_topcv()
    # insert_data >> discord_post_job