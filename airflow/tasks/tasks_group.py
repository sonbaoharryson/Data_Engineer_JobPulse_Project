from airflow.decorators import task, task_group
from tasks.process_tasks import load_crawl_sources_url, scrape_source_job, insert_jobs_to_staging_layer, post_job_to_discord, insert_company_logos_to_staging_layer, download_logos_and_upload_to_minio, update_company_logos_in_staging_layer
from tasks.audit_tasks import dbt_task_callback, discord_task_callback, task_failure_callback, task_success_callback
import logging

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

PROJECT_DIR = '/opt/airflow/dbt/job_warehouse'
PROFILE_DIR = '/opt/airflow/dbt/job_warehouse'

# task group for itviec pipeline
@task_group
def itviec_pipeline():
    @task(
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    def load_itviec_url():
        return load_crawl_sources_url(source_crawl="itviec")

    @task(
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    def scrape_itviec_job(sources: dict):
        return scrape_source_job(sources=sources, source_crawl="itviec")

    @task(
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    def insert_jobs_itviec(data):
        return insert_jobs_to_staging_layer(data_file_path=data['uploaded_file_path'], source_crawl="itviec")

    get_source_task = load_itviec_url()
    scrape_task = scrape_itviec_job(get_source_task)
    insert_staging_task = insert_jobs_itviec(scrape_task)
    
    return insert_staging_task

# task group for topcv pipeline
@task_group
def topcv_pipeline():
    @task(
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    def load_topcv_url():
        return load_crawl_sources_url(source_crawl="topcv")

    @task(
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    def scrape_topcv_job(sources: dict):
        return scrape_source_job(sources=sources, source_crawl="topcv")

    @task(
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    def insert_jobs_topcv(data):
        return insert_jobs_to_staging_layer(data_file_path=data['uploaded_file_path'], source_crawl="topcv")

    get_source_task = load_topcv_url()
    scrape_task = scrape_topcv_job(get_source_task)
    insert_staging_task = insert_jobs_topcv(scrape_task)
    
    return insert_staging_task

#task group for processing logos
@task_group
def process_company_logos_group():
    @task(
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    def insert_company_logos():
        return insert_company_logos_to_staging_layer()

    @task(
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    def download_and_upload_logos(data: list[dict]):
        return download_logos_and_upload_to_minio(data)
    
    @task(
        on_success_callback=task_success_callback,
        on_failure_callback=task_failure_callback
    )
    def update_company_logos(results: list[dict]):
        return update_company_logos_in_staging_layer(results)
    
    insert_logos_task = insert_company_logos()
    download_upload_task = download_and_upload_logos(insert_logos_task)
    update_logos_task = update_company_logos(download_upload_task)
    
    return {"insert_logos": insert_logos_task,
            "download_upload": download_upload_task,
            "update_logos": update_logos_task}

#task group for discord post
@task_group
def post_job_group():

    @task(
        on_success_callback=discord_task_callback,
        on_failure_callback=task_failure_callback
    )
    def post_job_to_discord_itviec():
        return post_job_to_discord(crawl_source="itviec")

    @task(
        on_success_callback=discord_task_callback,
        on_failure_callback=task_failure_callback
    )
    def post_job_to_discord_topcv():
        return post_job_to_discord(crawl_source="topcv")

    itviec_task = post_job_to_discord_itviec()
    topcv_task = post_job_to_discord_topcv()

    return {
        "itviec": itviec_task,
        "topcv": topcv_task
    }

# task group for dbt
@task_group
def dbt_wh_pipeline():
    @task.bash(
        on_success_callback=dbt_task_callback,
        on_failure_callback=task_failure_callback
    )
    def process_bronze_wh_layer():
        logger.info('Starting process data to bronze layer!!!')
        return f'dbt run --select bronze --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash(
        on_success_callback=dbt_task_callback,
        on_failure_callback=task_failure_callback
    )
    def bronze_wh_layer_test_models():
        logger.info('Starting process data to bronze layer!!!')
        return f'dbt test --select bronze --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash(
        on_success_callback=dbt_task_callback,
        on_failure_callback=task_failure_callback
    )
    def process_silver_wh_layer():
        logger.info('Starting process data to silver layer!!!')
        return f'dbt run --select silver --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash(
        on_success_callback=dbt_task_callback,
        on_failure_callback=task_failure_callback
    )
    def silver_wh_layer_test_models():
        logger.info('Starting process data to silver layer!!!')
        return f'dbt test --select silver --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash(
        on_success_callback=dbt_task_callback,
        on_failure_callback=task_failure_callback
    )
    def process_gold_wh_layer():
        logger.info('Starting process data to gold layer!!!')
        return f'dbt run --select gold --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash(
        on_success_callback=dbt_task_callback,
        on_failure_callback=task_failure_callback
    )
    def gold_wh_layer_test_models():
        logger.info('Starting process data to gold layer!!!')
        return f'dbt test --select gold --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash
    def process_audit_wh_layer():
        logger.info('Starting process data to audit layer!!!')
        return f'dbt run --select audit --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'
    
    bronze = process_bronze_wh_layer()
    test_bronze = bronze_wh_layer_test_models()
    silver = process_silver_wh_layer()
    test_silver = silver_wh_layer_test_models()
    gold = process_gold_wh_layer()
    test_gold = gold_wh_layer_test_models()
    audit = process_audit_wh_layer()
    bronze >> test_bronze >> silver >> test_silver >> gold >> test_gold >> audit
    
    return bronze