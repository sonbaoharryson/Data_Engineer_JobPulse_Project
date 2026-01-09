from airflow.decorators import dag, task
from datetime import datetime

PROJECT_DIR = '/opt/airflow/dbt/job_warehouse'
PROFILE_DIR = '/opt/airflow/dbt/job_warehouse'

@dag(
    dag_id='sample_dbt_dag',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['dbt_sample_dag']
)
def sample_dbt_call():

    @task.bash
    def check_dbt_conn():
        return f'dbt debug --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash
    def ingest_bronze():
        return f'dbt run --select bronze --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    @task.bash
    def ingest_silver():
        return f'dbt run --select silver --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    # @task.bash
    # def process_bronze_wh_layer():
    #     logger.info('Starting process data to bronze layer!!!')
    #     return f'dbt run --select bronze --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    # @task.bash
    # def process_silver_wh_layer():
    #     logger.info('Starting process data to silver layer!!!')
    #     return f'dbt run --select silver --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    # @task.bash
    # def process_gold_wh_layer():
    #     logger.info('Starting process data to gold layer!!!')
    #     return f'dbt run --select gold --project-dir {PROJECT_DIR} --profiles-dir {PROFILE_DIR}'

    check_dbt_conn() >> ingest_bronze() >> ingest_silver()

dag = sample_dbt_call()
