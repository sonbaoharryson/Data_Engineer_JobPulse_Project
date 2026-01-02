from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    dag_id='sample_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['test']
)

def test_dag():
    @task
    def print_hello():
        print("Hello, SONBAO!")

    print_hello()

dag = test_dag()