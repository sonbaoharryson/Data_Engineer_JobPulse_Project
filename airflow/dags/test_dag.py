from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    dag_id='test_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['test']
)

def test_dag():
    @task
    def print_hello():
        print("Hello, GIADINH!")

    @task
    def print_bao():
        print("Hello, Giang!")
    print_hello() >> print_bao()

dag = test_dag()