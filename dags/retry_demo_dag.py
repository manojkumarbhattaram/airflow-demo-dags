from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

def flaky_task():
    if random.random() < 0.5:
        raise Exception("Temporary failure, retrying...")
    print("Task succeeded!")

with DAG(
    dag_id="retry_demo_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={"retries": 1},
    tags=["demo", "retry"]
) as dag:
    
    retrying_task = PythonOperator(
        task_id="sometimes_fails",
        python_callable=flaky_task
    )
