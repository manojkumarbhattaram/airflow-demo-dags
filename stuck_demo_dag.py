from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def long_running_task():
    print("Simulating long-running task...")
    time.sleep(300)  # Sleeps for 5 minutes

with DAG(
    dag_id="stuck_demo_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["demo", "stuck"]
) as dag:
    
    stuck_task = PythonOperator(
        task_id="long_sleep",
        python_callable=long_running_task
    )
