from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def fail_task():
    raise Exception("This task fails intentionally for demo purposes.")

with DAG(
    dag_id="failure_demo_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["demo", "failure"]
) as dag:
    
    fail = PythonOperator(
        task_id="fail_task",
        python_callable=fail_task
    )
