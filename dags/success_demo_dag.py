from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def greet():
    print("👋 Hello from the success_demo_dag!")

def confirm():
    print("✅ All steps completed successfully.")

with DAG(
    dag_id="success_demo_dag",
    start_date=datetime(2025, 6, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["demo", "success"],
    default_args={
        "owner": "Manoj",
        "retries": 1
    }
) as dag:
    task_1 = PythonOperator(
        task_id="greet_task",
        python_callable=greet
    )

    task_2 = PythonOperator(
        task_id="confirm_success_task",
        python_callable=confirm
    )

    task_1 >> task_2  # Set dependency
