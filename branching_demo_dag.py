from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import random

def choose_branch():
    return "path_a" if random.choice([True, False]) else "path_b"

with DAG(
    dag_id="branching_demo_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["demo", "branch"]
) as dag:
    
    branch = BranchPythonOperator(
        task_id="choose_path",
        python_callable=choose_branch
    )

    path_a = DummyOperator(task_id="path_a")
    path_b = DummyOperator(task_id="path_b")
    join = DummyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

    branch >> [path_a, path_b] >> join
