from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


def start_pipeline():
    print("Healthcare Data Lake Pipeline Started")


with DAG(
    dag_id="test_healthcare_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    start_task = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
    )