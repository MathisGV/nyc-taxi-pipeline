from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_taxi_batch",
    description="Ingestion et transformation des données Yellow Taxi NYC",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 0 1 * *",
    catchup=True,
    max_active_runs=1,
    tags=["taxi", "batch", "ingestion"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_taxi",
        bash_command=(
            "python /opt/airflow/scripts/ingestion/ingest_taxi.py "
            "--start-year {{ execution_date.year }} "
            "--start-month {{ execution_date.month }} "
            "--end-year {{ execution_date.year }} "
            "--end-month {{ execution_date.month }}"
        ),
    )

    transform = BashOperator(
        task_id="transform_taxi",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/scripts/processing/transform_taxi.py "
            "--year {{ execution_date.year }} "
            "--month {{ execution_date.month }}"
        ),
    )

    ingest >> transform
