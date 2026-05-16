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
    catchup=False,
    max_active_runs=1,
    tags=["taxi", "batch", "ingestion"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_taxi",
        bash_command=(
            "python /opt/airflow/scripts/ingestion/ingest_taxi.py "
            "--start-year 2025 "
            "--start-month 1 "
            "--end-year 2025 "
            "--end-month 1"
        ),
    )

    transform = BashOperator(
        task_id="transform_taxi",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.6.0 "
            "--master local[*] "
            "/opt/spark/scripts/processing/transform_taxi.py "
            "--year 2025 "
            "--month 1"
        ),
    )

    ingest >> transform
