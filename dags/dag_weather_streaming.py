from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_weather_streaming",
    description="Orchestrate weather ingestion and streaming transform",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "streaming"],
) as dag:
    ingest_weather = BashOperator(
        task_id="ingest_weather_snapshot",
        bash_command=(
            "PYTHONPATH=/opt/airflow "
            "python -m scripts.ingestion.weather.fetch_weather --once"
        ),
    )

    stream_weather = BashOperator(
        task_id="run_weather_streaming_job",
        bash_command=(
            "PYTHONPATH=/opt/airflow "
            "python -m scripts.processing.weather_streaming.stream_weather "
            "--trigger-once"
        ),
    )

    ingest_weather >> stream_weather
