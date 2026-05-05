from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_weather_streaming",
    description="Ingestion et traitement streaming des données météo NYC",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "streaming", "ingestion"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_weather",
        bash_command=(
            "python /opt/airflow/scripts/ingestion/ingest_weather.py --once"
        ),
    )

    transform = BashOperator(
        task_id="transform_weather",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/airflow/scripts/processing/transform_weather.py "
            "--date {{ execution_date.strftime('%Y-%m-%d') }} "
            "--hour {{ execution_date.hour }}"
        ),
    )
