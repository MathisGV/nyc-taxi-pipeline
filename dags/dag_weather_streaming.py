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
    description="Ingestion et transformation meteo vers PostgreSQL",
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
            "python /opt/airflow/scripts/ingestion/ingest_weather.py --once"
        ),
    )

    stream_weather = BashOperator(
        task_id="transform_weather",
        bash_command=(
            "spark-submit "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.4 "
            "--master spark://spark-master:7077 "
            "/opt/airflow/scripts/processing/transform_weather.py "
            "--date {{ execution_date.strftime('%Y-%m-%d') }} "
            "--hour {{ execution_date.hour }}"
        ),
    )

    ingest_weather >> stream_weather
