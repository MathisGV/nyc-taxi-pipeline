import os
import sys
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[2]))
from common import get_logger

load_dotenv()

logger = get_logger(__name__)

MINIO_BUCKET = "weather-raw"


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("transform_weather")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, date: str, hour: int):
    year, month, day = date.split("-")
    path = f"s3a://{MINIO_BUCKET}/nyc/{year}/{month}/{day}/weather_{year}{month}{day}_{hour:02d}*.json"
    logger.info(f"Reading weather data from {path}")
    return spark.read.option("multiline", "true").json(path)


def transform(df):
    return (
        df
        .filter(F.col("main.temp").isNotNull())
        .withColumn("recorded_at", F.to_timestamp(F.col("_ingested_at")))
        .withColumn("temperature", F.col("main.temp").cast("float"))
        .withColumn("feels_like", F.col("main.feels_like").cast("float"))
        .withColumn("humidity", F.col("main.humidity").cast("integer"))
        .withColumn("wind_speed", F.col("wind.speed").cast("float"))
        .withColumn("wind_deg", F.col("wind.deg").cast("integer"))
        .withColumn("weather_main", F.col("weather")[0]["main"])
        .withColumn("weather_description", F.col("weather")[0]["description"])
        .withColumn(
            "weather_category",
            F.when(F.col("weather_main").isin("Rain", "Drizzle"), "Rainy")
             .when(F.col("weather_main").isin("Thunderstorm", "Snow"), "Stormy")
             .otherwise("Clear")
        )
        .withColumn("pickup_hour", F.hour("recorded_at"))
        .withColumn("day_of_week", F.dayofweek("recorded_at"))
        .withColumn("ingested_at", F.current_timestamp())
        .select(
            "recorded_at",
            "temperature",
            "feels_like",
            "humidity",
            "wind_speed",
            "wind_deg",
            "weather_main",
            "weather_description",
            "weather_category",
            "pickup_hour",
            "day_of_week",
            "ingested_at"
        )
    )


def write_to_postgres(df) -> None:
    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:5432"
        f"/{os.getenv('POSTGRES_DB')}"
    )
    props = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    logger.info("Writing to PostgreSQL raw.dim_weather")
    (
        df.write
        .jdbc(
            url=jdbc_url,
            table="raw.dim_weather",
            mode="append",
            properties=props
        )
    )
    logger.info("Write complete")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform NYC weather data")
    parser.add_argument("--date", type=str, required=True, help="Date format: YYYY-MM-DD")
    parser.add_argument("--hour", type=int, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session()

    try:
        df_raw = read_raw(spark, args.date, args.hour)
        df_transformed = transform(df_raw)
        write_to_postgres(df_transformed)
    finally:
        spark.stop()