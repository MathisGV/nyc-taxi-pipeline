import os
import sys
import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common import get_logger


load_dotenv()

logger = get_logger(__name__)

MINIO_BUCKET = "taxi-raw"
PAYMENT_TYPE_MAP = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("transform_taxi")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_SPARK_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, year: int, month: int):
    path = f"s3a://{MINIO_BUCKET}/yellow_taxi/{year}/{month:02d}/"
    logger.info(f"Reading raw data from {path}")
    return spark.read.parquet(path)


def transform(df):
    payment_map = F.create_map(
        *[item for pair in [(F.lit(k), F.lit(v)) for k, v in PAYMENT_TYPE_MAP.items()] for item in pair]
    )

    if "cbd_congestion_fee" not in df.columns:
        df = df.withColumn("cbd_congestion_fee", F.lit(0.0))

    return (
        df
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumnRenamed("DOLocationID", "do_location_id")

        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("trip_distance") > 0)
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
        )
        .filter(F.col("trip_duration_minutes") > 0)
        .filter(F.col("trip_duration_minutes") < 300)
        .withColumn(
            "distance_bucket",
            F.when(F.col("trip_distance") <= 2, "0-2km")
             .when(F.col("trip_distance") <= 5, "2-5km")
             .otherwise(">5km")
        )
        .withColumn(
            "payment_label",
            payment_map[F.col("payment_type")]
        )
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("fare_amount") > 0,
                (F.col("tip_amount") / F.col("fare_amount") * 100).cast(FloatType())
            ).otherwise(F.lit(0.0))
        )
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("ingested_at", F.current_timestamp())
        
        .select(
            "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "rate_code_id", "store_and_fwd_flag",
            "pu_location_id", "do_location_id", "payment_type", "fare_amount",
            "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
            "total_amount", "congestion_surcharge", "cbd_congestion_fee",
            "trip_duration_minutes", "distance_bucket", "payment_label",
            "tip_percentage", "pickup_hour", "pickup_day_of_week", "ingested_at"
        )
    )


def write_to_postgres(df, year: int, month: int) -> None:
    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:5432"
        f"/{os.getenv('POSTGRES_DB')}"
    )
    props = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }

    logger.info(f"Writing to PostgreSQL raw.fact_taxi_trips ({year}-{month:02d})")
    (
        df.write
        .jdbc(
            url=jdbc_url,
            table="raw.fact_taxi_trips",
            mode="append",
            properties=props
        )
    )
    logger.info("Write complete")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform NYC Yellow Taxi data")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session()

    try:
        df_raw = read_raw(spark, args.year, args.month)
        df_transformed = transform(df_raw)
        write_to_postgres(df_transformed, args.year, args.month)
    finally:
        spark.stop()