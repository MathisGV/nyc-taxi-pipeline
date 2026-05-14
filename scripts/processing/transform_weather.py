import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    dayofweek,
    from_unixtime,
    hour,
    lit,
    lower,
    to_timestamp,
    when,
)


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("transform_weather")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                    "org.postgresql:postgresql:42.7.4",
                ]
            ),
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    return spark


def read_weather_batch(spark: SparkSession, date_value: str, hour_value: int) -> DataFrame:
    input_path = f"s3a://weather-raw/nyc/{date_value.replace('-', '/')}/weather_*.json"
    df = spark.read.option("multiLine", "true").json(input_path)
    ingested_at_col = (
        to_timestamp(col("_ingested_at"))
        if "_ingested_at" in df.columns
        else lit(None).cast("timestamp")
    )
    ingested_at_alt_col = (
        to_timestamp(col("ingested_at"))
        if "ingested_at" in df.columns
        else lit(None).cast("timestamp")
    )
    dt_col = (
        to_timestamp(from_unixtime(col("dt")))
        if "dt" in df.columns
        else lit(None).cast("timestamp")
    )
    event_ts = coalesce(
        ingested_at_col,
        ingested_at_alt_col,
        dt_col,
    )
    return df.withColumn("_event_ts", event_ts)


def transform_weather(df: DataFrame) -> DataFrame:
    ingested_at_col = (
        to_timestamp(col("_ingested_at"))
        if "_ingested_at" in df.columns
        else lit(None).cast("timestamp")
    )
    ingested_at_alt_col = (
        to_timestamp(col("ingested_at"))
        if "ingested_at" in df.columns
        else lit(None).cast("timestamp")
    )
    dt_col = (
        to_timestamp(from_unixtime(col("dt")))
        if "dt" in df.columns
        else lit(None).cast("timestamp")
    )
    event_ts = coalesce(
        col("_event_ts"),
        ingested_at_col,
        ingested_at_alt_col,
        dt_col,
    )

    transformed = (
        df.withColumn("recorded_at", event_ts)
        .withColumn("temperature", col("main.temp").cast("double"))
        .withColumn("feels_like", col("main.feels_like").cast("double"))
        .withColumn("humidity", col("main.humidity").cast("int"))
        .withColumn("wind_speed", col("wind.speed").cast("double"))
        .withColumn("wind_deg", col("wind.deg").cast("int"))
        .withColumn("weather_main", col("weather")[0]["main"])
        .withColumn("weather_description", col("weather")[0]["description"])
        .withColumn(
            "weather_category",
            when(lower(col("weather")[0]["main"]) == "clear", lit("Clear"))
            .when(lower(col("weather")[0]["main"]).isin("rain", "drizzle"), lit("Rainy"))
            .when(lower(col("weather")[0]["main"]).isin("thunderstorm", "snow"), lit("Stormy"))
            .otherwise(lit("Other")),
        )
        .withColumn("pickup_hour", hour(col("recorded_at")))
        .withColumn("day_of_week", dayofweek(col("recorded_at")) - 1)
    )

    return transformed.select(
        col("recorded_at"),
        col("temperature"),
        col("feels_like"),
        col("humidity"),
        col("wind_speed"),
        col("wind_deg"),
        col("weather_main"),
        col("weather_description"),
        col("weather_category"),
        col("pickup_hour"),
        col("day_of_week"),
        coalesce(ingested_at_col, ingested_at_alt_col, col("recorded_at")).alias(
            "ingested_at"
        ),
    )


def write_to_postgres(df: DataFrame) -> None:
    df.write.format("jdbc").mode("append").option(
        "url",
        f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', os.getenv('PGHOST', 'postgres'))}:{os.getenv('PGPORT', '5432')}/{os.getenv('POSTGRES_DB')}",
    ).option("dbtable", "raw.dim_weather").option("user", os.getenv("POSTGRES_USER")).option(
        "password", os.getenv("POSTGRES_PASSWORD")
    ).option("driver", "org.postgresql.Driver").save()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform weather data and load to PostgreSQL")
    parser.add_argument("--date", required=True, help="Date in format YYYY-MM-DD")
    parser.add_argument("--hour", required=True, type=int, help="Hour in range [0, 23]")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.hour < 0 or args.hour > 23:
        raise SystemExit("--hour must be between 0 and 23")

    spark = build_spark_session()
    try:
        raw_df = read_weather_batch(spark, args.date, args.hour)
        transformed_df = transform_weather(raw_df)
        write_to_postgres(transformed_df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
