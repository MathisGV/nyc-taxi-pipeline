from __future__ import annotations

import argparse
import os
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, dayofweek, from_unixtime, hour, lower, to_timestamp, when
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType


def build_schema() -> StructType:
    return StructType(
        [
            StructField("ingested_at", StringType(), True),
            StructField("source", StringType(), True),
            StructField("city", StringType(), True),
            StructField(
                "data",
                StructType(
                    [
                        StructField("dt", DoubleType(), True),
                        StructField(
                            "weather",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("main", StringType(), True),
                                        StructField("description", StringType(), True),
                                    ]
                                )
                            ),
                            True,
                        ),
                        StructField("main", StructType([StructField("temp", DoubleType(), True), StructField("humidity", DoubleType(), True)]), True),
                        StructField("wind", StructType([StructField("speed", DoubleType(), True)]), True),
                    ]
                ),
                True,
            ),
        ]
    )


def read_weather_stream(spark: SparkSession, input_path: str) -> DataFrame:
    schema = build_schema()
    raw_df = (
        spark.readStream.format("json")
        .schema(schema)
        .option("maxFilesPerTrigger", 1)
        .load(input_path)
    )

    return raw_df.select(
        col("ingested_at"),
        col("source"),
        col("city"),
        col("data.dt").alias("event_unix_ts"),
        col("data.main.temp").alias("temperature_c"),
        col("data.main.humidity").cast("int").alias("humidity_pct"),
        col("data.wind.speed").alias("wind_speed_ms"),
        col("data.weather")[0]["main"].alias("weather_main"),
        col("data.weather")[0]["description"].alias("weather_description"),
    )


def transform_weather_stream(raw_stream_df: DataFrame) -> DataFrame:
    transformed_df = raw_stream_df.withColumn(
        "event_ts", to_timestamp(from_unixtime(col("event_unix_ts")))
    ).withColumn(
        "weather_category",
        when(lower(col("weather_main")) == "clear", "Clair")
        .when(lower(col("weather_main")).isin("rain", "drizzle"), "Pluvieux")
        .when(lower(col("weather_main")).isin("thunderstorm"), "Orageux")
        .otherwise("Autre"),
    ).withColumn(
        "hour_of_day", hour(col("event_ts"))
    ).withColumn(
        "day_of_week", dayofweek(col("event_ts")) - 1
    )

    cleaned_df = transformed_df.filter(
        col("event_ts").isNotNull()
        & col("city").isNotNull()
        & col("temperature_c").isNotNull()
        & col("weather_main").isNotNull()
    )

    return cleaned_df.select(
        col("event_ts"),
        col("city"),
        col("temperature_c"),
        col("humidity_pct"),
        col("wind_speed_ms"),
        col("weather_main"),
        col("weather_description"),
        col("weather_category"),
        col("hour_of_day"),
        col("day_of_week"),
        col("source"),
        col("ingested_at"),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read weather JSON snapshots with Spark Structured Streaming."
    )
    parser.add_argument(
        "--input-path",
        default=os.getenv("WEATHER_STREAM_INPUT_PATH", "data_lake/weather/raw"),
        help="Path watched by Spark Structured Streaming.",
    )
    parser.add_argument(
        "--checkpoint-path",
        default=os.getenv(
            "WEATHER_STREAM_CHECKPOINT_PATH", "data_lake/weather/checkpoints/stream_weather"
        ),
        help="Checkpoint path for streaming state.",
    )
    parser.add_argument(
        "--trigger-once",
        action="store_true",
        help="Process available files once and stop.",
    )
    parser.add_argument(
        "--pg-host",
        default=os.getenv("PGHOST", "localhost"),
        help="PostgreSQL host.",
    )
    parser.add_argument(
        "--pg-port",
        type=int,
        default=int(os.getenv("PGPORT", "5432")),
        help="PostgreSQL port.",
    )
    parser.add_argument(
        "--pg-db",
        default=os.getenv("POSTGRES_DB", "postgres"),
        help="PostgreSQL database name.",
    )
    parser.add_argument(
        "--pg-user",
        default=os.getenv("POSTGRES_USER", "postgres"),
        help="PostgreSQL username.",
    )
    parser.add_argument(
        "--pg-password",
        default=os.getenv("POSTGRES_PASSWORD", ""),
        help="PostgreSQL password.",
    )
    parser.add_argument(
        "--pg-table",
        default=os.getenv("WEATHER_PG_TABLE", "dim_weather"),
        help="Target PostgreSQL table for weather data.",
    )
    return parser.parse_args()


def build_postgres_batch_writer(
    jdbc_url: str, pg_table: str, pg_user: str, pg_password: str
) -> Callable[[DataFrame, int], None]:
    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        batch_df.write.format("jdbc").mode("append").option("url", jdbc_url).option(
            "dbtable", pg_table
        ).option("user", pg_user).option("password", pg_password).option(
            "driver", "org.postgresql.Driver"
        ).save()
        print(f"batch {batch_id} written to {pg_table}")

    return _write_batch


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("weather_streaming_read").getOrCreate()

    stream_df = read_weather_stream(spark=spark, input_path=args.input_path)
    transformed_stream_df = transform_weather_stream(stream_df)
    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
    batch_writer = build_postgres_batch_writer(
        jdbc_url=jdbc_url,
        pg_table=args.pg_table,
        pg_user=args.pg_user,
        pg_password=args.pg_password,
    )

    writer = (
        transformed_stream_df.writeStream.foreachBatch(batch_writer)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_path)
    )
    if args.trigger_once:
        writer = writer.trigger(once=True)

    query = writer.start()
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
