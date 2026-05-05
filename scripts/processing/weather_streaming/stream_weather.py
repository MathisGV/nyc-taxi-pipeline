from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("weather_streaming_read").getOrCreate()

    stream_df = read_weather_stream(spark=spark, input_path=args.input_path)

    writer = (
        stream_df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("checkpointLocation", args.checkpoint_path)
    )
    if args.trigger_once:
        writer = writer.trigger(once=True)

    query = writer.start()
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
