from __future__ import annotations

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_date

from match_event_schema import MATCH_EVENT_SCHEMA


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw_match_events")

BRONZE_OUTPUT_PATH = "./data/bronze/match_events"
CHECKPOINT_PATH = "./data/checkpoints/bronze_streaming"

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("SportsBronzeStreaming")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)

        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )

def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"-> Łączę się z Kafką: {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = raw_stream.select(
        col("key").cast("string").alias("kafka_key"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), MATCH_EVENT_SCHEMA).alias("data"),
    ).select(
        "data.*",
        "kafka_key",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )
    enriched_stream = parsed_stream.withColumn("ingestion_date", current_date())

    query = (
        enriched_stream.writeStream.format("parquet")
        .option("path", BRONZE_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ingestion_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print(f" Streaming uruchomiony. Bronze layer zapisywany do: {BRONZE_OUTPUT_PATH}")
    print(" Naciśnij Ctrl+C aby zatrzymać.")

    query.awaitTermination()

if __name__ == "__main__":
    main()