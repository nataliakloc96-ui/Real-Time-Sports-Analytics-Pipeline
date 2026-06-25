from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, row_number
from pyspark.sql.window import Window

BRONZE_PATH = "./data/bronze/match_events"
SILVER_PATH = "./data/silver/match_events"

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("SportsSilverBatch")
        .master("local[*]")
        .getOrCreate()
    )

def transform_bronze_to_silver(spark: SparkSession):
    bronze_df = spark.read.parquet(BRONZE_PATH)

    print(f"-> Wczytano {bronze_df.count()} rekordów z Bronze layer")

    cleaned_df = bronze_df.filter(
        col("event_id").isNotNull() & col("match_id").isNotNull()
    )

    window_spec = Window.partitionBy("event_id").orderBy(col("kafka_offset").desc())
    deduped_df = (
        cleaned_df.withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    duplicates_removed = cleaned_df.count() - deduped_df.count()
    print(f"Usunięto {duplicates_removed} duplikatów")

    typed_df = deduped_df.withColumn(
        "match_timestamp", to_timestamp(col("utc_date"))
    ).withColumn(
        "match_date", to_date(col("utc_date"))
    )

    return typed_df

def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = transform_bronze_to_silver(spark)

    record_count = silver_df.count()
    print(f"-> Zapisuję {record_count} rekordów do Silver layer: {SILVER_PATH}")

    (
        silver_df.write.mode("overwrite")
        .partitionBy("match_date")
        .parquet(SILVER_PATH)
    )

    print("Silver layer zaktualizowany")
    spark.stop()

if __name__ == "__main__":
    main()