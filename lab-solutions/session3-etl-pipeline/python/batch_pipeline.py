#!/usr/bin/env python3
"""
batch_pipeline.py – Session 3: Batch ETL (for comparison with Streaming)
=========================================================================
Reads ALL current messages from the sensor-events topic as a BOUNDED batch
(not a streaming query), applies the same transformations as etl_pipeline.py,
and writes the result to Parquet in one shot.

This demonstrates the ETL vs ELT conceptual differences and shows that
PySpark's DataFrame API is identical for batch and streaming jobs.

Usage:
    python python/batch_pipeline.py

Note: Uses spark.read (batch) instead of spark.readStream (streaming).
      The transformations are identical — only the source/sink API differs.
"""

import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, avg, count, max as spark_max, min as spark_min,
    window, to_timestamp, expr, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

KAFKA_BROKERS  = "localhost:9092,localhost:9094,localhost:9096"
TOPIC          = "sensor-events"
OUTPUT_PATH    = "C:/tmp/spark-etl/batch-output"
TEMP_THRESHOLD = 35.0
HUM_THRESHOLD  = 90.0

SENSOR_SCHEMA = StructType([
    StructField("sensor",    StringType(), nullable=False),
    StructField("value",     DoubleType(), nullable=False),
    StructField("unit",      StringType(), nullable=True),
    StructField("timestamp", LongType(),   nullable=False),
    StructField("device_id", StringType(), nullable=True),
])


def main():
    print("=" * 60)
    print(" Batch ETL Pipeline – Session 3")
    print(f" Source : Kafka topic '{TOPIC}' (reads ALL messages)")
    print(f" Output : {OUTPUT_PATH}")
    print("=" * 60)

    spark = (
        SparkSession.builder
        .appName("Session3-BatchETL")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── READ (batch, not streaming) ────────────────────────────
    print("\n▶  Reading all messages from Kafka (batch mode)…")
    raw_df = (
        spark.read               # ← batch API (vs readStream for streaming)
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets",   "latest")   # bounded: up to current end
        .load()
    )

    total_raw = raw_df.count()
    print(f"   Raw messages read: {total_raw}")

    if total_raw == 0:
        print("\n  ⚠  No messages found in topic.")
        print(f"     Run: python python/producer.py")
        spark.stop()
        return

    # ── PARSE ──────────────────────────────────────────────────
    print("▶  Parsing JSON payloads…")
    parsed_df = (
        raw_df
        .select(
            col("key").cast("string").alias("message_key"),
            from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data"),
            col("partition"),
            col("offset"),
        )
        .select(
            col("message_key"),
            col("data.sensor"),
            col("data.value"),
            col("data.unit"),
            col("data.device_id"),
            to_timestamp(expr("data.timestamp / 1000")).alias("event_time"),
            col("partition"),
            col("offset"),
        )
        .filter(col("sensor").isNotNull())
        .filter(col("value").isNotNull())
        .filter(col("event_time").isNotNull())
    )

    # ── FLAG ANOMALIES ─────────────────────────────────────────
    flagged_df = parsed_df.withColumn(
        "is_anomaly",
        when(
            ((col("sensor") == "temperature") & (col("value") > TEMP_THRESHOLD)), True
        ).when(
            ((col("sensor") == "humidity") & (col("value") > HUM_THRESHOLD)), True
        ).otherwise(False),
    )

    # ── AGGREGATE (5-minute tumbling windows) ─────────────────
    print("▶  Computing windowed aggregations…")
    agg_df = (
        flagged_df
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("sensor"),
        )
        .agg(
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("*").alias("event_count"),
            count(when(col("is_anomaly"), True)).alias("anomaly_count"),
        )
        .orderBy("window.start", "sensor")
    )

    # ── DISPLAY RESULTS ────────────────────────────────────────
    print("\n  Aggregated results:")
    agg_df.select(
        col("window.start").alias("window_start"),
        col("sensor"),
        col("avg_value"),
        col("min_value"),
        col("max_value"),
        col("event_count"),
        col("anomaly_count"),
    ).show(truncate=False)

    # Summary stats
    print("  Record breakdown per sensor:")
    flagged_df.groupBy("sensor").agg(
        count("*").alias("total"),
        count(when(col("is_anomaly"), True)).alias("anomalies"),
    ).orderBy("sensor").show()

    # ── WRITE ──────────────────────────────────────────────────
    print(f"▶  Writing Parquet output to {OUTPUT_PATH}…")
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    agg_df.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"✅  Batch ETL complete. Files written to: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()
