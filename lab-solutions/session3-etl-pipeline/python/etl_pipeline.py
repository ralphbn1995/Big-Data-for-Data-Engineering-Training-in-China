#!/usr/bin/env python3
"""
etl_pipeline.py – Session 3 Lab: Kafka + Spark Structured Streaming ETL
========================================================================
Full pipeline:
  1. Read raw sensor events from Kafka topic `sensor-events`
  2. Parse binary JSON payload into a typed Spark schema
  3. Filter out malformed records
  4. Flag anomalies (temperature > 35°C, humidity > 90%)
  5. Compute 5-minute windowed averages per sensor type (with watermark)
  6. Write aggregated results to Parquet sink (checkpoint-aware)
  7. Optionally also write raw flagged records to a second Parquet sink

Usage:
    # Option A – via spark-submit (recommended)
    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\
      python/etl_pipeline.py

    # Option B – run directly (SparkSession has jars.packages embedded)
    python python/etl_pipeline.py

Prerequisites:
    pip install pyspark==3.5.3 kafka-python
    Docker Kafka cluster running (docker compose up -d)
    sensor-events topic populated (python python/producer.py)
"""

import os
import argparse

# Override HADOOP_HOME to a Windows-native path so the JVM loads hadoop.dll.
# Bash may have set it to /c/hadoop which the JVM cannot resolve on Windows.
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, avg, count, max as spark_max, min as spark_min,
    window, to_timestamp, expr, when
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType
)

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BROKERS   = "localhost:9092,localhost:9094,localhost:9096"
TOPIC           = "sensor-events"

OUTPUT_PATH          = "C:/tmp/spark-etl/output"
CHECKPOINT_PATH      = "C:/tmp/spark-etl/checkpoint"
RAW_OUTPUT_PATH      = "C:/tmp/spark-etl/raw"
RAW_CHECKPOINT_PATH  = "C:/tmp/spark-etl/checkpoint-raw"

TEMP_THRESHOLD   = 35.0    # °C
HUM_THRESHOLD    = 90.0    # %
TRIGGER_INTERVAL = "10 seconds"
WATERMARK_DELAY  = "2 minutes"
WINDOW_DURATION  = "5 minutes"

# ── Schema of each JSON message (must match producer.py) ─────────────────────
#  { "sensor": "temperature", "value": 28.5, "unit": "C",
#    "timestamp": 1700000000000, "device_id": "temp-01" }
SENSOR_SCHEMA = StructType([
    StructField("sensor",    StringType(),  nullable=False),
    StructField("value",     DoubleType(),  nullable=False),
    StructField("unit",      StringType(),  nullable=True),
    StructField("timestamp", LongType(),    nullable=False),
    StructField("device_id", StringType(),  nullable=True),
])


# ── Spark Session ─────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    """Create a local SparkSession with the Kafka connector package."""
    return (
        SparkSession.builder
        .appName("Session3-ETL-Pipeline")
        .master("local[*]")
        # Download Kafka connector JAR automatically from Maven Central
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        # Keep shuffle partitions small for a local 3-partition topic
        .config("spark.sql.shuffle.partitions", "3")
        # Finish current micro-batch before stopping on SIGTERM
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Reduce verbose Spark logging
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ── Step 1 – Read raw Kafka stream ───────────────────────────────────────────
def read_kafka(spark: SparkSession):
    """
    Returns a streaming DataFrame with Kafka's default schema:
      key        binary
      value      binary   ← our JSON payload, must be parsed
      topic      string
      partition  int
      offset     long
      timestamp  timestamp
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        # On first start: read from offset 0.
        # On restart with checkpoint: this is ignored (resumes from checkpoint).
        .option("startingOffsets", "earliest")
        # Don't crash if Kafka deleted old segments (e.g., retention expired)
        .option("failOnDataLoss", "false")
        .load()
    )


# ── Step 2 – Parse JSON payload ───────────────────────────────────────────────
def parse_json(raw_df):
    """
    Cast binary value → string → JSON struct, then flatten.
    Two-step select pattern keeps the code readable.
    """
    # Step 2a: cast binary → string, then parse JSON
    with_struct = raw_df.select(
        col("key").cast("string").alias("message_key"),
        from_json(
            col("value").cast("string"),
            SENSOR_SCHEMA,
        ).alias("data"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    # Step 2b: flatten the nested struct into top-level columns
    parsed = with_struct.select(
        col("message_key"),
        col("data.sensor"),
        col("data.value"),
        col("data.unit"),
        col("data.device_id"),
        # Producer stores epoch ms; to_timestamp() expects seconds
        to_timestamp(expr("data.timestamp / 1000")).alias("event_time"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
    )

    return parsed


# ── Step 3 – Filter malformed records ────────────────────────────────────────
def filter_clean(parsed_df):
    """
    Drop rows where JSON parsing failed (null sensor or value).
    In production these would go to a dead-letter Kafka topic.
    """
    return (
        parsed_df
        .filter(col("sensor").isNotNull())
        .filter(col("value").isNotNull())
        .filter(col("event_time").isNotNull())
    )


# ── Step 4 – Flag anomalies ───────────────────────────────────────────────────
def flag_anomalies(clean_df):
    """
    Add a boolean `is_anomaly` column based on sensor-type thresholds.
    No data is filtered out here — anomalies are flagged and kept.
    """
    return clean_df.withColumn(
        "is_anomaly",
        when(
            ((col("sensor") == "temperature") & (col("value") > TEMP_THRESHOLD)),
            True,
        ).when(
            ((col("sensor") == "humidity") & (col("value") > HUM_THRESHOLD)),
            True,
        ).otherwise(False),
    )


# ── Step 5 – Windowed aggregation ────────────────────────────────────────────
def windowed_aggregation(flagged_df):
    """
    Group events into 5-minute tumbling windows per sensor type.
    withWatermark allows Spark to safely expire closed windows.

    Result schema:
      window   struct<start: timestamp, end: timestamp>
      sensor   string
      avg_value double
      min_value double
      max_value double
      count     long
      anomalies long
    """
    return (
        flagged_df
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), WINDOW_DURATION),
            col("sensor"),
        )
        .agg(
            avg("value").alias("avg_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("*").alias("event_count"),
            count(when(col("is_anomaly"), True)).alias("anomaly_count"),
        )
    )


# ── Step 6a – Write aggregated stream to Parquet ─────────────────────────────
def write_aggregated(windowed_df):
    """
    Sink: Parquet files under OUTPUT_PATH.
    outputMode("update"): only rows that changed since last trigger are written.
    """
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)

    return (
        windowed_df
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# ── Step 6b – Write raw flagged stream to Parquet (optional) ─────────────────
def write_raw(flagged_df):
    """
    Dual-write pattern: also persist the raw (un-aggregated) records
    with the anomaly flag, for audit / replay purposes.
    outputMode("append"): each record is written exactly once.
    """
    os.makedirs(RAW_OUTPUT_PATH, exist_ok=True)
    os.makedirs(RAW_CHECKPOINT_PATH, exist_ok=True)

    return (
        flagged_df
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", RAW_OUTPUT_PATH)
        .option("checkpointLocation", RAW_CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main(write_raw_stream: bool = False):
    print("=" * 65)
    print(" Session 3 – Kafka + Spark Structured Streaming ETL")
    print(f" Source  : Kafka topic '{TOPIC}'")
    print(f" Sink    : Parquet → {OUTPUT_PATH}")
    print(f" Trigger : {TRIGGER_INTERVAL}")
    print(f" Window  : {WINDOW_DURATION}  |  Watermark: {WATERMARK_DELAY}")
    print("=" * 65)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("\n▶  Spark session started.")
    print(f"   Spark version : {spark.version}")
    print(f"   Master        : {spark.conf.get('spark.master')}")

    # ── Build the pipeline DAG (lazy – nothing executes yet) ──────────────
    raw_df     = read_kafka(spark)
    parsed_df  = parse_json(raw_df)
    clean_df   = filter_clean(parsed_df)
    flagged_df = flag_anomalies(clean_df)
    agg_df     = windowed_aggregation(flagged_df)

    # ── Start streaming queries (actions – DAG now executes) ──────────────
    print("\n▶  Starting streaming query (aggregated → Parquet)…")
    query_agg = write_aggregated(agg_df)

    if write_raw_stream:
        print("▶  Starting streaming query (raw flagged → Parquet)…")
        query_raw = write_raw(flagged_df)

    print(f"\n✅  Pipeline running. Batches fire every {TRIGGER_INTERVAL}.")
    print(f"   Aggregated output : {OUTPUT_PATH}")
    if write_raw_stream:
        print(f"   Raw output        : {RAW_OUTPUT_PATH}")
    print(f"\n   Produce messages : python python/producer.py")
    print(f"   Read output      : python python/read_output.py")
    print(f"\n   Press Ctrl+C to stop gracefully.\n")

    # Block until any query terminates or Ctrl+C.
    # awaitAnyTermination() covers both queries when --raw is used.
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n⚡  Stopping pipeline…")
        query_agg.stop()
        if write_raw_stream:
            query_raw.stop()
        spark.stop()
        print("✅  Pipeline stopped.")
    except Exception as e:
        print(f"\n❌  Streaming query failed: {e}")
        query_agg.stop()
        if write_raw_stream:
            try:
                query_raw.stop()
            except Exception:
                pass
        spark.stop()
        raise


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Session 3 ETL pipeline")
    parser.add_argument(
        "--raw", action="store_true",
        help="Also write raw flagged records to a second Parquet sink"
    )
    args = parser.parse_args()
    main(write_raw_stream=args.raw)
