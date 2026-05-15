#!/usr/bin/env python3
"""
datalake_pipeline.py – Session 4 Lab: Three-Zone Data Lake Pipeline
====================================================================
Implements the Medallion Architecture (Bronze → Silver → Gold):

  Bronze / Raw Zone
    • Kafka → JSON files, partitioned by INGESTION TIME
    • Immutable, schema-free, forever retention
    • Path: C:/tmp/datalake/raw/source=kafka/topic=sensor-events/
             year=YYYY/month=MM/day=DD/hour=HH/

  Silver / Curated Zone
    • Parse JSON, validate, convert to Parquet (Snappy)
    • Partitioned by EVENT TIME + sensor_type
    • Path: C:/tmp/datalake/curated/domain=iot/
             sensor_type=<X>/year=YYYY/month=MM/day=DD/

  Gold / Consumption Zone  (built by consumption_zone.py)
    • Daily aggregated stats per sensor type
    • Written as a batch job from the curated zone

Usage:
    python python/datalake_pipeline.py
    # Ctrl+C to stop gracefully

In a second terminal, produce messages:
    python python/producer.py --count 500
"""

import os
import argparse

# Override HADOOP_HOME with a Windows-native path so the JVM can locate
# hadoop.dll at C:\hadoop\bin. The bash environment may have set it to
# /c/hadoop which the JVM on Windows cannot resolve.
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, expr,
    year, month, dayofmonth, hour, when,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
)

# ── Path configuration ────────────────────────────────────────────────────────
LAKE_ROOT    = "C:/tmp/datalake"
RAW_PATH     = f"{LAKE_ROOT}/raw/source=kafka/topic=sensor-events"
CURATED_PATH = f"{LAKE_ROOT}/curated/domain=iot"

CKPT_ROOT    = "C:/tmp/datalake-ckpt"
CKPT_RAW     = f"{CKPT_ROOT}/raw"
CKPT_CUR     = f"{CKPT_ROOT}/curated"

# ── Kafka configuration ───────────────────────────────────────────────────────
KAFKA_BROKERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC         = "sensor-events"

# ── Business rules ────────────────────────────────────────────────────────────
TEMP_THRESHOLD  = 35.0
HUM_THRESHOLD   = 90.0
VALUE_MIN       = -100.0    # range check: below this → corrupt data
VALUE_MAX       = 2000.0    # range check: above this → corrupt data
TRIGGER_SECS    = "30 seconds"

# ── Kafka payload schema ──────────────────────────────────────────────────────
#  Must match what producer.py writes.
SENSOR_SCHEMA = StructType([
    StructField("sensor",    StringType(),  False),
    StructField("value",     DoubleType(),  False),
    StructField("unit",      StringType(),  True),
    StructField("timestamp", LongType(),    False),
    StructField("device_id", StringType(),  True),
    StructField("source",    StringType(),  True),
])


# ── SparkSession ──────────────────────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Session4-DataLake")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ── Step 1 – Read raw Kafka stream ────────────────────────────────────────────
def read_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


# ── Step 2 – Build RAW ZONE stream (JSON, partitioned by ingestion time) ──────
def build_raw_stream(raw_kafka):
    """
    Preserve data exactly as received from Kafka.
    Partition by INGESTION TIME so the raw zone answers:
    'when did this data arrive in our system?'
    """
    return raw_kafka.select(
        col("value").cast("string").alias("raw_json"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("ingestion_ts"),
        # Partition columns derived from ingestion (not event) time
        year(col("timestamp")).alias("year"),
        month(col("timestamp")).alias("month"),
        dayofmonth(col("timestamp")).alias("day"),
        hour(col("timestamp")).alias("hour"),
    )


def write_raw_zone(raw_stream):
    """Write raw JSON files, partitioned year/month/day/hour."""
    os.makedirs(RAW_PATH, exist_ok=True)
    os.makedirs(CKPT_RAW, exist_ok=True)
    return (
        raw_stream
        .writeStream
        .outputMode("append")
        .format("json")
        .option("path", RAW_PATH)
        .option("checkpointLocation", CKPT_RAW)
        .partitionBy("year", "month", "day", "hour")
        .trigger(processingTime=TRIGGER_SECS)
        .start()
    )


# ── Step 3 – Build CURATED ZONE stream (Parquet, partitioned by event time) ───
def build_curated_stream(raw_kafka):
    """
    Parse JSON, validate, flag anomalies.
    Partition by EVENT TIME (not ingestion time), so business queries
    correctly answer 'what was the avg temperature on Jan 15?'
    """
    # Parse JSON payload
    parsed = (
        raw_kafka
        .select(
            from_json(
                col("value").cast("string"),
                SENSOR_SCHEMA,
            ).alias("d"),
            col("partition"),
            col("offset"),
        )
        .select(
            col("d.sensor").alias("sensor_type"),
            col("d.value"),
            col("d.unit"),
            col("d.device_id"),
            col("d.source"),
            to_timestamp(expr("d.timestamp / 1000")).alias("event_time"),
            col("partition"),
            col("offset"),
        )
    )

    # Data quality filters: drop nulls and out-of-range values
    clean = (
        parsed
        .filter(col("sensor_type").isNotNull())
        .filter(col("value").isNotNull())
        .filter(col("event_time").isNotNull())
        .filter(col("value").between(VALUE_MIN, VALUE_MAX))
    )

    # Flag anomalies (kept in dataset — not filtered out)
    flagged = clean.withColumn(
        "is_anomaly",
        when(
            ((col("sensor_type") == "temperature") & (col("value") > TEMP_THRESHOLD)),
            True,
        ).when(
            ((col("sensor_type") == "humidity") & (col("value") > HUM_THRESHOLD)),
            True,
        ).otherwise(False),
    )

    # Add partition columns from EVENT TIME
    return (
        flagged
        .withColumn("year",  year(col("event_time")))
        .withColumn("month", month(col("event_time")))
        .withColumn("day",   dayofmonth(col("event_time")))
    )


def write_curated_zone(curated_stream):
    """
    Write Parquet files with Snappy compression.
    sensor_type is the outermost partition → most queries filter on it first.
    """
    os.makedirs(CURATED_PATH, exist_ok=True)
    os.makedirs(CKPT_CUR, exist_ok=True)
    return (
        curated_stream
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", CURATED_PATH)
        .option("checkpointLocation", CKPT_CUR)
        .option("compression", "snappy")
        .partitionBy("sensor_type", "year", "month", "day")
        .trigger(processingTime=TRIGGER_SECS)
        .start()
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main(raw_zone: bool = True):
    print("=" * 65)
    print(" Session 4 – Three-Zone Data Lake Pipeline")
    print(f" Kafka topic : {TOPIC}")
    print(f" Raw zone    : {RAW_PATH}")
    print(f"              partitioned by year/month/day/hour (ingestion time)")
    print(f" Curated zone: {CURATED_PATH}")
    print(f"              partitioned by sensor_type/year/month/day (event time)")
    print(f" Trigger     : {TRIGGER_SECS}")
    print("=" * 65)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n▶  Spark {spark.version} started.")

    raw_kafka = read_kafka(spark)

    active_queries = []

    if raw_zone:
        print("▶  Starting Raw Zone writer (JSON)…")
        raw_stream   = build_raw_stream(raw_kafka)
        q_raw        = write_raw_zone(raw_stream)
        active_queries.append(("raw", q_raw))

    print("▶  Starting Curated Zone writer (Parquet / Snappy)…")
    curated_stream = build_curated_stream(raw_kafka)
    q_curated      = write_curated_zone(curated_stream)
    active_queries.append(("curated", q_curated))

    print(f"\n✅  {len(active_queries)} streaming quer{'y' if len(active_queries)==1 else 'ies'} running.")
    print(f"   Send messages : python python/producer.py --count 500")
    print(f"   After batches : python python/query_lake.py")
    print(f"   Build gold    : python python/consumption_zone.py")
    print(f"\n   Press Ctrl+C to stop gracefully.\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n⚡  Stopping all streaming queries…")
        for name, q in active_queries:
            q.stop()
            print(f"   ✔  {name} stopped.")
        spark.stop()
        print("✅  Pipeline stopped.")
    except Exception as e:
        print(f"\n❌  Streaming query failed: {e}")
        for name, q in active_queries:
            try:
                q.stop()
            except Exception:
                pass
        spark.stop()
        raise


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Session 4 data lake pipeline")
    p.add_argument(
        "--no-raw", action="store_true",
        help="Skip raw zone writing (curated zone only)"
    )
    args = p.parse_args()
    main(raw_zone=not args.no_raw)
