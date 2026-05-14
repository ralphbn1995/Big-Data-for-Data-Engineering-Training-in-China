#!/usr/bin/env python3
"""
read_output.py – Session 3: Read and Display Parquet Output
============================================================
Reads the Parquet files written by etl_pipeline.py and displays:
  - Schema
  - Aggregated windowed averages per sensor type
  - Anomaly counts per window
  - Raw flagged records (if --raw flag used)

Usage:
    python python/read_output.py
    python python/read_output.py --raw          # also show raw records
    python python/read_output.py --path C:/tmp/spark-etl/output
"""

import argparse
import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round

OUTPUT_PATH     = "C:/tmp/spark-etl/output"
RAW_OUTPUT_PATH = "C:/tmp/spark-etl/raw"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Session3-ReadOutput")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def read_and_display(spark: SparkSession, path: str, label: str):
    if not os.path.exists(path):
        print(f"\n  ⚠  Path not found: {path}")
        print(f"     Run the ETL pipeline first: python python/etl_pipeline.py")
        return

    # Check if there are any parquet files
    parquet_files = [
        f for f in os.listdir(path) if f.endswith(".parquet")
    ] if os.path.isdir(path) else []

    if not parquet_files:
        print(f"\n  ⚠  No Parquet files found in {path}")
        print(f"     The pipeline may still be processing its first batch.")
        print(f"     Wait ~15 seconds and retry.")
        return

    print(f"\n{'─' * 65}")
    print(f"  {label}")
    print(f"  Path: {path}")
    print(f"{'─' * 65}")

    df = spark.read.parquet(path)

    print(f"\n  Schema:")
    df.printSchema()

    print(f"\n  Row count: {df.count()}")

    if "window" in [f.name for f in df.schema.fields]:
        # Aggregated output
        print(f"\n  Windowed averages (ordered by window start, sensor):")
        (
            df.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("sensor"),
                spark_round(col("avg_value"), 2).alias("avg"),
                spark_round(col("min_value"), 2).alias("min"),
                spark_round(col("max_value"), 2).alias("max"),
                col("event_count"),
                col("anomaly_count"),
            )
            .orderBy("window_start", "sensor")
            .show(50, truncate=False)
        )
    else:
        # Raw records output
        print(f"\n  Raw records (last 20):")
        df.orderBy(col("kafka_timestamp").desc()).show(20, truncate=False)

        print(f"\n  Anomalies:")
        anomalies = df.filter(col("is_anomaly"))
        print(f"  Total anomalies: {anomalies.count()}")
        anomalies.select(
            "event_time", "sensor", "value", "unit", "device_id"
        ).orderBy("event_time").show(20, truncate=False)


def main(show_raw: bool, agg_path: str):
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 65)
    print(" Session 3 – Parquet Output Reader")
    print("=" * 65)

    read_and_display(spark, agg_path, "AGGREGATED WINDOWED OUTPUT")

    if show_raw:
        read_and_display(spark, RAW_OUTPUT_PATH, "RAW FLAGGED RECORDS")

    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Read Session 3 Parquet output")
    p.add_argument("--raw",  action="store_true",
                   help="Also show raw flagged records")
    p.add_argument("--path", default=OUTPUT_PATH,
                   help=f"Path to aggregated Parquet output (default: {OUTPUT_PATH})")
    args = p.parse_args()
    main(args.raw, args.path)
