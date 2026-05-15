#!/usr/bin/env python3
"""
consumption_zone.py – Session 4: Build the Gold/Consumption Zone
=================================================================
Reads the curated Parquet zone (Silver) and computes:
  - Daily aggregates per sensor type (avg, min, max, anomaly count)
  - Hourly trends
  - Sensor comparison report

Writes results to the Consumption zone partitioned by sensor_type/year/month.

Usage:
    python python/consumption_zone.py
    python python/consumption_zone.py --sensor temperature
"""

import argparse
import os
import time

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min as spark_min, max as spark_max,
    count, sum as spark_sum, round as spark_round,
    when,
)

LAKE_ROOT    = "C:/tmp/datalake"
CURATED_PATH = f"{LAKE_ROOT}/curated/domain=iot"
CONSUME_PATH = f"{LAKE_ROOT}/consumption/use_case=sensor_averages"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Session4-ConsumptionZone")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def check_curated(path: str) -> bool:
    if not os.path.exists(path):
        print(f"\n  ⚠  Curated path not found: {path}")
        print(f"     Run datalake_pipeline.py first, then produce messages.")
        return False
    parquets = []
    for root, dirs, files in os.walk(path):
        parquets += [f for f in files if f.endswith(".parquet")]
    if not parquets:
        print(f"\n  ⚠  No Parquet files in {path}")
        print(f"     Wait for the pipeline to complete at least one batch (30 s).")
        return False
    print(f"  Found {len(parquets)} Parquet file(s) in curated zone.")
    return True


def main(sensor_filter: str | None = None):
    print("=" * 65)
    print(" Session 4 – Consumption Zone Builder")
    print(f" Source : {CURATED_PATH}")
    print(f" Output : {CONSUME_PATH}")
    if sensor_filter:
        print(f" Filter : sensor_type = '{sensor_filter}'")
    print("=" * 65)

    if not check_curated(CURATED_PATH):
        return

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # ── Read curated zone ─────────────────────────────────────
    print("\n▶  Loading curated Parquet data…")
    df = spark.read.parquet(CURATED_PATH)

    if sensor_filter:
        df = df.filter(col("sensor_type") == sensor_filter)

    total = df.count()
    print(f"   Total records : {total}")

    if total == 0:
        print("   No records found. Check your filter or produce more data.")
        spark.stop()
        return

    # ── Daily aggregates ──────────────────────────────────────
    print("\n▶  Computing daily aggregates (Gold layer)…")
    daily_agg = (
        df
        .groupBy("sensor_type", "year", "month", "day")
        .agg(
            count("value").alias("record_count"),
            spark_round(avg("value"), 3).alias("avg_value"),
            spark_round(spark_min("value"), 3).alias("min_value"),
            spark_round(spark_max("value"), 3).alias("max_value"),
            spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
            spark_round(
                100.0 * spark_sum(col("is_anomaly").cast("int")) / count("value"), 2
            ).alias("anomaly_pct"),
        )
        .orderBy("sensor_type", "year", "month", "day")
    )

    print("\n  Daily aggregates:")
    daily_agg.show(30, truncate=False)

    # ── Sensor comparison ─────────────────────────────────────
    print("▶  Sensor comparison summary:")
    comparison = (
        df
        .groupBy("sensor_type")
        .agg(
            count("value").alias("total_records"),
            spark_round(avg("value"), 2).alias("overall_avg"),
            spark_round(spark_min("value"), 2).alias("overall_min"),
            spark_round(spark_max("value"), 2).alias("overall_max"),
            spark_sum(col("is_anomaly").cast("int")).alias("total_anomalies"),
        )
        .orderBy("sensor_type")
    )
    comparison.show(truncate=False)

    # ── Write to consumption zone ─────────────────────────────
    print(f"▶  Writing consumption zone to {CONSUME_PATH}…")
    os.makedirs(CONSUME_PATH, exist_ok=True)
    (
        daily_agg
        .write
        .mode("overwrite")
        .partitionBy("sensor_type", "year", "month")
        .parquet(CONSUME_PATH)
    )
    print(f"✅  Consumption zone written.")

    # Verify the written files
    written = spark.read.parquet(CONSUME_PATH)
    print(f"\n  Rows in consumption zone : {written.count()}")
    written.orderBy("sensor_type", "year", "month", "day").show(30, truncate=False)

    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build Session 4 consumption zone")
    p.add_argument(
        "--sensor", default=None,
        help="Filter on a specific sensor type (e.g. temperature)"
    )
    args = p.parse_args()
    main(args.sensor)
