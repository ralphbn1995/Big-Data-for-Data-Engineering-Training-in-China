#!/usr/bin/env python3
"""
query_lake.py – Session 4 Lab: Spark SQL + Partition Pruning
=============================================================
Runs the five SQL queries from the lab and measures the pruning benefit.

Steps:
  1. Register curated & consumption zones as Spark SQL temp views
  2. Query 1 – Basic aggregation with partition pruning
  3. Query 2 – Anomaly analysis across all sensors
  4. Query 3 – Time-series: hourly trend for one sensor
  5. Query 4 – Cross-sensor comparison
  6. Measure full-scan vs pruned-scan timing

Usage:
    python python/query_lake.py
    python python/query_lake.py --sensor humidity   # change target sensor
"""

import argparse
import os
import time

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

LAKE_ROOT    = "C:/tmp/datalake"
CURATED_PATH = f"{LAKE_ROOT}/curated/domain=iot"
CONSUME_PATH = f"{LAKE_ROOT}/consumption/use_case=sensor_averages"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Session4-QueryLake")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def path_ok(path: str, label: str) -> bool:
    parquets = []
    for root, _, files in os.walk(path):
        parquets += [f for f in files if f.endswith(".parquet")]
    if not parquets:
        print(f"\n  ⚠  No Parquet files in {label} ({path})")
        return False
    print(f"  {label}: {len(parquets)} file(s)")
    return True


def sep(title: str = ""):
    print("\n" + "─" * 65)
    if title:
        print(f"  {title}")
        print("─" * 65)


def run_timed(spark: SparkSession, sql: str, label: str) -> float:
    """Run a SQL query, print the result, return elapsed time in seconds."""
    sep(label)
    start = time.time()
    result = spark.sql(sql)
    result.show(30, truncate=False)
    elapsed = time.time() - start
    print(f"  ⏱  {elapsed:.3f} s")
    return elapsed


VALID_SENSORS = {"temperature", "humidity", "pressure"}


def main(target_sensor: str = "temperature"):
    if target_sensor not in VALID_SENSORS:
        raise ValueError(f"Unknown sensor '{target_sensor}'. Must be one of: {VALID_SENSORS}")

    print("=" * 65)
    print(" Session 4 – Spark SQL Queries + Partition Pruning Demo")
    print(f" Curated : {CURATED_PATH}")
    print(f" Gold    : {CONSUME_PATH}")
    print(f" Target sensor for pruning demo: {target_sensor}")
    print("=" * 65)

    if not (path_ok(CURATED_PATH, "Curated zone")):
        print("\n  Run datalake_pipeline.py then produce messages first.")
        return

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # ── Register temp views ───────────────────────────────────
    print("\n▶  Registering Spark SQL temp views…")
    curated_df = spark.read.parquet(CURATED_PATH)
    curated_df.createOrReplaceTempView("sensor_curated")

    print("  sensor_curated schema:")
    curated_df.printSchema()

    total = curated_df.count()
    print(f"  Total records in curated zone: {total}")

    if path_ok(CONSUME_PATH, "Consumption zone"):
        consume_df = spark.read.parquet(CONSUME_PATH)
        consume_df.createOrReplaceTempView("sensor_daily")

    # ── Query 1: Partition pruning on sensor_type + date ─────
    run_timed(spark, f"""
        SELECT
            sensor_type,
            day,
            ROUND(AVG(value), 2)   AS avg_value,
            ROUND(MIN(value), 2)   AS min_value,
            ROUND(MAX(value), 2)   AS max_value,
            COUNT(*)               AS total_records,
            SUM(CAST(is_anomaly AS INT)) AS anomalies
        FROM sensor_curated
        WHERE sensor_type = '{target_sensor}'
        GROUP BY sensor_type, day
        ORDER BY day
    """, f"QUERY 1 – Daily stats for '{target_sensor}' (partition pruned)")

    # ── Query 2: Anomaly % across all sensors ────────────────
    run_timed(spark, """
        SELECT
            sensor_type,
            COUNT(*)                                              AS total,
            SUM(CAST(is_anomaly AS INT))                          AS anomalies,
            ROUND(
                100.0 * SUM(CAST(is_anomaly AS INT)) / COUNT(*), 2
            )                                                     AS anomaly_pct
        FROM sensor_curated
        GROUP BY sensor_type
        ORDER BY anomaly_pct DESC
    """, "QUERY 2 – Anomaly percentage per sensor type (full scan)")

    # ── Query 3: Hourly trend (event_time bucketed by hour) ───
    # Note: event_time is a timestamp; we extract the hour for grouping
    run_timed(spark, f"""
        SELECT
            HOUR(event_time)       AS hour_of_day,
            ROUND(AVG(value), 2)   AS avg_value,
            COUNT(*)               AS readings
        FROM sensor_curated
        WHERE sensor_type = '{target_sensor}'
        GROUP BY HOUR(event_time)
        ORDER BY hour_of_day
    """, f"QUERY 3 – Hourly trend for '{target_sensor}'")

    # ── Query 4: Cross-sensor comparison ─────────────────────
    run_timed(spark, """
        SELECT
            sensor_type,
            ROUND(AVG(value), 2)     AS mean,
            ROUND(STDDEV(value), 2)  AS std_dev,
            ROUND(MIN(value), 2)     AS p0,
            ROUND(MAX(value), 2)     AS p100,
            COUNT(*)                 AS n
        FROM sensor_curated
        GROUP BY sensor_type
        ORDER BY sensor_type
    """, "QUERY 4 – Cross-sensor statistical comparison")

    # ── Partition pruning benchmark ───────────────────────────
    sep("STEP 5 – Partition Pruning Benchmark")

    print("  Running full table scan…")
    t0 = time.time()
    spark.sql("SELECT COUNT(*) FROM sensor_curated").collect()
    full_scan = time.time() - t0

    print(f"  Running pruned scan (sensor_type = '{target_sensor}')…")
    t0 = time.time()
    spark.sql(f"""
        SELECT COUNT(*) FROM sensor_curated
        WHERE sensor_type = '{target_sensor}'
    """).collect()
    pruned_scan = time.time() - t0

    speedup = full_scan / pruned_scan if pruned_scan > 0 else float("inf")

    print(f"\n  Full scan time   : {full_scan:.3f} s")
    print(f"  Pruned scan time : {pruned_scan:.3f} s")
    print(f"  Speedup          : {speedup:.1f}×")

    if speedup >= 2:
        print(f"\n  ✅  Partition pruning is working.")
    else:
        print(f"\n  ⚠  Speedup < 2× — dataset may be too small to show pruning benefit.")
        print(f"     Produce more messages (python python/producer.py --count 2000)")

    # ── Query plan to confirm PartitionFilters ────────────────
    sep("QUERY PLAN – Confirming PartitionFilters in the physical plan")
    pruned_df = spark.sql(f"""
        SELECT sensor_type, value FROM sensor_curated
        WHERE sensor_type = '{target_sensor}' LIMIT 1
    """)
    pruned_df.explain(mode="formatted")

    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Session 4 Spark SQL query lab")
    p.add_argument(
        "--sensor", default="temperature",
        choices=["temperature", "humidity", "pressure"],
        help="Target sensor for pruning queries (default: temperature)"
    )
    args = p.parse_args()
    main(args.sensor)
