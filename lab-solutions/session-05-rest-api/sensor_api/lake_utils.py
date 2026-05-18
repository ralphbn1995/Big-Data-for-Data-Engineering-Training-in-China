"""
lake_utils.py – Session 5: Parquet data lake query helpers
===========================================================
Provides two functions used by the Flask API:

  get_sensor_types()
    → Returns the list of distinct sensor types present in the
      curated Parquet zone (discovered from partition directory names).

  get_statistics(sensor_type, days)
    → Returns daily aggregates for a sensor type over the last N days,
      queried from the curated Parquet zone using PySpark.

Design note: SparkSession is created once (singleton) and reused across
requests. PySpark startup takes ~5 seconds; we pay this cost once at
first request, not on every call.
"""

import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

# Windows: set HADOOP_HOME before PySpark starts the JVM so hadoop.dll can be found.
# Bash-style paths like /c/hadoop are rejected by the JVM on Windows.
if os.name == "nt":
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

# ── Parquet path (from Session 4) ─────────────────────────────────────────────
_default_lake = (
    "C:/tmp/datalake/curated/domain=iot" if os.name == "nt"
    else "/tmp/datalake/curated/domain=iot"
)
CURATED_PATH = os.environ.get("CURATED_PATH", _default_lake)

# ── Lazy SparkSession singleton ───────────────────────────────────────────────
_spark = None
_spark_lock = threading.Lock()


def _get_spark():
    """Return a singleton SparkSession, creating it on first call."""
    global _spark
    if _spark is None:
        with _spark_lock:
            if _spark is None:
                try:
                    from pyspark.sql import SparkSession
                    _spark = (
                        SparkSession.builder
                        .appName("Session5-SensorAPI")
                        .master("local[2]")    # 2 cores is enough for API queries
                        .config("spark.sql.shuffle.partitions", "3")
                        .config("spark.ui.showConsoleProgress", "false")
                        .config("spark.ui.enabled", "false")   # no web UI for API
                        .getOrCreate()
                    )
                    _spark.sparkContext.setLogLevel("ERROR")
                except Exception:
                    _spark = None   # PySpark/Java unavailable → fallback to returning []
    return _spark


# ── get_sensor_types ──────────────────────────────────────────────────────────
def get_sensor_types() -> list[str]:
    """
    Discover sensor types from the curated zone's Hive-style partition directories.
    Pattern: .../curated/domain=iot/sensor_type=temperature/

    Falls back to a static list if the path doesn't exist yet.
    """
    if not os.path.isdir(CURATED_PATH):
        return sorted(["temperature", "humidity", "pressure"])

    sensor_types = []
    for entry in os.listdir(CURATED_PATH):
        if entry.startswith("sensor_type="):
            sensor_types.append(entry.split("=", 1)[1])

    if not sensor_types:
        # No partition directories yet; return known types
        return sorted(["temperature", "humidity", "pressure"])

    return sorted(sensor_types)


# ── get_statistics ────────────────────────────────────────────────────────────
def get_statistics(sensor_type: str, days: int = 7) -> list[dict]:
    """
    Return daily aggregated statistics for a sensor type.

    Uses PySpark to read the Parquet curated zone with partition pruning.
    Falls back to a filesystem-only check if PySpark is unavailable.

    Args:
        sensor_type: e.g. "temperature"
        days: number of recent days to include

    Returns:
        list of dicts, one per day, sorted date descending:
        [{
            "date":          "2024-01-15",
            "sensor_type":   "temperature",
            "record_count":  148,
            "avg_value":     27.34,
            "min_value":     10.20,
            "max_value":     39.80,
            "anomaly_count": 12,
            "anomaly_pct":   8.11,
        }, ...]
    """
    # Sensor-specific partition path
    sensor_path = os.path.join(CURATED_PATH, f"sensor_type={sensor_type}")
    if not os.path.isdir(sensor_path):
        return []

    try:
        spark = _get_spark()
        if spark is None:
            return []

        from pyspark.sql.functions import (
            col, avg, min as spark_min, max as spark_max,
            count, sum as spark_sum, round as spark_round,
            concat_ws, lpad, when,
        )

        # Read with partition pruning — Spark reads only the sensor_type dir
        df = (
            spark.read.parquet(CURATED_PATH)
            .filter(col("sensor_type") == sensor_type)
        )

        if df.rdd.isEmpty():
            return []

        # Filter to the last `days` days
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        cutoff_year  = cutoff.year
        cutoff_month = cutoff.month
        cutoff_day   = cutoff.day

        # Build a string date from partition columns for filtering
        recent = (
            df
            .filter(
                (col("year") > cutoff_year) |
                (
                    (col("year") == cutoff_year) &
                    (col("month") > cutoff_month)
                ) |
                (
                    (col("year") == cutoff_year) &
                    (col("month") == cutoff_month) &
                    (col("day") >= cutoff_day)
                )
            )
        )

        if recent.rdd.isEmpty():
            return []

        # Daily aggregates
        agg = (
            recent
            .groupBy("year", "month", "day")
            .agg(
                count("value").alias("record_count"),
                spark_round(avg("value"), 3).alias("avg_value"),
                spark_round(spark_min("value"), 3).alias("min_value"),
                spark_round(spark_max("value"), 3).alias("max_value"),
                spark_sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
            )
            .orderBy(col("year").desc(), col("month").desc(), col("day").desc())
        )

        rows = agg.collect()
        results = []
        for row in rows:
            total   = row["record_count"]
            anomaly = row["anomaly_count"] or 0
            pct     = round(100.0 * anomaly / total, 2) if total > 0 else 0.0
            date_str = (
                f"{row['year']:04d}-{row['month']:02d}-{row['day']:02d}"
            )
            results.append({
                "date":          date_str,
                "sensor_type":   sensor_type,
                "record_count":  int(total),
                "avg_value":     float(row["avg_value"] or 0),
                "min_value":     float(row["min_value"] or 0),
                "max_value":     float(row["max_value"] or 0),
                "anomaly_count": int(anomaly),
                "anomaly_pct":   pct,
            })

        return results

    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("get_statistics error: %s", exc, exc_info=True)
        return []
