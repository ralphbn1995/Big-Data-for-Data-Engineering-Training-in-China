#!/usr/bin/env python3
"""
compaction.py – Session 4: Small File Compaction
=================================================
Demonstrates the "small file problem" and its solution.

Step 1: Count files in the curated zone before compaction.
Step 2: Read all small Parquet files and re-write as fewer larger files.
Step 3: Count files after compaction and measure size improvement.

This simulates what happens after hours of 30-second streaming triggers,
each creating 3 small files (one per sensor type per batch).

Usage:
    python python/compaction.py            # compact all sensor types
    python python/compaction.py --dry-run  # count files, no write
    python python/compaction.py --target-files 5
"""

import argparse
import os
import time

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from utils import fmt_bytes

LAKE_ROOT       = "C:/tmp/datalake"
CURATED_PATH    = f"{LAKE_ROOT}/curated/domain=iot"
COMPACTED_PATH  = f"{LAKE_ROOT}/curated-compacted/domain=iot"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Session4-Compaction")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def count_files(path: str) -> tuple[int, int]:
    """Returns (parquet_count, total_bytes)."""
    count, total_bytes = 0, 0
    for root, _, files in os.walk(path):
        for f in files:
            if f.endswith(".parquet"):
                count += 1
                total_bytes += os.path.getsize(os.path.join(root, f))
    return count, total_bytes


def main(dry_run: bool, target_files: int):
    print("=" * 65)
    print(" Session 4 – Small File Compaction")
    print(f" Source  : {CURATED_PATH}")
    print(f" Output  : {COMPACTED_PATH}")
    print(f" Target  : {target_files} file(s) per partition")
    print(f" Mode    : {'DRY RUN (no write)' if dry_run else 'EXECUTE'}")
    print("=" * 65)

    before_count, before_bytes = count_files(CURATED_PATH)

    if before_count == 0:
        print(f"\n  ⚠  No Parquet files found in {CURATED_PATH}")
        print(f"     Run datalake_pipeline.py first.")
        return

    print(f"\n  Before compaction:")
    print(f"    Files : {before_count}")
    print(f"    Size  : {fmt_bytes(before_bytes)}")
    print(f"    Avg   : {fmt_bytes(before_bytes // before_count) if before_count else 'N/A'} per file")

    if dry_run:
        print(f"\n  (Dry run – nothing written.)")
        return

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"\n▶  Reading {before_count} files…")
    df = spark.read.parquet(CURATED_PATH)

    print(f"▶  Coalescing to {target_files} file(s) per partition and writing…")
    t0 = time.time()
    (
        df
        .coalesce(target_files)              # reduce output files per task
        .write
        .mode("overwrite")
        .partitionBy("sensor_type", "year", "month", "day")
        .option("compression", "snappy")
        .parquet(COMPACTED_PATH)
    )
    elapsed = time.time() - t0

    after_count, after_bytes = count_files(COMPACTED_PATH)

    print(f"\n  After compaction ({elapsed:.1f} s):")
    print(f"    Files  : {after_count}")
    print(f"    Size   : {fmt_bytes(after_bytes)}")
    if after_count > 0:
        print(f"    Avg    : {fmt_bytes(after_bytes // after_count)} per file")

    if before_count > 0 and after_count > 0:
        reduction = (1 - after_count / before_count) * 100
        print(f"\n  File count reduction : {reduction:.0f}%")
        print(f"  ({before_count} → {after_count} files)")

    print(f"\n✅  Compacted data written to: {COMPACTED_PATH}")
    print(f"   In production: replace original with compacted files.")

    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Session 4 compaction utility")
    p.add_argument("--dry-run", action="store_true",
                   help="Count files only, do not write")
    p.add_argument("--target-files", type=int, default=1,
                   help="Target output files per partition (default: 1)")
    args = p.parse_args()
    main(args.dry_run, args.target_files)
