#!/usr/bin/env python3
"""
explore_lake.py – Session 4: Data Lake Structure Explorer
==========================================================
Walks the C:/tmp/datalake directory tree and displays:
  - Zone layout with file counts and sizes
  - Partition directory tree
  - Parquet schema per zone
  - File size histogram (small file problem detection)

Usage:
    python python/explore_lake.py
    python python/explore_lake.py --zone curated
"""

import argparse
import os

from utils import fmt_bytes

LAKE_ROOT = "C:/tmp/datalake"


def walk_zone(path: str, max_depth: int = 5, depth: int = 0, prefix: str = ""):
    if depth > max_depth or not os.path.isdir(path):
        return
    entries = sorted(os.listdir(path))
    for i, entry in enumerate(entries):
        full = os.path.join(path, entry)
        is_last = (i == len(entries) - 1)
        connector = "└── " if is_last else "├── "
        extension = "    " if is_last else "│   "

        if os.path.isdir(full):
            # Count parquet files recursively
            parquet_files = [f for r, _, fs in os.walk(full)
                             for f in fs if f.endswith(".parquet")]
            n = len(parquet_files)
            size = sum(os.path.getsize(os.path.join(r, f))
                       for r, _, fs in os.walk(full) for f in fs
                       if f.endswith(".parquet"))
            tag = f"  [{n} parquet file(s), {fmt_bytes(size)}]" if n else ""
            print(f"{prefix}{connector}{entry}/{tag}")
            walk_zone(full, max_depth, depth + 1, prefix + extension)
        else:
            if entry.endswith(".parquet"):
                size = os.path.getsize(full)
                print(f"{prefix}{connector}{entry}  ({fmt_bytes(size)})")
            elif not entry.startswith("_") and not entry.startswith("."):
                print(f"{prefix}{connector}{entry}")


def zone_summary(zone_path: str, zone_name: str):
    print(f"\n{'─' * 60}")
    print(f"  {zone_name.upper()}")
    print(f"  {zone_path}")
    print(f"{'─' * 60}")

    if not os.path.exists(zone_path):
        print("  (not yet created)")
        return

    all_parquets, all_jsons, all_other = [], [], []
    for root, _, files in os.walk(zone_path):
        for f in files:
            fp = os.path.join(root, f)
            if f.endswith(".parquet"):
                all_parquets.append(fp)
            elif f.endswith(".json") and not f.startswith("_"):
                all_jsons.append(fp)
            elif not f.startswith("_") and not f.startswith("."):
                all_other.append(fp)

    def summarize_files(label, files):
        if not files:
            return
        sizes = [os.path.getsize(f) for f in files]
        total = sum(sizes)
        avg   = total // len(sizes)
        min_s = min(sizes)
        max_s = max(sizes)
        print(f"\n  {label}: {len(files)} file(s)  total={fmt_bytes(total)}")
        print(f"    avg={fmt_bytes(avg)}  min={fmt_bytes(min_s)}  max={fmt_bytes(max_s)}")
        # Small file warning
        if avg < 1024 * 1024 and len(files) > 10:
            print(f"    ⚠  Small file problem: avg file < 1 MB with {len(files)} files")
            print(f"       Run: python python/compaction.py")

    summarize_files("Parquet files", all_parquets)
    summarize_files("JSON files",    all_jsons)

    print(f"\n  Directory tree (max depth 4):")
    walk_zone(zone_path, max_depth=4)


def main(zone_filter: str | None):
    print("=" * 60)
    print(f" Session 4 – Data Lake Explorer")
    print(f" Root: {LAKE_ROOT}")
    print("=" * 60)

    if not os.path.exists(LAKE_ROOT):
        print(f"\n  ⚠  {LAKE_ROOT} does not exist.")
        print(f"     Run scripts/setup.sh first.")
        return

    zones = {
        "raw":         f"{LAKE_ROOT}/raw",
        "curated":     f"{LAKE_ROOT}/curated",
        "consumption": f"{LAKE_ROOT}/consumption",
        "compacted":   f"{LAKE_ROOT}/curated-compacted",
    }

    for name, path in zones.items():
        if zone_filter and zone_filter != name:
            continue
        zone_summary(path, f"{name} zone")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Explore Session 4 data lake")
    p.add_argument(
        "--zone", default=None,
        choices=["raw", "curated", "consumption", "compacted"],
        help="Show only one zone"
    )
    args = p.parse_args()
    main(args.zone)
