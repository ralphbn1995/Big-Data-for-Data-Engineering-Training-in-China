#!/usr/bin/env python3
"""
lag_monitor.py – Session 2 Lab: Consumer Lag Monitor
=====================================================
Polls consumer group lag for `sensor-analytics` every N seconds and
displays a live table in the terminal — just like the Kafka UI but in CLI.

Key metric: LAG = high_water_mark_offset - committed_offset
  LAG = 0  → consumer is fully caught up
  LAG > 0  → consumer is behind; a growing lag means consumer is too slow

Usage:
    python python/lag_monitor.py [--interval N]
    # Press Ctrl+C to stop
"""

import argparse
import signal
import sys
import time

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from confluent_kafka import Consumer, TopicPartition

# ── Configuration ─────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC     = "sensor-events"
GROUP_ID  = "sensor-analytics"
DEFAULT_INTERVAL = 3   # seconds between refresh

# ── Graceful shutdown ─────────────────────────────────────────────────────────
_running = True

def _sigint(sig, frame):
    global _running
    print("\n⚡  Stopping lag monitor.")
    _running = False

signal.signal(signal.SIGINT, _sigint)


# ── Lag calculation ───────────────────────────────────────────────────────────
def get_lag_table(consumer: Consumer) -> list[dict]:
    """Return per-partition lag information."""
    # Step 1: discover partitions via topic metadata
    meta = consumer.list_topics(TOPIC, timeout=10)
    if TOPIC not in meta.topics or meta.topics[TOPIC].error:
        return []

    partitions = sorted(meta.topics[TOPIC].partitions.keys())
    tps = [TopicPartition(TOPIC, p) for p in partitions]

    # Step 2: get committed offsets for our consumer group
    committed_tps = consumer.committed(tps, timeout=5)

    # Step 3: get high-water mark (end offset) per partition
    rows = []
    for tp in committed_tps:
        _low, high = consumer.get_watermark_offsets(tp, timeout=5)
        # offset is OFFSET_INVALID (-1001) when no commit exists yet
        committed_val = tp.offset if tp.offset >= 0 else 0
        lag = max(0, high - committed_val)

        rows.append({
            "partition":  tp.partition,
            "committed":  committed_val,
            "end":        high,
            "lag":        lag,
        })

    return rows


# ── Display ───────────────────────────────────────────────────────────────────
def print_table(rows: list[dict], tick: int):
    """Print a formatted lag table."""
    now = time.strftime("%H:%M:%S")
    total_lag = sum(r["lag"] for r in rows)

    print(f"\n  ── Lag Monitor  [{now}]  tick={tick}  group={GROUP_ID} ──")
    print(f"  {'Part':>5}  {'Committed':>10}  {'End Offset':>10}  {'LAG':>8}")
    print(f"  {'─'*5}  {'─'*10}  {'─'*10}  {'─'*8}")

    for r in rows:
        lag_str = str(r["lag"])
        if r["lag"] > 50:
            lag_str = f"⚠ {r['lag']}"
        elif r["lag"] == 0:
            lag_str = "✔ 0"
        print(
            f"  {r['partition']:>5}  "
            f"{r['committed']:>10}  "
            f"{r['end']:>10}  "
            f"{lag_str:>8}"
        )

    status = "✅ HEALTHY" if total_lag == 0 else f"⚠  BEHIND by {total_lag} msg"
    print(f"  {'─'*38}")
    print(f"  Total lag: {total_lag}   →  {status}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main(interval: int):
    print("=" * 55)
    print(" Consumer Lag Monitor – Session 2")
    print(f" Topic   : {TOPIC}")
    print(f" Group   : {GROUP_ID}")
    print(f" Refresh : every {interval} s")
    print(f" Open Kafka UI: http://localhost:8080 → Consumer Groups")
    print("=" * 55)

    # Unsubscribed consumer used only for metadata/offset APIs
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'enable.auto.commit': False,
    })

    tick = 0
    try:
        while _running:
            tick += 1
            rows = get_lag_table(consumer)
            if rows:
                print_table(rows, tick)
            else:
                print(f"\n  [tick {tick}] Topic '{TOPIC}' not found or no partitions.")
            time.sleep(interval)
    finally:
        consumer.close()
        print("\n  Monitor stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Session 2 consumer lag monitor")
    parser.add_argument(
        "--interval", type=int, default=DEFAULT_INTERVAL,
        help=f"Refresh interval in seconds (default: {DEFAULT_INTERVAL})"
    )
    args = parser.parse_args()
    main(args.interval)
