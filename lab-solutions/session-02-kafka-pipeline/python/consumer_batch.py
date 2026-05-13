#!/usr/bin/env python3
"""
consumer_batch.py – Session 2 Lab (Advanced): Batch Commit Pattern
===================================================================
Instead of committing after EVERY message, this consumer batches up to
BATCH_SIZE records, processes them all, then commits ONCE per batch.

Trade-off:
  - Faster  → fewer round-trips to the broker for offset commits
  - Risk    → on crash, up to BATCH_SIZE messages may be reprocessed

This is the recommended pattern for high-throughput pipelines.

Usage:
    python python/consumer_batch.py [--batch N]
"""

import argparse
import json
import signal
import sys
import time
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from confluent_kafka import Consumer, KafkaError

# ── Configuration ─────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC     = "sensor-events"
GROUP_ID  = "sensor-analytics-batch"
DEFAULT_BATCH = 50

# ── Graceful shutdown ─────────────────────────────────────────────────────────
_running = True

def _sigint(sig, frame):
    global _running
    print("\n⚡  Stopping batch consumer…")
    _running = False

signal.signal(signal.SIGINT, _sigint)


# ── Per-message processing ────────────────────────────────────────────────────
def process(msg) -> dict:
    value = json.loads(msg.value().decode('utf-8'))
    return {
        "partition": msg.partition(),
        "offset":    msg.offset(),
        "sensor":    msg.key().decode('utf-8') if msg.key() else None,
        "value":     value.get("value"),
        "unit":      value.get("unit"),
    }


# ── Aggregation: compute stats per sensor type ───────────────────────────────
def aggregate(batch: list[dict]) -> dict:
    totals   = defaultdict(list)
    for rec in batch:
        if rec["sensor"]:
            totals[rec["sensor"]].append(rec["value"])
    stats = {}
    for sensor, values in totals.items():
        stats[sensor] = {
            "count": len(values),
            "min":   round(min(values), 2),
            "max":   round(max(values), 2),
            "avg":   round(sum(values) / len(values), 2),
        }
    return stats


# ── Main ──────────────────────────────────────────────────────────────────────
def main(batch_size: int):
    print("=" * 65)
    print(" Kafka Batch Consumer – Session 2 (Advanced)")
    print(f" Topic      : {TOPIC}")
    print(f" Group      : {GROUP_ID}")
    print(f" Batch size : {batch_size}")
    print(f" Commit     : once per batch (at-least-once)")
    print("=" * 65)

    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 45000,
        'heartbeat.interval.ms': 15000,
    })
    consumer.subscribe([TOPIC])

    total_processed = 0
    total_batches   = 0
    start = time.time()

    try:
        while _running:
            # consume() returns up to num_messages records, blocking up to timeout seconds
            messages = consumer.consume(num_messages=batch_size, timeout=1.0)

            if not messages:
                continue

            batch = []
            for msg in messages:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"  Consumer error: {msg.error()}")
                    continue
                batch.append(process(msg))

            if not batch:
                continue

            # ── Batch business logic ───────────────────────────
            stats = aggregate(batch)
            total_batches   += 1
            total_processed += len(batch)

            print(f"\n  ── Batch #{total_batches:>4}  ({len(batch)} records) ──")
            for sensor, s in stats.items():
                print(
                    f"    {sensor:<12}  "
                    f"n={s['count']:>3}  "
                    f"min={s['min']:>7.2f}  "
                    f"avg={s['avg']:>7.2f}  "
                    f"max={s['max']:>7.2f}"
                )

            # ── Commit ONCE per batch ──────────────────────────
            consumer.commit()
            print(f"    ✔  Offsets committed (batch #{total_batches})")

    except KeyboardInterrupt:
        pass

    finally:
        elapsed = time.time() - start
        consumer.close()
        print(f"\n{'─' * 65}")
        print(f"  Total processed : {total_processed} messages in {total_batches} batches")
        print(f"  Elapsed         : {elapsed:.1f} s")
        if elapsed > 0 and total_processed > 0:
            print(f"  Throughput      : {total_processed / elapsed:.0f} msg/s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Session 2 batch consumer")
    parser.add_argument(
        "--batch", type=int, default=DEFAULT_BATCH,
        help=f"Records per batch (default: {DEFAULT_BATCH})"
    )
    args = parser.parse_args()
    main(args.batch)
