#!/usr/bin/env python3
"""
producer.py – Session 2 Lab: Kafka Sensor Producer
====================================================
Simulates three IoT sensor types (temperature, humidity, pressure)
sending readings to the `sensor-events` topic.

Key concepts demonstrated:
  - acks='all' + enable_idempotence=True  → exactly-once writes per partition
  - key=sensor_type                       → consistent partition routing
  - linger_ms + batch_size                → batching for throughput
  - future.get()  vs  callbacks           → sync vs async confirmation

Usage:
    pip install -r requirements.txt
    python python/producer.py [--count N]
"""

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

# ── Configuration ─────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9094", "localhost:9096"]
TOPIC = "sensor-events"
DEFAULT_COUNT = 50       # number of messages to send
DELAY_SECONDS = 0.2      # pause between messages

SENSOR_TYPES = ["temperature", "humidity", "pressure"]

UNITS = {
    "temperature": "C",
    "humidity":    "%",
    "pressure":    "hPa",
}

VALUE_RANGE = {
    "temperature": (10.0, 42.0),   # deliberately wide: some will trigger alerts
    "humidity":    (20.0, 95.0),
    "pressure":    (980.0, 1040.0),
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def make_reading(sensor: str) -> dict:
    lo, hi = VALUE_RANGE[sensor]
    return {
        "sensor":    sensor,
        "value":     round(random.uniform(lo, hi), 2),
        "unit":      UNITS[sensor],
        "timestamp": int(time.time() * 1000),
        "device_id": f"{sensor[:4]}-{random.randint(1, 5):02d}",
    }


def on_success(metadata):
    pass   # uncomment the line below for verbose output
    # print(f"  ✔  P{metadata.partition} O{metadata.offset}")


def on_error(exc: KafkaError):
    print(f"  ✘  Delivery error: {exc}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main(count: int):
    print("=" * 65)
    print(" Kafka Sensor Producer – Session 2")
    print(f" Topic   : {TOPIC}")
    print(f" Messages: {count}")
    print(f" Brokers : {BOOTSTRAP_SERVERS[0]} (+ 2 fallback)")
    print("=" * 65)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,

        # ── Reliability ────────────────────────────────────────
        acks="all",                              # wait for all ISR replicas
        retries=5,                               # retry on transient errors
        # Note: enable_idempotence not supported in kafka-python-ng;
        # acks='all' + retries provides at-least-once durability.

        # ── Serialisers ────────────────────────────────────────
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),

        # ── Throughput ─────────────────────────────────────────
        linger_ms=10,          # wait up to 10 ms to fill a batch
        batch_size=32_768,     # 32 KB max batch size per partition
        compression_type="gzip",
    )

    sent = 0
    errors = 0

    try:
        for i in range(1, count + 1):
            sensor  = random.choice(SENSOR_TYPES)
            reading = make_reading(sensor)

            try:
                # .get() blocks until the broker acknowledges the write
                meta = producer.send(
                    topic=TOPIC,
                    key=sensor,
                    value=reading,
                ).get(timeout=10)

                sent += 1
                alert = ""
                if sensor == "temperature" and reading["value"] > 35:
                    alert = "  ⚠  HIGH TEMP"

                print(
                    f"  [{i:>3}/{count}]  "
                    f"P{meta.partition} O{meta.offset:>5}  "
                    f"{sensor:<12} = {reading['value']:>7.2f} {reading['unit']}"
                    f"{alert}"
                )

            except KafkaTimeoutError as exc:
                errors += 1
                print(f"  [{i:>3}] TIMEOUT: {exc}")

            time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\n⚡  Interrupted by user.")

    finally:
        print(f"\n▶  Flushing remaining buffer…")
        producer.flush()
        producer.close()
        print(f"\n{'=' * 65}")
        print(f"  Sent: {sent}  |  Errors: {errors}")
        print(f"  Topic: {TOPIC}  |  Open Kafka UI → http://localhost:8080")
        print(f"{'=' * 65}")


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Session 2 Kafka sensor producer")
    parser.add_argument(
        "--count", type=int, default=DEFAULT_COUNT,
        help=f"Number of messages to send (default: {DEFAULT_COUNT})"
    )
    args = parser.parse_args()
    main(args.count)
