#!/usr/bin/env python3
"""
consumer.py – Session 2 Lab: Kafka Sensor Consumer
====================================================
Reads sensor readings from `sensor-events`, triggers alerts for
high-temperature values, and commits offsets MANUALLY after each message.

Key concepts demonstrated:
  - enable.auto.commit=False + consumer.commit()  → at-least-once delivery
  - auto.offset.reset='earliest'                  → replay from start on first run
  - on_assign callback                            → show partition assignment
  - session.timeout.ms / heartbeat.interval.ms   → failure detection tuning
  - Crash-and-replay exercise (Steps 5b/5c in lab)

Usage:
    python python/consumer.py
    # Press Ctrl+C to stop

Run multiple instances simultaneously to trigger a rebalance (Step 4).
"""

import json
import signal
import sys
import time

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from confluent_kafka import Consumer, KafkaError, KafkaException

# ── Configuration ─────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC         = "sensor-events"
GROUP_ID      = "sensor-analytics"
TEMP_ALERT_C  = 35.0    # alert threshold for temperature readings

# ── Graceful shutdown ─────────────────────────────────────────────────────────
_running = True

def _sigint(sig, frame):
    global _running
    print("\n⚡  SIGINT received – stopping consumer…")
    _running = False

signal.signal(signal.SIGINT, _sigint)


# ── Processing logic ──────────────────────────────────────────────────────────
def process_record(record: dict, key: str, partition: int, offset: int):
    """Business logic: print reading and alert on high temperature."""
    alert = ""
    if key == "temperature" and record.get("value", 0) > TEMP_ALERT_C:
        alert = f"  ⚠  ALERT: HIGH TEMP {record['value']} °C !"

    print(
        f"  [P{partition} | O{offset:>5}]  "
        f"{key:<12} = {record.get('value', '?'):>8.2f} {record.get('unit', '')}  "
        f"device={record.get('device_id', '?')}"
        f"{alert}"
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print(" Kafka Sensor Consumer – Session 2")
    print(f" Topic  : {TOPIC}")
    print(f" Group  : {GROUP_ID}")
    print(f" Offset : earliest (replay from beginning on first run)")
    print(f" Commit : MANUAL (after-each-message = at-least-once)")
    print("=" * 65)

    def on_assign(c, partitions):
        parts = sorted(p.partition for p in partitions)
        print(f"  Assigned partitions: {parts}")

    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,

        # ── Group membership ───────────────────────────────────
        'group.id': GROUP_ID,

        # ── Offset management ──────────────────────────────────
        'auto.offset.reset': 'earliest',    # replay all on first run
        'enable.auto.commit': False,        # we commit manually after processing

        # ── Throughput & health ────────────────────────────────
        'session.timeout.ms': 45000,        # consumer considered dead after 45 s
        'heartbeat.interval.ms': 15000,     # heartbeat every 15 s
        'fetch.min.bytes': 1,               # return immediately even for 1 byte
    })

    consumer.subscribe([TOPIC], on_assign=on_assign)

    print("\n▶  Joining consumer group, waiting for partition assignment…")
    print(f"\n  Listening for messages… (Ctrl+C to stop)\n{'─' * 65}")

    msg_count = 0
    start_time = time.time()

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"\n✘  Kafka error: {msg.error()}")
                break

            msg_count += 1
            key   = msg.key().decode('utf-8') if msg.key() else '(no-key)'
            value = json.loads(msg.value().decode('utf-8'))

            process_record(
                record=value,
                key=key,
                partition=msg.partition(),
                offset=msg.offset(),
            )

            # ── MANUAL COMMIT: only after successful processing ──
            # If the process crashes here, Kafka will re-deliver this message
            # on the next restart → at-least-once semantics
            consumer.commit(msg)

    except KafkaException as exc:
        print(f"\n✘  Kafka error: {exc}")

    finally:
        elapsed = time.time() - start_time
        consumer.close()
        print(f"\n{'─' * 65}")
        print(f"  Consumer stopped.")
        print(f"  Messages processed : {msg_count}")
        print(f"  Elapsed            : {elapsed:.1f} s")
        if elapsed > 0 and msg_count > 0:
            print(f"  Throughput         : {msg_count / elapsed:.1f} msg/s")


if __name__ == "__main__":
    main()
