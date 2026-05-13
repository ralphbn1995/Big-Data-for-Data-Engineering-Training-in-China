#!/usr/bin/env python3
"""
consumer.py – Session 1 Lab: Kafka Python Consumer
====================================================
Reads messages from the `session1-events` topic as part of the
`analytics-group` consumer group, and prints them with metadata.

Usage:
    pip install confluent-kafka
    python python/consumer.py

Press Ctrl+C to stop gracefully.
"""

import json
import signal
import sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from confluent_kafka import Consumer, KafkaError, KafkaException

# ── Configuration ─────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC        = "session1-events"
GROUP_ID     = "analytics-group"
AUTO_OFFSET  = "earliest"   # "earliest" = from-beginning; "latest" = only new msgs


# ── Graceful shutdown ─────────────────────────────────────────────────────────
_running = True

def _handle_sigint(sig, frame):
    global _running
    print("\n\n⚡  SIGINT received – stopping consumer …")
    _running = False

signal.signal(signal.SIGINT, _handle_sigint)


# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_value(raw: bytes) -> dict:
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {"raw": raw.decode("utf-8", errors="replace")}


def print_banner():
    print("=" * 65)
    print(" Kafka Python Consumer – Session 1")
    print(f" Topic  : {TOPIC}")
    print(f" Group  : {GROUP_ID}")
    print(f" Broker : {BOOTSTRAP_SERVERS.split(',')[0]}")
    print(f" Offset : {AUTO_OFFSET}")
    print("=" * 65)
    print(" Waiting for messages … (Ctrl+C to stop)")
    print("-" * 65)


def main():
    print_banner()

    consumer = Consumer({
        'bootstrap.servers':      BOOTSTRAP_SERVERS,
        'group.id':               GROUP_ID,
        'auto.offset.reset':      AUTO_OFFSET,
        'enable.auto.commit':     True,       # commit offsets automatically
        'auto.commit.interval.ms': 1000,
    })
    consumer.subscribe([TOPIC])

    msg_count = 0

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
            key   = msg.key().decode('utf-8') if msg.key() else None
            value = fmt_value(msg.value())

            print(
                f"  partition={msg.partition():>2}  "
                f"offset={msg.offset():>6}  "
                f"key={str(key):<10}  "
                f"event={value.get('event', '?'):<15}  "
                f"ts={value.get('ts', '')}"
            )

    except KafkaException as exc:
        print(f"\n✘  Kafka error: {exc}")
    finally:
        consumer.close()
        print("-" * 65)
        print(f"\n✅  Consumer stopped. Total messages read: {msg_count}")


if __name__ == "__main__":
    main()
