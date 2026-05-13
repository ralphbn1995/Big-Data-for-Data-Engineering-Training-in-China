#!/usr/bin/env python3
"""
fault_tolerance_test.py – Session 1 Lab: Fault Tolerance Demo
==============================================================
Continuously produces messages while simulating a broker failure via
`docker stop kafka1`. Verifies that:
  1. The producer does NOT lose messages (acks="all" + retries).
  2. The consumer can still read ALL messages from the surviving brokers.

Usage:
    pip install kafka-python-ng confluent-kafka
    python python/fault_tolerance_test.py

The script runs three phases:
  Phase A – Produce 5 messages (all brokers healthy)
  Phase B – Stop kafka1 externally, then produce 5 more messages
  Phase C – Restart kafka1, produce 5 final messages
  Verify  – Consume from beginning and count all 15 messages
"""

import json
import subprocess
import sys
import time
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from kafka import KafkaProducer        # kafka-python-ng (producer)
from kafka.errors import KafkaError
from confluent_kafka import Consumer   # confluent-kafka (consumer)

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9094", "localhost:9096"]
FALLBACK_SERVERS  = ["localhost:9094", "localhost:9096"]   # used after kafka1 stops
TOPIC = "fault-tolerance-test"
TOTAL_EXPECTED = 15


# ── Helpers ───────────────────────────────────────────────────────────────────
def make_producer(servers: list[str]) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=500,
        request_timeout_ms=15000,
    )


def produce_batch(producer: KafkaProducer, phase: str, count: int, start: int = 0):
    sent = []
    for i in range(start, start + count):
        key   = f"key-{i % 3}"          # 3 keys → 3 partitions
        value = {
            "phase": phase,
            "seq":   i,
            "ts":    datetime.now(timezone.utc).isoformat(),
        }
        future = producer.send(TOPIC, key=key, value=value)
        try:
            meta = future.get(timeout=10)
            sent.append(meta.offset)
            print(f"    sent seq={i:>3}  partition={meta.partition}  offset={meta.offset}")
        except KafkaError as exc:
            print(f"    ✘ seq={i} FAILED: {exc}")
    producer.flush()
    return sent


def consume_all(expected: int) -> int:
    """Consume all messages from the topic using confluent-kafka.
    Stops after 5 consecutive seconds with no new messages."""
    consumer = Consumer({
        'bootstrap.servers': ','.join(FALLBACK_SERVERS),
        'group.id':          'ft-verify-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([TOPIC])

    count = 0
    no_msg_streak = 0
    while no_msg_streak < 5:   # 5 consecutive empty 1-second polls → done
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            no_msg_streak += 1
            continue
        if msg.error():
            no_msg_streak += 1
            continue
        count += 1
        no_msg_streak = 0

    consumer.close()
    return count


def sep(title: str = ""):
    print("\n" + "─" * 60)
    if title:
        print(f" {title}")
        print("─" * 60)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    sep("FAULT TOLERANCE DEMO – Session 1")
    print(f" Topic   : {TOPIC}")
    print(f" Expected: {TOTAL_EXPECTED} messages (3 phases × 5)")
    print("─" * 60)

    # ── Phase A: All brokers healthy ─────────────────────────
    sep("PHASE A – All brokers healthy (producing 5 msgs)")
    p = make_producer(BOOTSTRAP_SERVERS)
    produce_batch(p, "A", 5, start=0)
    p.close()

    # ── Stop kafka1 ──────────────────────────────────────────
    sep("Stopping kafka1 …")
    print(" $ docker stop kafka1")
    try:
        result = subprocess.run(
            ["docker", "stop", "kafka1"],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            print(" ✔ kafka1 stopped.")
        else:
            print(f" ✘ docker stop failed: {result.stderr}")
            print("   → Make sure Docker is running and the cluster is up.")
            sys.exit(1)
    except FileNotFoundError:
        print(" ✘ Docker CLI not found. Run `docker stop kafka1` manually, then re-run.")
        sys.exit(1)

    time.sleep(5)   # give the cluster time to elect new leaders

    # ── Phase B: One broker down ─────────────────────────────
    sep("PHASE B – kafka1 DOWN (producing 5 msgs via remaining brokers)")
    p = make_producer(FALLBACK_SERVERS)
    produce_batch(p, "B", 5, start=5)
    p.close()

    # ── Restart kafka1 ────────────────────────────────────────
    sep("Restarting kafka1 …")
    print(" $ docker start kafka1")
    try:
        result = subprocess.run(
            ["docker", "start", "kafka1"],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            print(" ✔ kafka1 started.")
        else:
            print(f" ✘ docker start failed: {result.stderr}")
    except FileNotFoundError:
        print(" ✘ Docker CLI not found. Run `docker start kafka1` manually, then re-run.")
        sys.exit(1)

    time.sleep(10)  # wait for log replication catch-up

    # ── Phase C: All brokers healthy again ───────────────────
    sep("PHASE C – All brokers restored (producing 5 msgs)")
    p = make_producer(BOOTSTRAP_SERVERS)
    produce_batch(p, "C", 5, start=10)
    p.close()

    # ── Verification ─────────────────────────────────────────
    sep("VERIFICATION – Consuming all messages from beginning")
    time.sleep(2)
    total = consume_all(TOTAL_EXPECTED)
    print(f"\n  Messages expected : {TOTAL_EXPECTED}")
    print(f"  Messages received : {total}")

    if total >= TOTAL_EXPECTED:
        print(f"\n  ✅  PASS – No messages lost during broker failure!")
    else:
        missing = TOTAL_EXPECTED - total
        print(f"\n  ✘  FAIL – {missing} message(s) missing.")

    sep()


if __name__ == "__main__":
    main()
