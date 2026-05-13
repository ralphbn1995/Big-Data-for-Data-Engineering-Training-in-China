#!/usr/bin/env python3
"""
producer.py – Session 1 Lab: Kafka Python Producer
====================================================
Sends synthetic user-event messages to the `session1-events` topic.

Usage:
    pip install kafka-python-ng
    python python/producer.py

The producer sends messages with a message key (user ID) to guarantee
that all events for the same user land on the same partition (ordering guarantee).
"""

import json
import sys
import time
import random
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Configuration ─────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9094", "localhost:9096"]
TOPIC = "session1-events"
NUM_MESSAGES = 20          # total messages to produce
DELAY_SECONDS = 0.5        # pause between messages (seconds)

# ── Sample data ───────────────────────────────────────────────────────────────
USERS = [f"user{i}" for i in range(1, 6)]   # user1 … user5
EVENTS = ["login", "page_view", "add_to_cart", "purchase", "logout"]
ITEMS  = ["laptop", "phone", "headphones", "keyboard", "monitor"]


def make_event(user_id: str) -> dict:
    """Build a realistic-looking event payload."""
    event_type = random.choice(EVENTS)
    payload: dict = {
        "event": event_type,
        "user_id": user_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        "session_id": f"sess_{random.randint(100, 999)}",
    }
    if event_type in ("add_to_cart", "purchase"):
        payload["item"]   = random.choice(ITEMS)
        payload["amount"] = round(random.uniform(10, 2000), 2)
    return payload


def on_send_success(record_metadata):
    print(
        f"  ✔  topic={record_metadata.topic}  "
        f"partition={record_metadata.partition}  "
        f"offset={record_metadata.offset}"
    )


def on_send_error(exc: KafkaError):
    print(f"  ✘  Error: {exc}")


def main():
    print("=" * 60)
    print(" Kafka Python Producer – Session 1")
    print(f" Topic : {TOPIC}")
    print(f" Broker: {BOOTSTRAP_SERVERS[0]}")
    print("=" * 60)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Serialize key and value as UTF-8 JSON
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Durability: wait for all in-sync replicas to acknowledge
        acks="all",
        # Retry up to 3 times on transient errors
        retries=3,
    )

    for i in range(1, NUM_MESSAGES + 1):
        user_id = random.choice(USERS)
        event   = make_event(user_id)

        print(f"\n[{i:02d}/{NUM_MESSAGES}] key={user_id}  event={event['event']}")

        producer.send(TOPIC, key=user_id, value=event) \
                .add_callback(on_send_success) \
                .add_errback(on_send_error)

        time.sleep(DELAY_SECONDS)

    # Flush ensures all buffered messages are sent before exit
    producer.flush()
    producer.close()
    print("\n✅  All messages sent. Producer closed.")


if __name__ == "__main__":
    main()
