#!/usr/bin/env python3
"""
scripts/seed_data.py – Session 5: Populate Kafka with test data
================================================================
Sends a burst of sensor readings to Kafka so the /latest endpoint
has data to return immediately.

Usage:
    python scripts/seed_data.py
    python scripts/seed_data.py --count 200
"""
import argparse
import json
import random
import sys
import time
import os

sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9094", "localhost:9096"]
TOPIC             = "sensor-events"

SENSOR_TYPES = ["temperature", "humidity", "pressure"]
UNITS        = {"temperature": "C", "humidity": "%", "pressure": "hPa"}
VALUE_RANGE  = {
    "temperature": (8.0,   42.0),
    "humidity":    (20.0,  95.0),
    "pressure":    (975.0, 1045.0),
}


def main(count: int):
    print(f"Seeding Kafka topic '{TOPIC}' with {count} messages…")

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            acks="all",
            # Note: enable_idempotence not supported in kafka-python-ng;
            # acks='all' provides at-least-once durability.
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            linger_ms=10,
        )
    except NoBrokersAvailable:
        print("  ❌  Kafka not reachable. Start Docker cluster first.")
        sys.exit(1)

    sent = 0
    try:
        for i in range(count):
            sensor = random.choice(SENSOR_TYPES)
            lo, hi = VALUE_RANGE[sensor]
            reading = {
                "sensor":    sensor,
                "value":     round(random.uniform(lo, hi), 2),
                "unit":      UNITS[sensor],
                "timestamp": int(time.time() * 1000),
                "device_id": f"{sensor[:4]}-{random.randint(1,5):02d}",
                "source":    "seed-script",
            }
            producer.send(TOPIC, key=sensor, value=reading)
            sent += 1
            if sent % 50 == 0:
                print(f"  {sent}/{count} sent…")

        producer.flush()
        producer.close()
        print(f"\n✅  {sent} messages sent to '{TOPIC}'.")
        print(f"   Now start the API: python run.py")

    except KeyboardInterrupt:
        producer.flush()
        producer.close()
        print(f"\n⚡  Interrupted after {sent} messages.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=100)
    args = p.parse_args()
    main(args.count)
