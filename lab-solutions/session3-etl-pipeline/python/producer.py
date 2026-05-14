#!/usr/bin/env python3
"""
producer.py – Session 3: Kafka Sensor Producer
===============================================
Reuse of the Session 2 producer, with a `source` field added
so the ETL pipeline can demonstrate schema flexibility.

Usage:
    python python/producer.py [--count N] [--delay D]
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9094", "localhost:9096"]
TOPIC             = "sensor-events"

SENSOR_TYPES = ["temperature", "humidity", "pressure"]
UNITS        = {"temperature": "C", "humidity": "%", "pressure": "hPa"}
VALUE_RANGE  = {
    "temperature": (8.0, 42.0),   # wide range: some will trigger alerts
    "humidity":    (20.0, 95.0),
    "pressure":    (975.0, 1045.0),
}


def make_reading(sensor: str, source: str = "lab") -> dict:
    lo, hi = VALUE_RANGE[sensor]
    return {
        "sensor":    sensor,
        "value":     round(random.uniform(lo, hi), 2),
        "unit":      UNITS[sensor],
        "timestamp": int(time.time() * 1000),   # epoch ms
        "device_id": f"{sensor[:4]}-{random.randint(1, 5):02d}",
        "source":    source,
    }


def main(count: int, delay: float):
    print("=" * 60)
    print(" Sensor Producer – Session 3")
    print(f" Topic   : {TOPIC}")
    print(f" Messages: {count}  |  Delay: {delay}s")
    print("=" * 60)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        retries=5,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=10,
        batch_size=32_768,
        compression_type="gzip",
    )

    sent = 0
    try:
        for i in range(1, count + 1):
            sensor  = random.choice(SENSOR_TYPES)
            reading = make_reading(sensor)

            try:
                meta = producer.send(TOPIC, key=sensor, value=reading).get(timeout=10)
                sent += 1
                alert = "  ⚠" if (
                    (sensor == "temperature" and reading["value"] > 35) or
                    (sensor == "humidity"    and reading["value"] > 90)
                ) else ""
                print(
                    f"  [{i:>3}/{count}]  "
                    f"P{meta.partition}  O{meta.offset:>5}  "
                    f"{sensor:<12} = {reading['value']:>8.2f} {reading['unit']}"
                    f"{alert}"
                )
            except KafkaTimeoutError as e:
                print(f"  [{i:>3}] TIMEOUT: {e}")

            time.sleep(delay)

    except KeyboardInterrupt:
        print("\n⚡  Interrupted.")
    finally:
        producer.flush()
        producer.close()
        print(f"\n✅  Sent {sent}/{count} messages to topic '{TOPIC}'.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=60)
    p.add_argument("--delay", type=float, default=0.3,
                   help="Seconds between messages (default 0.3)")
    args = p.parse_args()
    main(args.count, args.delay)
