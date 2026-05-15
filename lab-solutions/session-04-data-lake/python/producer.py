#!/usr/bin/env python3
"""
producer.py – Session 4: Kafka Sensor Producer
===============================================
Extended producer that populates sensor-events with enough volume
for meaningful partition pruning benchmarks.

Usage:
    python python/producer.py --count 500    # 500 messages (recommended)
    python python/producer.py --count 2000   # larger dataset for pruning demo
    python python/producer.py --loop         # continuous production until Ctrl+C
"""

import argparse
import json
import random
import time

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9094", "localhost:9096"]
TOPIC             = "sensor-events"

SENSOR_TYPES = ["temperature", "humidity", "pressure"]
UNITS        = {"temperature": "C", "humidity": "%", "pressure": "hPa"}
VALUE_RANGE  = {
    "temperature": (8.0, 42.0),
    "humidity":    (20.0, 95.0),
    "pressure":    (975.0, 1045.0),
}


def make_reading(sensor: str) -> dict:
    lo, hi = VALUE_RANGE[sensor]
    return {
        "sensor":    sensor,
        "value":     round(random.uniform(lo, hi), 2),
        "unit":      UNITS[sensor],
        "timestamp": int(time.time() * 1000),
        "device_id": f"{sensor[:4]}-{random.randint(1, 5):02d}",
        "source":    "lab-session4",
    }


def main(count: int, delay: float, loop: bool):
    print("=" * 60)
    print(f" Session 4 Producer – topic: {TOPIC}")
    print(f" {'Continuous loop (Ctrl+C to stop)' if loop else f'{count} messages'}")
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
    i = 0
    try:
        while loop or i < count:
            sensor  = random.choice(SENSOR_TYPES)
            reading = make_reading(sensor)
            try:
                meta = producer.send(TOPIC, key=sensor, value=reading).get(timeout=10)
                sent += 1
                i    += 1
                if sent % 50 == 0 or (not loop and i == count):
                    print(f"  [{sent:>5}] {sensor:<12} = {reading['value']:>8.2f} {reading['unit']}  "
                          f"P{meta.partition} O{meta.offset}")
            except KafkaTimeoutError as e:
                print(f"  TIMEOUT: {e}")
            time.sleep(delay)
    except KeyboardInterrupt:
        print("\n⚡  Interrupted.")
    finally:
        producer.flush()
        producer.close()
        print(f"\n✅  Sent {sent} messages to '{TOPIC}'.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Session 4 Kafka producer")
    p.add_argument("--count",  type=int,   default=500,  help="Messages to send (default 500)")
    p.add_argument("--delay",  type=float, default=0.05, help="Seconds between messages (default 0.05)")
    p.add_argument("--loop",   action="store_true",      help="Produce indefinitely")
    args = p.parse_args()
    main(args.count, args.delay, args.loop)
