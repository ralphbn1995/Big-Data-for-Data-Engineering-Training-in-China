"""
kafka_utils.py – Session 5: Kafka helper functions for the REST API
====================================================================
Provides two functions used by the Flask API:

  get_latest_readings(sensor_type, n)
    → Consumes the last N messages from `sensor-events` for a given
      sensor type using a temporary Kafka consumer (confluent-kafka).

  publish_reading(reading_dict)
    → Publishes one reading dict to `sensor-events` and returns
      the partition + offset metadata (kafka-python-ng).

Design note: We create a fresh consumer per request to keep the API
stateless. The producer is a singleton reused across requests. In
production you would use a background consumer writing to a Redis cache.
"""

import json
import time
import threading
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaProducer                      # kafka-python-ng (producer)
from kafka.errors import KafkaError, KafkaTimeoutError
from confluent_kafka import Consumer as CKConsumer   # confluent-kafka (consumer)
from confluent_kafka import TopicPartition as CKTP

# ── Configuration ─────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9094", "localhost:9096"]
TOPIC             = "sensor-events"
CONSUMER_TIMEOUT  = 3000   # ms to wait for messages between polls


# ── Singleton producer (created once, reused across requests) ─────────────────
_producer: Optional[KafkaProducer] = None
_producer_lock = threading.Lock()


def _get_producer() -> KafkaProducer:
    """Return a singleton KafkaProducer, creating it on first call."""
    global _producer
    if _producer is None:
        with _producer_lock:
            if _producer is None:
                _producer = KafkaProducer(
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    acks="all",
                    retries=3,
                    # Note: enable_idempotence not supported in kafka-python-ng;
                    # acks='all' + retries provides at-least-once durability.
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8"),
                    request_timeout_ms=10_000,
                )
    return _producer


# ── publish_reading ────────────────────────────────────────────────────────────
def publish_reading(reading: dict) -> dict:
    """
    Publish one sensor reading to Kafka.

    Args:
        reading: dict with keys sensor, value, unit, timestamp, source

    Returns:
        dict with keys partition, offset, topic

    Raises:
        KafkaError on publish failure
    """
    producer = _get_producer()
    sensor   = reading["sensor"]

    try:
        future = producer.send(TOPIC, key=sensor, value=reading)
        meta   = future.get(timeout=10)          # block until ack
        producer.flush()
        return {
            "topic":     meta.topic,
            "partition": meta.partition,
            "offset":    meta.offset,
        }
    except KafkaTimeoutError as exc:
        raise KafkaError(f"Kafka publish timed out: {exc}") from exc


# ── get_latest_readings ────────────────────────────────────────────────────────
def get_latest_readings(sensor_type: str, n: int = 5) -> list[dict]:
    """
    Fetch the N most recent messages for a given sensor type from Kafka.

    Strategy:
      1. Create a temporary consumer (confluent-kafka) with a unique group ID.
      2. Discover all partitions via list_topics().
      3. Get the high-water mark offset per partition.
      4. Seek to max(low, end - seek_back) on each partition.
      5. Poll until CONSUMER_TIMEOUT ms of idle silence, collect matching records.
      6. Return the N most recent ones sorted by timestamp descending.

    Args:
        sensor_type: e.g. "temperature"
        n: number of recent messages to return

    Returns:
        list of dicts (may be fewer than n if not enough data)
    """
    group_id = f"api-latest-{sensor_type}-{int(time.time() * 1000)}"

    consumer = CKConsumer({
        'bootstrap.servers': ','.join(BOOTSTRAP_SERVERS),
        'group.id':          group_id,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False,
    })

    results = []

    try:
        # Step 1: discover partitions via topic metadata
        meta = consumer.list_topics(TOPIC, timeout=5)
        if TOPIC not in meta.topics or meta.topics[TOPIC].error:
            return []

        partitions = sorted(meta.topics[TOPIC].partitions.keys())
        if not partitions:
            return []

        # Step 2: build seek positions — seek backwards by seek_back per partition
        seek_back = max(n * 3, 30)   # over-fetch to find n of the correct type
        tps = []
        for p in partitions:
            low, high = consumer.get_watermark_offsets(CKTP(TOPIC, p), timeout=5)
            if high == 0:
                continue
            start = max(low, high - seek_back)
            tps.append(CKTP(TOPIC, p, start))   # offset in TopicPartition → auto-seek

        if not tps:
            return []

        # Step 3: assign and seek in one call
        consumer.assign(tps)

        # Step 4: poll until CONSUMER_TIMEOUT ms of silence
        idle_secs = CONSUMER_TIMEOUT / 1000
        last_msg_time = time.time()

        while time.time() - last_msg_time < idle_secs:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                continue

            last_msg_time = time.time()

            try:
                record = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue

            if not isinstance(record, dict):
                continue

            if record.get("sensor") == sensor_type:
                results.append({
                    "sensor_type": record.get("sensor"),
                    "value":       record.get("value"),
                    "unit":        record.get("unit", ""),
                    "device_id":   record.get("device_id", ""),
                    "timestamp":   record.get("timestamp"),
                    "event_time":  _ms_to_iso(record.get("timestamp")),
                    "partition":   msg.partition(),
                    "offset":      msg.offset(),
                })

    finally:
        consumer.close()

    # Sort by timestamp descending and return the n most recent
    results.sort(key=lambda r: r.get("timestamp") or 0, reverse=True)
    return results[:n]


# ── Helpers ───────────────────────────────────────────────────────────────────
def _ms_to_iso(epoch_ms: Optional[int]) -> Optional[str]:
    """Convert epoch milliseconds to ISO 8601 UTC string."""
    if epoch_ms is None:
        return None
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, OSError, OverflowError):
        return None
