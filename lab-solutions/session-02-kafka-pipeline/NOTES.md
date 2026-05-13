# Session 2 – Kafka Pipeline: Code Review & Run Notes

**Date:** 2026-04-28  

---

## Project Overview

A pure-Python Kafka streaming pipeline that demonstrates:
1. A sensor producer publishing random IoT readings to a 3-partition topic
2. A manual-commit consumer implementing at-least-once delivery
3. A batch-commit consumer for high-throughput patterns
4. A CLI lag monitor showing per-partition consumer group lag

No PySpark or Hadoop involved — pure Python with `kafka-python-ng` (producer)
and `confluent-kafka` (consumers and lag monitor).

---

## Bugs Found and Fixed

### Bug 1 — `requirements.txt`: `kafka-python==2.0.2` incompatible with Python 3.12+
**File:** `requirements.txt`  
**Severity:** Critical (all scripts crash on import)

`kafka-python==2.0.2` depends on `kafka.vendor.six.moves`, an internal vendored module
removed in Python 3.12:

```
ModuleNotFoundError: No module named 'kafka.vendor.six.moves'
```

**Fix:** Replaced with `kafka-python-ng>=2.0.2` (actively maintained fork) for the
producer, and `confluent-kafka>=2.0.0` for consumers and the lag monitor.

```
# BEFORE
kafka-python==2.0.2

# AFTER
kafka-python-ng>=2.0.2   # producer (Python 3.12+ compatible fork)
confluent-kafka>=2.0.0   # consumers and lag monitor (Python 3.14 compatible)
```

---

### Bug 2 — `producer.py`: `enable_idempotence=True` crashes kafka-python-ng
**File:** `python/producer.py`  
**Severity:** High (producer crashes on startup)

After switching to `kafka-python-ng`, the producer failed immediately:

```
AssertionError: Unrecognized configs: {'enable_idempotence': True}
```

`kafka-python-ng` does not implement the idempotent producer protocol
(`enable_idempotence` is a Confluent extension not present in the pure-Python client).

**Fix:** Removed `enable_idempotence=True` and its companion
`max_in_flight_requests_per_connection=1`. The producer still provides strong
durability via `acks='all'` and `retries=5` (at-least-once writes per partition).

```python
# BEFORE — crashes on kafka-python-ng
KafkaProducer(
    acks="all",
    retries=5,
    enable_idempotence=True,
    max_in_flight_requests_per_connection=1,
    ...
)

# AFTER
KafkaProducer(
    acks="all",
    retries=5,
    # enable_idempotence not supported; acks=all + retries gives at-least-once
    ...
)
```

---

### Bug 3 — Consumers crash on Python 3.14 (`ValueError: Invalid file descriptor`)
**Files:** `python/consumer.py`, `python/consumer_batch.py`, `python/lag_monitor.py`  
**Severity:** Critical (every `poll()` call crashes)

Even with `kafka-python-ng`, the consumers failed on every message poll:

```
ValueError: Invalid file descriptor: -1
  File "kafka/client_async.py", line 640, in _poll
    self._selector.unregister(key.fileobj)
```

Root cause: `kafka-python-ng`'s async I/O layer uses the `selectors` module, which
changed its file-descriptor handling in Python 3.14. The pure-Python client cannot
keep up with CPython internals.

**Fix:** Rewrote all three consumer scripts to use `confluent-kafka`, a C-backed
library wrapping `librdkafka`. It is fully compatible with Python 3.14 and is the
production-grade choice for Kafka in Python.

Key `confluent-kafka` API differences from `kafka-python`:

| Aspect | kafka-python | confluent-kafka |
|---|---|---|
| Config style | keyword args with underscores | dict with dot-separated keys |
| `poll()` return | dict of lists | one `Message` object |
| `consume()` | not available | list of up to N messages |
| Attribute access | `msg.partition` | `msg.partition()` (methods) |
| Group membership | `auto_offset_reset=` | `'auto.offset.reset':` |

---

### Bug 4 — `lag_monitor.py`: dead imports
**File:** `python/lag_monitor.py`  
**Severity:** Minor (unused code)

`KafkaAdminClient` and `NewTopic` were imported but never referenced.

**Fix:** Removed both unused imports.

---

### Bug 5 — All scripts: `UnicodeEncodeError` on emoji characters (Windows)
**Files:** `python/producer.py`, `python/consumer.py`, `python/consumer_batch.py`, `python/lag_monitor.py`  
**Severity:** Medium (scripts crash on first emoji print on Windows terminals)

Windows terminal defaults to `cp1252` encoding. Characters like `⚠ ✔ ⚡ ✅ ▶ ─`
caused:

```
UnicodeEncodeError: 'charmap' codec can't encode character '✅' in position 12: ...
```

**Fix:** Added at the top of every script, before any print statement:

```python
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
```

This forces UTF-8 output on any platform without requiring terminal configuration.

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Docker Desktop | 20.10+ | For 3-broker Kafka cluster |
| Python | 3.10+ | 3.12–3.14 all work with fixed scripts |
| kafka-python-ng | ≥ 2.0.2 | Producer only |
| confluent-kafka | ≥ 2.0.0 | Consumers + lag monitor |

> **No Java or Hadoop required.** Session 2 is pure Python — no PySpark involved.

---

## Architecture

| Component | Detail |
|---|---|
| Kafka cluster | 3-broker KRaft (no ZooKeeper), ports 9092 / 9094 / 9096 |
| Topic | `sensor-events`, 3 partitions, RF=3, min.insync.replicas=2 |
| Producer library | `kafka-python-ng`, acks=all, retries=5, linger_ms=10 |
| Consumer library | `confluent-kafka`, enable.auto.commit=False |
| Consumer groups | `sensor-analytics` (per-message), `sensor-analytics-batch` (batch) |

---

## How to Run (Windows)

```bash
# 1. Start Kafka cluster
docker compose up -d && docker ps

# 2. Install dependencies
pip install kafka-python-ng confluent-kafka

# 3. Create topic (if not already created)
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create --topic sensor-events --partitions 3 --replication-factor 3

# 4. Terminal 1 – produce messages
python python/producer.py --count 50

# 5. Terminal 2 – consume (per-message commit, at-least-once)
python python/consumer.py

# 6. Terminal 3 – monitor lag in real time
python python/lag_monitor.py --interval 3

# 7. Advanced – batch consumer
python python/consumer_batch.py --batch 15

# 8. Reset offsets to replay (stop all consumers first)
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:29092 \
  --group sensor-analytics \
  --topic sensor-events \
  --reset-offsets --to-earliest --execute
```

---

## Verified Output (Live Test Run — 2026-04-28)

**Environment:** Windows 11 Home 10.0.26200, Python 3.14.2, Docker Desktop 29.4.0

**producer.py — PASS** (30 messages)
```
Sent: 30  |  Errors: 0
humidity    → P0 (consistent key routing)
temperature → P2 (consistent key routing)
pressure    → P2 (consistent key routing)
HIGH TEMP alerts fired: 35.77 °C, 39.45 °C, 40.94 °C
```

**consumer.py — PASS**
```
Assigned partitions: [0, 1, 2]
Manual commit working after every message
HIGH TEMP alerts displayed correctly
Clean shutdown: messages processed, elapsed, throughput printed
```

**lag_monitor.py — PASS** (3 ticks, group=sensor-analytics)
```
 Part   Committed  End Offset       LAG
─────  ──────────  ──────────  ────────
    0         313         313       ✔ 0
    1          19          19       ✔ 0
    2         578         578       ✔ 0
──────────────────────────────────────
Total lag: 0   →  ✅ HEALTHY
```

**consumer_batch.py — PASS** (batch=15)
```
── Batch #7  (15 records) ──
  pressure      n= 7   min=  982.81  avg= 1013.11  max= 1034.67
  temperature   n= 4   min=   12.12  avg=   24.23  max=   35.77
  humidity      n= 4   min=   30.49  avg=   50.15  max=   80.12
  ✔  Offsets committed (batch #7)
```

---

## Design Notes

1. **`confluent-kafka` is the production-grade Python Kafka library.** `kafka-python`
   and its fork `kafka-python-ng` are pure-Python and brittle against CPython internals
   changes. `confluent-kafka` wraps `librdkafka` (C library, used by the official
   Confluent Platform) and tracks CPython releases closely.

2. **Partition routing by key.** All messages with `key=sensor_type` hash to the same
   partition, guaranteeing per-sensor ordering. With 3 sensor types and 3 partitions,
   each sensor type maps to exactly one partition.

3. **At-least-once semantics.** `consumer.py` calls `consumer.commit(msg)` only after
   `process_record()` completes. A crash between receive and commit causes re-delivery
   on restart. Business logic must be idempotent to handle duplicates safely.

4. **Batch commit trade-off.** `consumer_batch.py` commits once per batch. A crash
   mid-batch causes up to `batch_size` messages to be reprocessed. Tune `batch_size`
   to balance throughput vs. reprocessing exposure.

---

## File Map

```
session2-kafka-pipeline/
├── docker-compose.yml          3-broker KRaft Kafka cluster + Kafka UI (port 8080)
├── requirements.txt      ★     kafka-python-ng>=2.0.2  confluent-kafka>=2.0.0
├── TEST_NOTES.md               Original informal test notes (superseded by this file)
├── NOTES.md                    ← this file
├── scripts/
│   ├── setup_topic.sh          Create sensor-events topic
│   ├── rebalance_demo.sh       Launch multiple consumers → trigger rebalance
│   ├── induce_lag.sh           Produce messages with no consumer → show lag
│   └── reset_offsets.sh        Replay from start or skip to latest
└── python/
    ├── producer.py       ★     Publish sensor events (kafka-python-ng)
    ├── consumer.py       ★     Per-message manual commit (confluent-kafka)
    ├── consumer_batch.py ★     Batch-commit consumer (confluent-kafka)
    └── lag_monitor.py    ★     CLI lag dashboard (confluent-kafka)

★ = modified from original
```
