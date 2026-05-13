# Session 2 Kafka Pipeline — Test Notes
**Date:** 2026-04-28  
**Tester:** Ralph Bounader (rbounader60@gmail.com)  
**Environment:** Windows 11 Home 10.0.26200, Python 3.14.2, Docker Desktop 29.4.0

---

## Bugs Found & Fixed

### Bug 1 — `kafka-python==2.0.2` incompatible with Python 3.12+
**All 4 scripts** — `requirements.txt` pinned `kafka-python==2.0.2` which crashes on Python 3.14:
```
ModuleNotFoundError: No module named 'kafka.vendor.six.moves'
```
**Fix:** Updated `requirements.txt` to use `kafka-python-ng>=2.0.2` (maintained fork) for the
producer, and `confluent-kafka>=2.0.0` for all consumers and the lag monitor.

---

### Bug 2 — `enable_idempotence=True` not supported in kafka-python-ng (`producer.py`)
After switching to `kafka-python-ng`, the producer crashed:
```
AssertionError: Unrecognized configs: {'enable_idempotence': True}
```
**Fix:** Removed `enable_idempotence=True` and `max_in_flight_requests_per_connection=1`
(which existed only to support idempotence). The producer still provides strong durability
via `acks='all'` and `retries=5`.

---

### Bug 3 — Consumer crashes with `ValueError: Invalid file descriptor: -1` (Python 3.14)
`consumer.py`, `consumer_batch.py`, `lag_monitor.py` — the `kafka-python-ng` consumer
crashes on Python 3.14 due to a `selectors` module incompatibility:
```
ValueError: Invalid file descriptor: -1
  File "kafka/client_async.py", line 640, in _poll
    self._selector.unregister(key.fileobj)
```
**Fix:** Rewrote all three consumer scripts to use `confluent-kafka` API, which is
fully Python 3.14 compatible. Business logic, output format, and Kafka semantics
preserved exactly.

---

### Bug 4 — Dead imports in `lag_monitor.py`
`KafkaAdminClient` and `NewTopic` were imported but never used.  
**Fix:** Removed both unused imports.

---

### Bug 5 — `UnicodeEncodeError` on emoji output (all scripts, Windows)
Windows terminal uses cp1252 encoding by default. Emoji characters (`⚠ ✔ ⚡ ✅ ▶ ─`)
caused:
```
UnicodeEncodeError: 'charmap' codec can't encode character '✅'
```
**Fix:** Added `sys.stdout.reconfigure(encoding='utf-8', errors='replace')` at the top
of each script. This forces UTF-8 output on any platform.

---

## Files Changed

| File | Changes |
|---|---|
| `requirements.txt` | `kafka-python==2.0.2` → `kafka-python-ng>=2.0.2` + `confluent-kafka>=2.0.0` |
| `python/producer.py` | Added `sys.stdout.reconfigure`; removed `enable_idempotence` + `max_in_flight_requests_per_connection` |
| `python/consumer.py` | Full rewrite to `confluent-kafka` API; same logic and output |
| `python/consumer_batch.py` | Full rewrite to `confluent-kafka` API using `consumer.consume()` |
| `python/lag_monitor.py` | Full rewrite to `confluent-kafka`; removed dead imports; uses `list_topics`, `committed`, `get_watermark_offsets` |

---

## Test Results

### Prerequisites
| Tool | Version | Status |
|---|---|---|
| Docker Desktop | 29.4.0 | OK |
| Docker Compose | v5.1.1 | OK |
| Python | 3.14.2 | OK (with fixes above) |

### Cluster (reused from session1 — same docker-compose)
| Container | Port | Status |
|---|---|---|
| kafka1 | 9092 | Up (healthy) |
| kafka2 | 9094 | Up (healthy) |
| kafka3 | 9096 | Up (healthy) |
| kafka-ui | 8080 | Up |

### Topic: `sensor-events`
```
Created topic sensor-events.
PartitionCount: 3  ReplicationFactor: 3  min.insync.replicas=2
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

### producer.py — PASS
- Sent 30 messages, all acknowledged (`acks=all`, `retries=5`)
- Partition routing by sensor type confirmed:
  - `humidity`    → Partition 0
  - (none sampled) → Partition 1
  - `temperature` + `pressure` → Partition 2
- HIGH TEMP alert fired correctly at 35.78 °C (threshold 35.0)
- Output: `Sent: 30 | Errors: 0`

### consumer.py — PASS
- Read 30/30 messages from `sensor-events` (group `session2-test-consumer`)
- Assigned all 3 partitions
- HIGH TEMP alert displayed for temperature > 35 °C
- Manual commit after each message working
- Per-partition ordering preserved (all P2 before P0)

### consumer_batch.py — PASS
- Consumed all 60 total messages in batches of 15
- Group `sensor-analytics-batch` ended at LAG=0 on all partitions
- Aggregation (count/min/avg/max per sensor type) computed correctly
- Single `commit()` per batch confirmed via CLI

### lag_monitor.py — PASS
- Connected to cluster without subscribing to topic
- `list_topics()` correctly discovered 3 partitions
- `committed()` returned per-partition committed offsets
- `get_watermark_offsets()` returned correct end offsets
- Sample output (group `sensor-analytics` with partial consumption):
  ```
   Part   Committed  End Offset       LAG
  ─────  ──────────  ──────────  ────────
      0          12          12       ✔ 0
      1           0          19        19
      2          18          29        11
  ──────────────────────────────────────
  Total lag: 30   ->  ⚠  BEHIND by 30 msg
  ```
- Status correctly shows HEALTHY (0) vs BEHIND (>0)

---

## Full Testing Checklist

```
Prerequisites
  [x] docker --version -> 29.4.0
  [x] docker compose version -> v5.1.1
  [x] python --version -> 3.14.2

Dependencies
  [x] pip install kafka-python-ng confluent-kafka -> installed OK

Cluster
  [x] All 4 containers Up and healthy
  [x] http://localhost:8080 -> local-cluster with 3 brokers

Topic
  [x] sensor-events created: 3 partitions, replication-factor 3, ISR full

producer.py
  [x] 30 messages sent, Errors: 0
  [x] Sensor type → same partition every time (key routing)
  [x] HIGH TEMP alert printed for temperature > 35°C
  [x] Partition/offset printed per message

consumer.py
  [x] Partition assignment printed on startup
  [x] All 30 messages consumed from earliest
  [x] HIGH TEMP alert fired
  [x] Manual commit after each message
  [x] Clean shutdown output (messages processed, elapsed, throughput)

consumer_batch.py
  [x] batch_size=15 respected per poll cycle
  [x] Aggregation: count/min/avg/max per sensor type
  [x] Single commit per batch
  [x] group sensor-analytics-batch at LAG=0 after run

lag_monitor.py
  [x] Discovers partitions via list_topics()
  [x] Shows committed offset, end offset, lag per partition
  [x] Total lag and HEALTHY/BEHIND status displayed
  [x] Works without subscribing to topic (metadata-only consumer)
```

---

## Notes for Students

- **Use Python 3.11 or 3.12** if you want to use the original `kafka-python==2.0.2`
  without any changes — it works out of the box on those versions.
- **Python 3.13+**: Use the fixed scripts in this folder (`kafka-python-ng` + `confluent-kafka`).
- The `confluent-kafka` API differences from `kafka-python`:
  - `consumer.poll(timeout=1.0)` returns **one message** (not a dict of lists)
  - `consumer.consume(num_messages=N, timeout=1.0)` returns a **list** of up to N messages
  - Config keys use dots: `'enable.auto.commit'` instead of `enable_auto_commit=`
  - `msg.partition()`, `msg.offset()`, `msg.key()`, `msg.value()` are **methods**, not attributes
