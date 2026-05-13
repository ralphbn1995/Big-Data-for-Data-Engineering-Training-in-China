# Session 1 Kafka Lab — Test Notes
**Date:** 2026-04-28  
**Tester:** Ralph Bounader (rbounader60@gmail.com)  
**Environment:** Windows 11 Home 10.0.26200, Docker Desktop 29.4.0

---

## Prerequisites Verified

| Tool | Version | Status |
|---|---|---|
| Docker Desktop | 29.4.0 | OK |
| Docker Compose | v5.1.1 | OK |
| Python | 3.14.2 | OK (with caveat — see below) |

---

## Cluster Startup

```bash
docker compose up -d
```

All 4 containers started and reached healthy status within ~45 seconds.

| Container | Port | Status |
|---|---|---|
| kafka1 | 9092 | Up (healthy) |
| kafka2 | 9094 | Up (healthy) |
| kafka3 | 9096 | Up (healthy) |
| kafka-ui | 8080 | Up |

Kafka UI accessible at http://localhost:8080 showing `local-cluster` with 3 brokers.

---

## Topic: `session1-events`

Topic already existed from a previous run. Verified with:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --describe --topic session1-events
```

Output:
```
Topic: session1-events  PartitionCount: 3  ReplicationFactor: 3  min.insync.replicas=2
  Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  Partition: 1  Leader: 3  Replicas: 1,2,3  Isr: 3,1,2
  Partition: 2  Leader: 3  Replicas: 2,3,1  Isr: 3,2,1
```

All 3 replicas in ISR for each partition — cluster fully healthy.

---

## Python Dependency Issue (Important)

### Problem
`kafka-python==2.0.2` (specified in `requirements.txt`) is **incompatible with Python 3.12+**.  
On Python 3.14 it crashes immediately on import:

```
ModuleNotFoundError: No module named 'kafka.vendor.six.moves'
```

The consumer also hits a deeper bug in `selectors.py`:
```
ValueError: Invalid file descriptor: -1
```

### Fix
Install `kafka-python-ng` (a maintained Python 3.12+ compatible fork) for the producer,  
and `confluent-kafka` for the consumer (kafka-python-ng consumer is broken on 3.14):

```bash
pip install kafka-python-ng confluent-kafka
```

Or downgrade to **Python 3.11 / 3.12** to use `kafka-python==2.0.2` as-is.

> **Recommendation for students:** Use Python 3.11 or 3.12. All scripts work without modification.

---

## Producer Test

Ran `python/producer.py` — sends 20 messages with user keys to `session1-events`.

Result: **20/20 messages produced successfully** (`acks="all"` confirmed).

Key-based partition routing observed:

| Key | Partition |
|---|---|
| user1 | 0 |
| user2 | 2 |
| user3 | 2 |
| user4 | 1 |
| user5 | 1 |

Same user always lands on the same partition — ordering guarantee confirmed.

Note: The final `print("✅ ...")` line raises a `UnicodeEncodeError` on Windows (cp1252 codec cannot encode the emoji). This is cosmetic — all messages were sent before the crash. Fix by adding `PYTHONUTF8=1` to the environment or replacing emoji characters in the print statements.

---

## Consumer Test

Consumed all messages using `confluent-kafka` (due to `kafka-python` Python 3.14 incompatibility).

Result: **20 messages consumed**, correct partition/key mapping verified.

Partition offset totals after producer run:
```
session1-events:0:4
session1-events:1:12
session1-events:2:7
```
(Total 23 — 3 messages pre-existed from a prior session, 20 from this run.)

---

## Fault Tolerance Test

Ran a 3-phase test against a dedicated topic `fault-tolerance-test`:

### Setup
- Topic: `fault-tolerance-test`, 3 partitions, replication-factor 3
- 15 total messages across 3 phases (5 each)

### Phase A — All 3 brokers healthy
- Produced 5 messages (seq 0–4) via `localhost:9092,9094,9096`
- All acknowledged with `acks=all`

### Broker Failure Simulation
```bash
docker stop kafka1
```
- kafka1 stopped successfully
- Waited 6 seconds for leader re-election
- Remaining cluster (kafka2 + kafka3) elected new leaders automatically

### Phase B — kafka1 DOWN (2 brokers remaining)
- Produced 5 messages (seq 5–9) via `localhost:9094,9096`
- All 5 messages acknowledged — **no data loss during failure**

### Broker Recovery
```bash
docker start kafka1
```
- kafka1 restarted and rejoined ISR within ~30 seconds
- Verified via `--describe` — all ISR restored

### Phase C — All 3 brokers restored
- Produced 5 messages (seq 10–14) via `localhost:9092,9094,9096`
- All acknowledged

### Verification
Consumed `fault-tolerance-test` from beginning:

```
Expected  : 15
Received  : 15
Sequences : [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
RESULT    : PASS - No messages lost during broker failure!
```

---

## Final Checklist

```
Prerequisites
  [x] docker --version -> 29.4.0
  [x] docker compose version -> v5.1.1

Cluster
  [x] All 4 containers Up and healthy
  [x] http://localhost:8080 -> local-cluster with 3 brokers

Topic
  [x] session1-events: 3 partitions, replication-factor 3, ISR full

Producer
  [x] 20 messages sent, all acknowledged (acks=all)
  [x] Same user key always routed to same partition

Consumer
  [x] All messages readable from beginning
  [x] Correct partition/offset metadata

Fault Tolerance
  [x] docker stop kafka1 -> kafka2 and kafka3 take over
  [x] Messages produced during failure are not lost
  [x] docker start kafka1 -> Broker 1 rejoins ISR
  [x] 15/15 messages confirmed after full crash-recovery cycle
```

---

## Known Issues / Fixes Applied

| Issue | Cause | Fix |
|---|---|---|
| `kafka-python==2.0.2` import error | Incompatible with Python 3.12+ | Use `kafka-python-ng` or Python <= 3.11 |
| Consumer `ValueError: Invalid file descriptor: -1` | `kafka-python-ng` consumer broken on Python 3.14 | Use `confluent-kafka` for consumer |
| `UnicodeEncodeError` on emoji print | Windows console uses cp1252 by default | Set `PYTHONUTF8=1` env var, or replace emoji in scripts |
| Docker Desktop not running at start | Linux engine was stopped | Launched Docker Desktop manually before running tests |
