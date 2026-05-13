# Session 2 – Kafka and Distributed Messaging
## Python Streaming Pipeline Lab

> **Big Data Engineering Programme · Session 2 of 7 · Duration: ~90 minutes**
>
> **Prerequisite:** Session 1 completed. The 3-broker Kafka cluster must be running.

---

## 📁 Project Structure

```
session2-kafka-pipeline/
├── docker-compose.yml                  # Same 3-broker KRaft cluster as Session 1
├── requirements.txt                    # kafka-python-ng>=2.0.2  confluent-kafka>=2.0.0
├── NOTES.md                            # Bug fixes, Windows notes, verified test results
├── scripts/
│   ├── setup_topic.sh                  # Create sensor-events topic
│   ├── rebalance_demo.sh               # Launch multiple consumers → trigger rebalance
│   ├── induce_lag.sh                   # Produce messages with no consumer → show lag
│   └── reset_offsets.sh                # Replay from start or skip to latest
└── python/
    ├── producer.py                     # ★ Sensor producer (kafka-python-ng, acks=all)
    ├── consumer.py                     # ★ Manual-commit consumer (confluent-kafka)
    ├── consumer_batch.py               # ★ Batch-commit consumer (confluent-kafka)
    └── lag_monitor.py                  # ★ CLI consumer lag dashboard (confluent-kafka)

★ = modified from original
```

---

## 🧰 Prerequisites

| Tool | Version | Check |
|---|---|---|
| Docker Desktop | 20.10+ | `docker --version` |
| Docker Compose | v2.0+ | `docker compose version` |
| Python | 3.10+ | `python --version` |
| kafka-python-ng | ≥ 2.0.2 | `pip install kafka-python-ng` |
| confluent-kafka | ≥ 2.0.0 | `pip install confluent-kafka` |

> **Python 3.12+?** The original `kafka-python==2.0.2` crashes with
> `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'` on Python 3.12+.
> The fixed scripts use `kafka-python-ng` (producer) and `confluent-kafka`
> (consumers + lag monitor), which work on Python 3.10 through 3.14.

> **No Java or Hadoop required.** Session 2 is pure Python — no PySpark involved.

---

## 🚀 Quick Start

### Linux / macOS

```bash
# 1. Start the Kafka cluster
docker compose up -d
docker compose ps    # all 4 containers Up

# 2. Create the virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Create the topic
chmod +x scripts/*.sh
./scripts/setup_topic.sh

# 5. Open Kafka UI
open http://localhost:8080

# 6. Terminal 1 – produce messages
python python/producer.py --count 50

# 7. Terminal 2 – consume
python python/consumer.py
```

### Windows

```bash
# 1. Start the Kafka cluster
docker compose up -d && docker ps

# 2. Install dependencies (no venv needed if using system Python)
pip install kafka-python-ng confluent-kafka

# 3. Create the topic
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create --topic sensor-events --partitions 3 --replication-factor 3

# 4. Open Kafka UI → http://localhost:8080

# 5. Terminal 1 – produce messages
python python/producer.py --count 50

# 6. Terminal 2 – consume
python python/consumer.py

# 7. Terminal 3 – monitor lag
python python/lag_monitor.py
```

---

## 🔬 Lab Steps — Detailed

### Step 0 — Environment Setup

#### 0a — Start the cluster

```bash
docker compose up -d
docker compose ps    # Expected: all 4 containers → Status: Up
```

If you're using the cluster from Session 1, you can reuse it directly — no need
to start a new one.

#### 0b — Create the virtual environment (Linux/macOS)

```bash
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

#### 0c — Create the topic

```bash
# Linux/macOS
./scripts/setup_topic.sh

# Windows (or manually on any platform)
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create --topic sensor-events \
  --partitions 3 --replication-factor 3

docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events
```

**Why 3 partitions?** There are 3 sensor types (temperature, humidity, pressure).
Using `key=sensor_type`, Kafka routes all readings of the same type to the same
partition. This ensures per-sensor-type ordering.

---

### Step 1 — Run the Producer

```bash
# Terminal 1
python python/producer.py
```

Default: sends 50 messages. To send a different number:

```bash
python python/producer.py --count 100
```

**What to observe:**
- Each line shows `P{partition} O{offset}` — verify that `temperature` always
  lands on the same partition, same for `humidity` and `pressure`.
- Temperatures above 35 °C are flagged with `⚠ HIGH TEMP`.
- The producer uses `acks='all'` + `retries=5` → **at-least-once writes**.

**Key producer config explained:**

| Parameter | Value | Why |
|---|---|---|
| `acks` | `'all'` | Wait for all ISR replicas before confirming |
| `retries` | `5` | Retry on transient broker errors |
| `linger_ms` | `10` | Wait 10 ms to fill a batch → better throughput |
| `batch_size` | `32768` | 32 KB max batch per partition |
| `compression_type` | `'gzip'` | Reduce network overhead |

> **Note:** `enable_idempotence=True` is not supported by `kafka-python-ng`.
> `acks='all'` + `retries` provides at-least-once durability. For exactly-once
> semantics, use `confluent-kafka` with transactional producers.

---

### Step 2 — Run the Consumer

```bash
# Terminal 2 (keep producer running in Terminal 1)
python python/consumer.py
```

**What to observe:**
- Messages appear in real time as the producer sends them.
- All `temperature` readings appear on the same partition.
- Offsets are strictly increasing within each partition.
- Temperatures > 35 °C trigger the `⚠ ALERT` message.
- The consumer prints its assigned partitions at startup.

**Key consumer config explained (`confluent-kafka` dot-notation):**

| Parameter | Value | Why |
|---|---|---|
| `'group.id'` | `'sensor-analytics'` | Identifies the consumer group |
| `'auto.offset.reset'` | `'earliest'` | On first run, replay all messages |
| `'enable.auto.commit'` | `False` | We commit manually after each message |
| `'session.timeout.ms'` | `45000` | Consumer considered dead after 45 s |
| `'heartbeat.interval.ms'` | `15000` | Heartbeat every 15 s |
| `'fetch.min.bytes'` | `1` | Return immediately even for 1 byte |

---

### Step 3 — Run the Pipeline End-to-End

For the most instructive experience, run both scripts **simultaneously**:

```bash
# Terminal 1: consumer (start first to see messages arrive in real time)
python python/consumer.py

# Terminal 2: producer
python python/producer.py
```

You should see messages appear in Terminal 1 as the producer sends them.

---

### Step 4 — Consumer Group Scaling Exercise

#### 4a — Start a second consumer instance

Without stopping the first consumer, open a **third terminal**:

```bash
# Terminal 3 (same group.id → triggers rebalance)
python python/consumer.py
```

Within a few seconds, Kafka triggers a **rebalance**. Both consumers belong
to `sensor-analytics`, so Kafka redistributes the 3 partitions between them.
Each terminal will now only receive messages from its assigned partitions.

**Expected result:**
```
Terminal 1:  Assigned partitions: [0, 1]
Terminal 2:  Assigned partitions: [2]
```

(exact assignment depends on Kafka's range assignor)

#### 4b — Maximum parallel consumers

With 3 partitions, only 3 consumers can work in parallel. A 4th consumer
would be assigned **no partitions** and sit idle. This is the horizontal
scaling ceiling for this topic.

#### 4c — Observe rebalance on exit

Stop one consumer with `Ctrl+C`. After `session.timeout.ms` (45 s), the
Group Coordinator detects the departure and triggers a new rebalance.
The remaining consumer will inherit all 3 partitions.

---

### Step 5 — Fault Injection: Crash and Replay (At-Least-Once)

This demonstrates why manual offset commit matters.

#### 5a — Produce more messages

```bash
python python/producer.py --count 20
```

#### 5b — Start the consumer and crash it immediately

```bash
python python/consumer.py
# Wait for 3-4 messages to print, then press Ctrl+C QUICKLY
```

Because `enable.auto.commit=False` and we call `consumer.commit(msg)` **after**
processing, pressing Ctrl+C before the commit call means those messages
were processed but their offsets were NOT saved to Kafka.

#### 5c — Restart and observe replay

```bash
python python/consumer.py
```

The consumer fetches its last committed offset from `__consumer_offsets` and
resumes from there. You will see the same messages that were printed just
before the crash **reappear** — this is **at-least-once delivery** in action.

> **Key insight:** to safely handle replays, your business logic should be
> **idempotent** — processing the same message twice produces the same result
> as processing it once (e.g., database upsert instead of insert).

---

### Step 6 — Monitor Consumer Lag

#### Option A — Kafka UI (visual)

Open **http://localhost:8080** → Consumer Groups → **sensor-analytics**

| Column | Meaning |
|---|---|
| **LAG** | Messages produced but not yet consumed. Healthy = 0 |
| **OFFSET** | Last committed offset for this partition in this group |
| **END OFFSET** | Latest message offset (high-water mark) |
| **CONSUMER ID** | Which consumer instance owns this partition |

#### Option B — CLI lag monitor

```bash
python python/lag_monitor.py
# Refreshes every 3 s; Ctrl+C to stop
python python/lag_monitor.py --interval 1   # refresh every 1 s
```

#### Option C — Kafka CLI

```bash
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:29092 \
  --describe --group sensor-analytics
```

#### Induce Lag Experiment

```bash
# Step 1: Stop all consumers
# Step 2: Produce 100 messages
python python/producer.py --count 100
# Step 3: Check Kafka UI → lag ≈ 100 per partition
# Step 4: Restart consumer → watch lag drop to 0
python python/consumer.py
```

#### Induce lag with the helper script:

```bash
./scripts/induce_lag.sh
```

---

### Step 7 — Advanced: Batch Consumer

```bash
python python/consumer_batch.py              # default: batch=50
python python/consumer_batch.py --batch 10  # smaller batches
```

Instead of committing after every message, this consumer:
1. Polls up to N records with `consumer.consume(num_messages=N, timeout=1.0)`
2. Processes the whole batch
3. Computes per-sensor aggregates (min / avg / max)
4. Commits **once** per batch with `consumer.commit()`

This is significantly faster for high-throughput scenarios at the cost of
potentially reprocessing up to `batch_size` messages on crash.

---

### Step 8 — Reset Offsets (Replay or Skip)

> ⚠️ Stop all consumers before resetting offsets.

```bash
./scripts/reset_offsets.sh
# Choose: 1 = replay from beginning, 2 = skip to latest
```

Or manually:

```bash
# Replay all messages from the beginning
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:29092 \
  --group sensor-analytics \
  --topic sensor-events \
  --reset-offsets --to-earliest --execute

# Skip all current messages (only new ones)
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:29092 \
  --group sensor-analytics \
  --topic sensor-events \
  --reset-offsets --to-latest --execute
```

---

## 📊 Testing Checklist

```
Environment
  [ ] docker compose ps → all 4 containers Up
  [ ] pip install kafka-python-ng confluent-kafka → no errors
  [ ] sensor-events topic: 3 partitions, RF=3, ISR=3

Producer
  [ ] python/producer.py sends 50 messages without errors
  [ ] temperature, humidity, pressure each land on a consistent partition
  [ ] Temperatures > 35 °C are flagged in output
  [ ] After exit, "Sent: N | Errors: 0" is printed (flush completed)

Consumer (per-message commit)
  [ ] python/consumer.py prints partition assignment on startup
  [ ] Messages appear with P/O metadata
  [ ] High-temp alerts appear correctly (value > 35 °C)
  [ ] Clean shutdown: messages processed, elapsed, throughput printed

Consumer Group & Rebalance
  [ ] Running 2 instances → each terminal shows different partitions
  [ ] After stopping one → the other inherits all 3 partitions
  [ ] Running 4 instances → 4th consumer is assigned [] (no partitions)

Fault Injection (At-Least-Once)
  [ ] Crash consumer before commit → messages reappear on restart
  [ ] Restart consumer → resumes from last committed offset (not from 0)

Lag Monitoring
  [ ] python/lag_monitor.py shows per-partition lag table
  [ ] After producing 100 messages with no consumer → LAG ≈ 100
  [ ] After starting consumer → LAG drops to 0
  [ ] Kafka UI → Consumer Groups → sensor-analytics shows same data

Batch Consumer (advanced)
  [ ] python/consumer_batch.py shows per-batch aggregates (count/min/avg/max)
  [ ] Commits once per batch (not per message)
  [ ] Throughput higher than per-message consumer

Offset Reset
  [ ] --to-earliest → consumer replays all messages
  [ ] --to-latest   → consumer only sees new messages
```

---

## 🧠 Delivery Semantics Summary

| Semantic | How to achieve | Risk |
|---|---|---|
| **At-most-once** | Auto-commit before processing | Messages can be lost on crash |
| **At-least-once** | Manual commit AFTER processing | Messages can be reprocessed |
| **Exactly-once** | Idempotent producer + transactional consumer | Highest complexity |

The lab uses **at-least-once** (safest for most pipelines when combined with
idempotent business logic).

---

## 🧠 Reflection Questions

1. You used `key=sensor_type` in the producer. What would happen to message
   ordering if you had used `key=None` instead?
2. With `enable.auto.commit=False`, what are the exact conditions under which a
   message could be processed **twice**? What about **zero** times?
3. You have a topic with 6 partitions and a consumer group with 8 consumers.
   How many consumers are idle? What is the maximum useful parallelism?
4. Explain the difference between `auto.offset.reset='earliest'` and `'latest'`.
   In which situations would you choose each?
5. Your producer's throughput is 10 000 msg/s but the broker acknowledges only
   2 000 msg/s. Which parameters would you tune while keeping `acks='all'`?

---

## 📖 Vocabulary Checklist

Before Session 3, make sure you can explain:

`append-only log` · `offset` · `message key / partition routing` · `acks (0, 1, all)` ·
`idempotent producer` · `batch_size / linger_ms` · `enable.auto.commit` ·
`manual offset commit` · `at-most-once / at-least-once / exactly-once` ·
`consumer group / group.id` · `rebalance / Group Coordinator` ·
`ISR / min.insync.replicas` · `consumer lag` · `auto.offset.reset`

---

## ➡️ Preview: Session 3

Next session — ETL Pipelines:
- Batch vs streaming ETL: concepts and trade-offs
- Integration with relational databases and cloud storage
- Introduction to Apache Spark and Spark Streaming
- **Lab:** Design a complete ETL pipeline using Kafka + Spark

---

## 🔗 Further Reading

- [confluent-kafka Python client documentation](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [kafka-python-ng documentation](https://kafka-python.readthedocs.io)
- [Confluent – Kafka Producers Deep Dive](https://developer.confluent.io/learn-kafka/apache-kafka/producers/)
- [Apache Kafka – Consumer Group Protocol](https://kafka.apache.org/documentation/#impl_consumer)
- Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly. Chapter 11.
- Narkhede, N. et al. (2021). *Kafka: The Definitive Guide*, 2nd ed. O'Reilly. Chapters 3–4.

---

*Course material – Big Data Engineering Programme 2024–2025*  
*Updated 2026-04-28: kafka-python==2.0.2 → kafka-python-ng + confluent-kafka; Python 3.14 compatibility fixes*
