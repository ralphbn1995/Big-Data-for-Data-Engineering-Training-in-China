# Session 1 – Introduction to Data Engineering & Distributed Architectures
## Local 3-Broker Kafka Cluster Lab

> **Big Data Engineering Programme · Session 1 of 7 · Duration: ~90 minutes**

---

## 📁 Project Structure

```
session1-kafka-lab/
├── docker-compose.yml              # 3 Kafka brokers (KRaft) + Kafka UI
├── requirements.txt                # kafka-python-ng>=2.0.2  confluent-kafka>=2.0.0
├── NOTES.md                        # Bug fixes, compatibility notes, verified test results
├── scripts/
│   ├── start_cluster.sh            # Launch cluster + wait for readiness
│   ├── setup_topic.sh              # Create session1-events topic
│   ├── fault_tolerance_demo.sh     # Step-by-step crash/recovery demo (CLI)
│   └── teardown.sh                 # Stop and optionally wipe volumes
└── python/
    ├── producer.py                 # ★ Python producer (kafka-python-ng, 20 user events)
    ├── consumer.py                 # ★ Python consumer (confluent-kafka, analytics-group)
    └── fault_tolerance_test.py     # ★ Automated fault-tolerance test

★ = modified from original
```

---

## 🧰 Prerequisites

| Tool | Minimum version | Check |
|---|---|---|
| Docker Desktop | 20.10+ | `docker --version` |
| Docker Compose | v2.0+ (plugin) | `docker compose version` |
| Python *(optional)* | 3.10+ | `python --version` |
| kafka-python-ng *(optional)* | ≥ 2.0.2 | `pip install kafka-python-ng` |
| confluent-kafka *(optional)* | ≥ 2.0.0 | `pip install confluent-kafka` |

> **No Java installation required.** All Kafka CLI tools run inside the Docker containers.

> **Python 3.12+?** The original `kafka-python==2.0.2` crashes with
> `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'` on Python 3.12+.
> The fixed scripts use `kafka-python-ng` (producer) and `confluent-kafka` (consumers),
> which work on Python 3.10 through 3.14. Python scripts are optional for this lab —
> the core exercises use Docker and Kafka CLI tools.

---

## 🚀 Quick Start (5 steps)

### Linux / macOS

```bash
# 1. Make scripts executable
chmod +x scripts/*.sh

# 2. Start the cluster (pulls images on first run, ~2 min)
./scripts/start_cluster.sh

# 3. Create the topic
./scripts/setup_topic.sh

# 4. Open Kafka UI
open http://localhost:8080          # macOS
# xdg-open http://localhost:8080   # Linux

# 5. (Optional) Install Python dependencies
pip install -r requirements.txt
```

### Windows

```bash
# 1. Start the cluster
docker compose up -d
docker compose ps    # wait until all 4 containers show "Up (healthy)"

# 2. Create the topic
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 \
  --create --topic session1-events \
  --partitions 3 --replication-factor 3

# 3. Open Kafka UI → http://localhost:8080

# 4. (Optional) Install Python dependencies
pip install kafka-python-ng confluent-kafka
```

---

## 📦 Architecture Overview

```
 ┌─────────────────────────────────────────────────────────┐
 │                     Docker Network                       │
 │                                                          │
 │  kafka1 ──── :29092 (internal) ──┐                       │
 │  kafka2 ──── :29092 (internal) ──┼── kafka-ui :8080     │
 │  kafka3 ──── :29092 (internal) ──┘                       │
 │                                                          │
 │  kafka1 controller port: :9093 (Raft quorum)             │
 │  kafka2 controller port: :9093                           │
 │  kafka3 controller port: :9093                           │
 └─────────────────────────────────────────────────────────┘
        │              │              │
   host:9092      host:9094      host:9096
 (external)     (external)     (external)
```

| Container | External Port | Role |
|---|---|---|
| kafka1 | 9092 | Broker + Controller (Raft) |
| kafka2 | 9094 | Broker + Controller (Raft) |
| kafka3 | 9096 | Broker + Controller (Raft) |
| kafka-ui | 8080 | Web dashboard |

All brokers use **KRaft mode** (no ZooKeeper). They elect a leader controller
among themselves using the Raft consensus algorithm.

---

## 🔬 Lab Steps — Detailed

### Step 0 — Verify Prerequisites

```bash
docker --version
# Expected: Docker version 20.10.x or higher

docker compose version
# Expected: Docker Compose version v2.x.x
```

---

### Step 1 — Start the Cluster

```bash
docker compose up -d
```

Wait ~30 seconds, then verify all containers are healthy:

```bash
docker compose ps
# Expected: all 4 containers → Status: running / Up
```

Check that Kafka has fully started:

```bash
docker logs kafka1 2>&1 | grep "started"
# Expected line: KafkaServer id=1 started
```

Open the web UI: **http://localhost:8080** — you should see **local-cluster** with
3 brokers.

---

### Step 2 — Enter a Broker Container

All CLI commands below run **inside** the `kafka1` container:

```bash
docker exec -it kafka1 bash
# You are now inside the container shell
# Kafka CLI tools are at /usr/bin/kafka-*
```

---

### Step 3 — Create and Inspect the Topic

```bash
# Inside kafka1 container:
kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create \
  --topic session1-events \
  --partitions 3 \
  --replication-factor 3
```

Inspect the result (leaders + ISR per partition):

```bash
kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic session1-events
```

Expected output format:

```
Topic: session1-events  Partition: 0  Leader: 2  Replicas: 2,1,3  Isr: 2,1,3
Topic: session1-events  Partition: 1  Leader: 3  Replicas: 3,2,1  Isr: 3,2,1
Topic: session1-events  Partition: 2  Leader: 1  Replicas: 1,3,2  Isr: 1,3,2
```

**Reading the output:**
- `Leader: 2` → all reads/writes for Partition 0 go through Broker 2
- `Replicas: 2,1,3` → 3 copies exist (one per broker)
- `Isr: 2,1,3` → all 3 replicas are fully in-sync (no lag)

---

### Step 4 — Produce Messages (CLI)

```bash
# Inside kafka1 container (Terminal 1):
kafka-console-producer \
  --bootstrap-server kafka1:29092 \
  --topic session1-events \
  --property "parse.key=true" \
  --property "key.separator=:"
```

The `>` prompt appears. Type the following messages, pressing **Enter** after each:

```
user1:{"event":"login","ts":"2024-01-01T10:00:00Z"}
user2:{"event":"purchase","item":"laptop","amount":1299}
user1:{"event":"logout","ts":"2024-01-01T10:05:00Z"}
```

Press `Ctrl+C` to exit the producer.

> **Why keys matter:** Both `user1` messages have the same key, so they are always
> routed to the same partition in order. A consumer reading that partition will always
> see `login` before `logout`.

---

### Step 5 — Consume Messages (CLI)

Open a **second terminal** and connect to the cluster:

```bash
# Terminal 2 on your host:
docker exec -it kafka1 bash

# Inside the container:
kafka-console-consumer \
  --bootstrap-server kafka1:29092 \
  --topic session1-events \
  --from-beginning \
  --property "print.key=true" \
  --property "print.offset=true" \
  --property "print.partition=true"
```

You should see all previously produced messages with their partition/offset metadata.

#### Consumer Groups

Stop the consumer (`Ctrl+C`), then run it with a named group:

```bash
kafka-console-consumer \
  --bootstrap-server kafka1:29092 \
  --topic session1-events \
  --group analytics-group \
  --from-beginning
```

Check the consumer group lag:

```bash
kafka-consumer-groups \
  --bootstrap-server kafka1:29092 \
  --describe \
  --group analytics-group
```

The **LAG** column = messages waiting to be read. `0` means fully caught up.

---

### Step 6 — Fault Tolerance Demo

> This is the most important exercise. You will crash one broker and observe
> automatic leader re-election.

#### 6a — Record the current state

```bash
# Inside kafka1 container:
kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe \
  --topic session1-events
```

Write down which broker is **Leader** for each of the 3 partitions.

#### 6b — Stop Broker 1

```bash
# In a NEW terminal on your HOST machine (outside the container):
docker stop kafka1
```

#### 6c — Observe re-election (within 5–10 seconds)

```bash
# Connect to kafka2 instead:
docker exec -it kafka2 bash

# Inside kafka2:
kafka-topics \
  --bootstrap-server kafka2:29092 \
  --describe \
  --topic session1-events
```

**What to look for:**
- Partitions previously led by Broker 1 now show a different leader (2 or 3)
- `Isr:` shows only 2 brokers (Broker 1 is offline)
- The cluster is still fully operational ✅

#### 6d — Verify data is still readable

```bash
# Inside kafka2:
kafka-console-consumer \
  --bootstrap-server kafka2:29092 \
  --topic session1-events \
  --from-beginning
```

All messages are still accessible because replication-factor=3 means each message
exists on all 3 brokers.

#### 6e — Restart Broker 1

```bash
# Host terminal:
docker start kafka1
```

Wait ~15 seconds, then run `--describe` again and verify Broker 1 is back in the ISR.

---

### Step 7 — Python Scripts (optional)

Install dependencies:

```bash
pip install -r requirements.txt
# or manually: pip install kafka-python-ng confluent-kafka
```

**Producer** (sends 20 random events with user keys):

```bash
python python/producer.py
```

Expected output (key → partition routing is consistent):
```
[01/20] key=user1  event=login
  ✔  topic=session1-events  partition=0  offset=4
[02/20] key=user3  event=page_view
  ✔  topic=session1-events  partition=2  offset=7
...
✅  All messages sent. Producer closed.
```

**Consumer** (reads from `analytics-group`, prints partition + offset):

```bash
python python/consumer.py
# Press Ctrl+C to stop
```

Expected output:
```
  partition= 0  offset=     4  key=user1       event=login           ts=2026-04-28T…
  partition= 2  offset=     7  key=user3       event=page_view       ts=2026-04-28T…
  partition= 1  offset=    12  key=user5       event=logout          ts=2026-04-28T…
```

**Automated fault-tolerance test** (runs all 3 phases automatically, invokes
`docker stop`/`start` internally):

```bash
python python/fault_tolerance_test.py
```

Expected output:
```
  Messages expected : 15
  Messages received : 15
  ✅  PASS – No messages lost during broker failure!
```

> **Note:** If you run the test multiple times, the message count will be higher
> than 15 (the topic accumulates messages). The script checks `received >= expected`
> so it passes correctly either way.

---

### Step 8 — Explore Kafka UI

Navigate to **http://localhost:8080** and explore:

| Section | What to Look For |
|---|---|
| **Brokers** | 3 nodes; identify the active controller (star icon) |
| **Topics** | `session1-events` with 3 partitions and replication map |
| **Messages** | Browse individual records; filter by partition or offset |
| **Consumer Groups** | `analytics-group`; lag per partition |

---

### Step 9 — Teardown

```bash
./scripts/teardown.sh
# Choose option 1 (keep data) or 2 (full wipe)
```

Or manually:

```bash
# Stop containers, keep volumes (restart fast):
docker compose down

# Stop containers AND delete all data:
docker compose down -v
```

---

## 📊 Testing Checklist

```
Prerequisites
  [ ] docker --version → 20.10+
  [ ] docker compose version → v2+

Cluster
  [ ] docker compose ps → all 4 containers Up (healthy)
  [ ] docker logs kafka1 | grep "started" → KafkaServer id=1 started
  [ ] http://localhost:8080 → local-cluster with 3 brokers

Topic
  [ ] kafka-topics --create → "Created topic session1-events."
  [ ] kafka-topics --describe → 3 partitions, Replicas: 3, Isr: 3

Produce / Consume
  [ ] Producer accepts messages at > prompt
  [ ] Consumer receives all messages --from-beginning
  [ ] Both user1 events appear on the SAME partition
  [ ] kafka-consumer-groups --describe → LAG = 0 after consuming

Fault Tolerance
  [ ] docker stop kafka1 → containers still: kafka2, kafka3, kafka-ui
  [ ] kafka-topics --describe (via kafka2) → new leaders visible, ISR = 2
  [ ] Consumer via kafka2 --from-beginning → all messages still present
  [ ] docker start kafka1 → Broker 1 rejoins ISR within 30 s

Python (optional)
  [ ] pip install kafka-python-ng confluent-kafka → no errors
  [ ] python/producer.py → 20 messages sent, all ✔, consistent key routing
  [ ] python/consumer.py → messages displayed with partition + offset + event
  [ ] python/fault_tolerance_test.py → PASS, received ≥ 15 messages
```

---

## 🧠 Reflection Questions

Answer these before Session 2:

1. After stopping Broker 1, which brokers became the new leaders for each partition?
   Does this match the Replicas list in the initial `--describe` output?
2. What would happen if you had set `replication-factor 1` before stopping the
   only broker holding a partition?
3. How does `MIN_INSYNC_REPLICAS: 2` protect against data loss? What happens if a
   third broker also goes down while Broker 1 is still stopped?
4. Why does Kafka use key-based routing rather than random routing for all messages?
5. What is the difference between consumer offset tracking and simply re-reading
   from `--from-beginning` every time?

---

## 📖 Key Concepts Summary

| Term | Definition |
|---|---|
| **Broker** | A Kafka server process that stores and serves partitions |
| **Topic** | A named, ordered, immutable log of events |
| **Partition** | An ordered sub-sequence of a topic; unit of parallelism |
| **Offset** | Integer identifying a record's position within a partition |
| **Producer** | Client that writes events to topics |
| **Consumer** | Client that reads events from topics |
| **Consumer Group** | Set of consumers sharing topic consumption |
| **Replication Factor** | Number of partition copies across brokers |
| **Leader** | The broker handling reads/writes for a partition |
| **ISR** | In-Sync Replica: follower fully caught up with the leader |
| **KRaft** | Kafka Raft — built-in consensus replacing ZooKeeper |
| **CAP Theorem** | A distributed system can guarantee only 2 of: Consistency, Availability, Partition Tolerance |

---

## 🔗 Further Reading

- Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly. Chapters 5–6.
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Fundamentals](https://developer.confluent.io/learn-kafka/)
- [confluent-kafka Python client](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- Brewer, E. A. (2000). *Towards robust distributed systems*. PODC Keynote.

---

## ➡️ Preview: Session 2

Next session covers Kafka internals in depth:
- **Producers:** partitioning strategies, `acks`, batching
- **Consumers:** offset management, commit strategies, consumer group rebalancing
- **Message durability:** `acks`, `min.insync.replicas`, `retries`
- **Lab:** Build a complete Kafka streaming pipeline in Python

---

*Course material – Big Data Engineering Programme 2024–2025*  
*Updated 2026-04-28: kafka-python==2.0.2 → kafka-python-ng + confluent-kafka; Python 3.14 compatibility fixes*
