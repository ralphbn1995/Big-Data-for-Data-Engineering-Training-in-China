# Big Data Engineering Programme

Course materials and lab correction code for the **Big Data Engineering** programme at WUT (Warsaw University of Technology).

The course covers distributed data architectures, streaming pipelines, ETL, and large-scale data processing across 7 sessions.

---

## Repository Structure

```
.
├── course-materials/                        # Lecture slides and math exercises (PDF)
│   ├── session-01-course-and-practical-work.pdf
│   ├── session-01-math-foundations-questions.pdf
│   ├── session-02-course-and-practical-work.pdf
│   ├── session-02-math-foundations-questions.pdf
│   ├── session-03-course-and-practical-work.pdf
│   ├── session-03-math-foundations-questions.pdf
│   ├── session-04-course-and-practical-work.pdf
│   └── session-04-math-foundations-questions.pdf
│
└── lab-solutions/                           # Full corrected code for each lab session
    ├── session-01-kafka-intro/              # Lab 1: Local 3-broker Kafka cluster (KRaft)
    │   ├── docker-compose.yml
    │   ├── requirements.txt
    │   ├── NOTES.md                         # Code review, bug fixes, verified output
    │   ├── python/                          # Producer, consumer, fault-tolerance test
    │   └── scripts/                         # Cluster setup, topic creation, teardown
    │
    ├── session-02-kafka-pipeline/           # Lab 2: Python streaming pipeline
    │   ├── docker-compose.yml
    │   ├── requirements.txt
    │   ├── NOTES.md                         # Code review, bug fixes, verified output
    │   ├── python/                          # Producer, consumers (per-msg & batch), lag monitor
    │   └── scripts/                         # Topic setup, rebalance demo, offset reset
    │
    └── session3-etl-pipeline/              # Lab 3: Kafka + Spark Structured Streaming ETL
        ├── docker-compose.yml
        ├── requirements.txt
        ├── NOTES.md                         # Code review, bug fixes, verified output
        ├── python/                          # Producer, ETL pipeline, batch pipeline, output reader
        └── scripts/                         # Setup, run pipeline, cleanup
```

---

## Sessions Overview

| # | Topic | Course Material | Lab Solution |
|---|-------|-----------------|--------------|
| 1 | Introduction to Data Engineering & Distributed Architectures | PDF | `lab-solutions/session-01-kafka-intro/` |
| 2 | Kafka and Distributed Messaging | PDF | `lab-solutions/session-02-kafka-pipeline/` |
| 3 | ETL Pipelines | PDF | `lab-solutions/session3-etl-pipeline/` |
| 4 | *(course materials available)* | PDF | — |
| 5–7 | *(coming soon)* | — | — |

---

## Lab Solutions

### Session 1 — Kafka Cluster Intro

**Folder:** `lab-solutions/session-01-kafka-intro/`

Full corrected code for the lab that spins up a **3-broker Apache Kafka cluster** in KRaft mode (no ZooKeeper) using Docker. Covers:

- Topic creation with 3 partitions and replication factor 3
- CLI produce/consume with key-based partition routing
- Fault tolerance: crash a broker, observe leader re-election, verify no data loss
- Python producer (`kafka-python-ng`) and consumer (`confluent-kafka`)
- Automated fault-tolerance test

**Quick start (Windows):**
```bash
cd lab-solutions/session-01-kafka-intro
docker compose up -d
# then follow the README inside the folder
```

---

### Session 2 — Python Streaming Pipeline

**Folder:** `lab-solutions/session-02-kafka-pipeline/`

Full corrected code for the lab that builds a complete **sensor data streaming pipeline** in Python. Covers:

- Producer with `acks=all`, batching, and gzip compression
- Per-message manual-commit consumer (at-least-once semantics)
- Batch consumer for high-throughput scenarios
- Consumer group scaling and partition rebalancing
- Real-time consumer lag monitoring dashboard
- Offset reset (replay from beginning / skip to latest)

**Quick start (Windows):**
```bash
cd lab-solutions/session-02-kafka-pipeline
docker compose up -d
pip install kafka-python-ng confluent-kafka
python python/producer.py --count 50   # Terminal 1
python python/consumer.py              # Terminal 2
```

---

### Session 3 — Kafka + Spark ETL Pipeline

**Folder:** `lab-solutions/session3-etl-pipeline/`

Full corrected code for the lab that builds a **Kafka → Spark Structured Streaming → Parquet** ETL pipeline processing sensor telemetry in real time. Covers:

- Kafka producer generating temperature, humidity, and pressure sensor events
- Spark Structured Streaming pipeline with 6 stages: Ingest → Parse → Clean → Enrich → Aggregate → Sink
- Anomaly flagging and 5-minute windowed aggregations with watermarks
- Checkpoint-based fault recovery (exactly-once delivery to Parquet)
- Batch pipeline for comparison against the streaming version
- Parquet output reader for inspecting results

**Quick start (Windows):**
```bash
cd lab-solutions/session3-etl-pipeline
docker compose up -d
pip install pyspark==3.5.3 kafka-python-ng confluent-kafka
python python/producer.py            # Terminal 1 — generate sensor events
python python/etl_pipeline.py        # Terminal 2 — run the ETL
python python/read_output.py         # Terminal 3 — inspect Parquet output
```

> See the `README.md` inside the folder for Windows-specific Java and Hadoop setup (`winutils.exe`, `hadoop.dll`).

---

## Course Materials

The `course-materials/` folder contains PDFs for each session:

- **`session-XX-course-and-practical-work.pdf`** — lecture slides and practical exercises
- **`session-XX-math-foundations-questions.pdf`** — mathematical foundations problem sets

---

## Prerequisites

| Tool | Version | Required for |
|------|---------|--------------|
| Docker Desktop | 20.10+ | All sessions |
| Docker Compose | v2.0+ | All sessions |
| Python | 3.10+ | All sessions |
| Java (OpenJDK) | 21 | Session 3+ (PySpark) |
| Hadoop binaries (`winutils.exe`, `hadoop.dll`) | 3.3.6 | Session 3+ (Windows only) |

> **Python packages (Sessions 1–2):** `kafka-python-ng >= 2.0.2` and `confluent-kafka >= 2.0.0`  
> **Python packages (Session 3+):** add `pyspark==3.5.3` (3.4.x removed `typing.io`, breaking Python 3.12+)  
> The legacy `kafka-python==2.0.2` crashes on Python 3.12+ — all lab solutions use `kafka-python-ng` instead.

---

*Big Data Engineering Programme · WUT · 2024–2025*
