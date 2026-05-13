# Big Data Engineering Programme

Course materials and lab correction code for the **Big Data Engineering** programme at WUT (Wuhan University of Technology).

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
│   └── session-03-math-foundations-questions.pdf
│
└── lab-solutions/                           # Full corrected code for each lab session
    ├── session-01-kafka-intro/              # Lab 1: Local 3-broker Kafka cluster (KRaft)
    │   ├── docker-compose.yml
    │   ├── requirements.txt
    │   ├── python/                          # Producer, consumer, fault-tolerance test
    │   └── scripts/                         # Cluster setup, topic creation, teardown
    │
    └── session-02-kafka-pipeline/           # Lab 2: Python streaming pipeline
        ├── docker-compose.yml
        ├── requirements.txt
        ├── python/                          # Producer, consumers (per-msg & batch), lag monitor
        └── scripts/                         # Topic setup, rebalance demo, offset reset
```

---

## Sessions Overview

| # | Topic | Course Material | Lab Solution |
|---|-------|-----------------|--------------|
| 1 | Introduction to Data Engineering & Distributed Architectures | PDF | `lab-solutions/session-01-kafka-intro/` |
| 2 | Kafka and Distributed Messaging | PDF | `lab-solutions/session-02-kafka-pipeline/` |
| 3 | ETL Pipelines | PDF | — |
| 4–7 | *(coming soon)* | — | — |

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

## Course Materials

The `course-materials/` folder contains PDFs for each session:

- **`session-XX-course-and-practical-work.pdf`** — lecture slides and practical exercises
- **`session-XX-math-foundations-questions.pdf`** — mathematical foundations problem sets

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker Desktop | 20.10+ |
| Docker Compose | v2.0+ |
| Python | 3.10+ |

> Python packages: `kafka-python-ng >= 2.0.2` and `confluent-kafka >= 2.0.0`  
> (compatible with Python 3.10–3.14; the legacy `kafka-python==2.0.2` crashes on Python 3.12+)

---

*Big Data Engineering Programme · WUT · 2024–2025*
