# Session 3 – Building ETL/ELT Pipelines
## Kafka + Spark Structured Streaming Lab

> **Big Data Engineering Programme · Session 3 of 7 · Duration: ~90 minutes**
>
> **Prerequisites:** Sessions 1 & 2 completed. Kafka cluster running. `sensor-events` topic populated.

---

## 📁 Project Structure

```
session3-etl-pipeline/
├── docker-compose.yml                  # Same 3-broker Kafka cluster (Sessions 1–3)
├── requirements.txt                    # pyspark==3.5.3  kafka-python==2.0.2
├── scripts/
│   ├── setup.sh                        # One-shot setup (cluster + venv + dirs)
│   ├── run_pipeline.sh                 # Launch ETL via spark-submit
│   └── clean.sh                        # Wipe Parquet & checkpoint dirs
└── python/
    ├── producer.py                     # ★ Kafka sensor producer (reused from S2)
    ├── etl_pipeline.py                 # ★ MAIN: Kafka → Spark Streaming → Parquet
    ├── batch_pipeline.py               # ★ Same transforms, batch mode (for comparison)
    └── read_output.py                  # ★ Read & display Parquet output

★ = modified from original
```

---

## 🧰 Prerequisites

| Tool | Version | Check |
|---|---|---|
| Docker Desktop | 20.10+ | `docker --version` |
| Python | 3.10+ | `python --version` |
| Java | 21 (OpenJDK) | `java -version` *(required by PySpark)* |
| PySpark | 3.5.3 | `pip install pyspark==3.5.3` |
| kafka-python | 2.0.2 | `pip install kafka-python==2.0.2` |

> **Java 21 is required.** PySpark 3.5.3 requires Java 11+ and Python 3.12 removed
> `typing.io` which PySpark 3.4.x relied on — use **3.5.3** to avoid import errors.

**Windows only — additional requirements:**

| Requirement | Location |
|---|---|
| `winutils.exe` (Hadoop 3.3.6) | `C:\hadoop\bin\winutils.exe` |
| `hadoop.dll` (Hadoop 3.3.6) | `C:\hadoop\bin\hadoop.dll` |

> Download from [github.com/cdarlint/winutils](https://github.com/cdarlint/winutils)
> (use the `hadoop-3.3.6` folder). Without these the JVM fails with
> `UnsatisfiedLinkError: NativeIO$Windows.access0`.

---

## 🚀 Quick Start

### Linux / macOS

```bash
# 1. Setup everything at once
chmod +x scripts/*.sh
./scripts/setup.sh

# 2. Start the ETL pipeline (Terminal 1)
source venv/bin/activate
python python/etl_pipeline.py

# 3. Send messages (Terminal 2)
source venv/bin/activate
python python/producer.py --count 120 --delay 0.1

# 4. After a few batches (~30 s), read the output (Terminal 3)
source venv/bin/activate
python python/read_output.py
```

### Windows

> **Set these in every terminal before running any script:**

```bash
export JAVA_HOME="/c/Program Files/Microsoft/jdk-21.0.10.7-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
export PYTHONIOENCODING="utf-8"
```

```bash
# 1. Install dependencies
pip install pyspark==3.5.3 kafka-python==2.0.2

# 2. Start Kafka cluster
docker compose up -d && docker ps

# 3. Create output directories
mkdir -p /c/tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}

# 4. Terminal 1 – start the ETL pipeline
python python/etl_pipeline.py
# Optional: also write raw flagged records
python python/etl_pipeline.py --raw

# 5. Terminal 2 – produce messages
python python/producer.py --count 120 --delay 0.1

# 6. Terminal 3 – read output (after first batch fires, ~15 s)
python python/read_output.py
python python/read_output.py --raw   # also show raw flagged records

# 7. Batch comparison
python python/batch_pipeline.py

# 8. Reset (wipe and recreate)
rm -rf /c/tmp/spark-etl
mkdir -p /c/tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}
```

> **Windows checkpoint note:** Always use `rm -rf /c/tmp/spark-etl` (full directory
> removal) rather than `rm -rf /c/tmp/spark-etl/*`. Hadoop writes hidden `.crc`
> sidecar files (e.g. `.metadata.crc`) alongside every file. The `*` glob skips
> dot-files, leaving stale `.crc` files that cause `FileAlreadyExistsException`
> on the next pipeline start.

> **Windows note:** Spark prints `IOException: Failed to delete snappy-java jar`
> on shutdown — this is a benign Windows file-lock warning. Output is fully written
> before cleanup runs.

---

## 🏗️ Pipeline Architecture

```
 ┌──────────────┐   publish    ┌─────────────────────┐
 │  producer.py  │ ──────────► │   Kafka Cluster      │
 │  (sensor IoT) │             │   topic: sensor-events│
 └──────────────┘             └──────────┬────────────┘
                                          │  subscribe
                                          ▼
                              ┌─────────────────────────────────────┐
                              │   Spark Structured Streaming         │
                              │                                       │
                              │  1. readStream (binary → DataFrame)   │
                              │  2. from_json() (parse payload)       │
                              │  3. filter() (drop nulls)             │
                              │  4. withColumn() (flag anomalies)     │
                              │  5. withWatermark() + window() + avg  │
                              │  6. writeStream (Parquet sink)        │
                              └──────────────┬──────────────────────┘
                                             │
                           ┌─────────────────┴──────────────────┐
                           │                                      │
                    ┌──────▼──────┐                   ┌──────────▼──────┐
                    │  Aggregated  │                   │  Raw Flagged     │
                    │  Parquet     │                   │  Parquet         │
                    │  /output/    │                   │  /raw/           │
                    └─────────────┘                   └──────────────────┘
                    (windowed avg,                     (with is_anomaly flag,
                     outputMode=append)                outputMode=append)
```

**Pipeline stages explained:**

| Stage | Code | Purpose |
|---|---|---|
| 1. Ingest | `spark.readStream.format("kafka")` | Subscribe to `sensor-events` topic |
| 2. Parse | `from_json(col("value").cast("string"), schema)` | Decode binary JSON payload |
| 3. Clean | `.filter(col("sensor").isNotNull())` | Drop malformed records |
| 4. Enrich | `.withColumn("is_anomaly", ...)` | Flag high-temp / high-humidity |
| 5. Aggregate | `.withWatermark(...).groupBy(window(...))` | 5-min tumbling averages |
| 6. Sink | `.writeStream.format("parquet")` | Persist results with checkpoint |

---

## 🔬 Lab Steps — Detailed

### Step 0 — Environment Setup

```bash
# Start Kafka cluster
docker compose up -d
docker compose ps    # all 4 containers Up

# Create/verify sensor-events topic
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events

# Set up Python environment (Linux/macOS)
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Install dependencies (Windows — no venv needed if using system Python)
pip install pyspark==3.5.3 kafka-python==2.0.2

# Create output directories (Linux/macOS)
mkdir -p /tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}

# Create output directories (Windows — use C:/tmp/)
mkdir -p /c/tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}
```

---

### Step 1 — Create the Spark Session

Key config parameters:

```python
spark = SparkSession.builder \
    .appName("Session3-ETL") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()
```

| Config | Value | Why |
|---|---|---|
| `master("local[*]")` | all CPU cores | local mode; replace with cluster URL in production |
| `spark.jars.packages` | Kafka connector | fetched automatically from Maven Central |
| `shuffle.partitions` | 3 | matches our 3-partition topic; avoids 200 empty files |
| `stopGracefullyOnShutdown` | true | finish current micro-batch before exit |

> **Windows note:** Every PySpark script sets `HADOOP_HOME` before importing PySpark:
> ```python
> import os
> os.environ["HADOOP_HOME"] = r"C:\hadoop"
> os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")
> ```
> This ensures the JVM resolves `hadoop.dll` via a Windows-native path. If `HADOOP_HOME`
> is a bash-style path like `/c/hadoop`, the JVM cannot resolve it and fails with
> `UnsatisfiedLinkError: NativeIO$Windows.access0`.

---

### Step 2 — Read the Raw Kafka Stream

```python
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,...") \
    .option("subscribe", "sensor-events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()
```

Print the schema to understand the raw structure:

```
root
 |-- key:           binary
 |-- value:         binary   ← JSON payload (MUST be parsed before use)
 |-- topic:         string
 |-- partition:     integer
 |-- offset:        long
 |-- timestamp:     timestamp
 |-- timestampType: integer
```

> ⚠️ **Most common mistake:** calling `.filter(col("value") > 30)` directly on the raw stream
> will fail because `value` is **binary bytes**, not a number. Always cast + `from_json()` first.

---

### Step 3 — Parse the JSON Payload

```python
# Define the expected schema (never use inferSchema on a stream)
sensor_schema = StructType([
    StructField("sensor",    StringType(), nullable=False),
    StructField("value",     DoubleType(), nullable=False),
    StructField("unit",      StringType(), nullable=True),
    StructField("timestamp", LongType(),   nullable=False),
    StructField("device_id", StringType(), nullable=True),
])

# Two-step select: create struct, then flatten
parsed = raw_stream \
    .select(
        col("key").cast("string").alias("message_key"),
        from_json(col("value").cast("string"), sensor_schema).alias("data"),
        col("partition"), col("offset"),
    ) \
    .select(
        col("message_key"),
        col("data.sensor"), col("data.value"), col("data.unit"),
        to_timestamp(expr("data.timestamp / 1000")).alias("event_time"),
        col("partition"), col("offset"),
    )
```

**Why explicit schema?** Schema inference is impossible on unbounded streams. Explicit schema
enables Catalyst optimization and produces `null` (not crashes) on type mismatches.

---

### Step 4 — Filter and Flag Anomalies

```python
clean = parsed \
    .filter(col("sensor").isNotNull()) \
    .filter(col("value").isNotNull()) \
    .filter(col("event_time").isNotNull())   # drop unparseable timestamps

flagged = clean.withColumn("is_anomaly",
    when((col("sensor") == "temperature") & (col("value") > 35), True)
    .when((col("sensor") == "humidity")    & (col("value") > 90), True)
    .otherwise(False)
)
```

---

### Step 5 — Windowed Aggregation

```python
windowed = flagged \
    .withWatermark("event_time", "2 minutes")   # tolerate up to 2 min late
    .groupBy(
        window(col("event_time"), "5 minutes"),  # 5-min tumbling windows
        col("sensor"),
    ) \
    .agg(avg("value").alias("avg_value"), ...)
```

**Why `withWatermark`?** Without it, Spark keeps state for ALL past windows forever
(memory leak). The watermark tells Spark: drop events > 2 min late and safely
free the memory for closed windows.

**Why `event_time` vs Kafka `timestamp`?** Event time = when the event happened in the
real world. Kafka timestamp = when it arrived at the broker. Network delays can make
Kafka timestamps misleading for time-based grouping.

---

### Step 6 — Write to Parquet Sink

```python
query = windowed \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "C:/tmp/spark-etl/output") \    # Linux: /tmp/spark-etl/output
    .option("checkpointLocation", "C:/tmp/spark-etl/checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()
```

**Output modes for Parquet file sink:**

| Mode | Writes | Supported by Parquet sink |
|---|---|---|
| `append` | Only new/finalized rows | ✅ Yes — required for windowed aggregations |
| `update` | Changed rows since last trigger | ❌ No — causes `AnalysisException` on startup |
| `complete` | Entire result table every trigger | ❌ No — too large; requires in-memory sink |

> ⚠️ **Common bug:** using `outputMode("update")` with a Parquet file sink crashes the
> pipeline on startup with `AnalysisException: Data source parquet does not support
> Update output mode`. Always use `append` for Parquet sinks. With `withWatermark`,
> Spark emits a window to the sink once the watermark advances past the window end
> (window_duration + watermark_delay = 5 + 2 = 7 minutes past window start).

**Checkpoint directory stores:**
- Kafka offsets read so far (prevents reprocessing on restart)
- Watermark state (prevents re-opening closed windows)
- Running aggregation state

---

### Step 7 — Run the Full Pipeline

**Set up environment (Windows — every terminal):**

```bash
export JAVA_HOME="/c/Program Files/Microsoft/jdk-21.0.10.7-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
export PYTHONIOENCODING="utf-8"
```

**Terminal 1 — Start the ETL pipeline:**

```bash
# Linux/macOS
source venv/bin/activate
python python/etl_pipeline.py

# Windows
python python/etl_pipeline.py

# Optional: also write raw flagged records
python python/etl_pipeline.py --raw
```

**Terminal 2 — Send messages:**

```bash
python python/producer.py --count 120 --delay 0.1
```

Watch for Spark log lines: `Batch: 0`, `Batch: 1`, ... Each batch fires every 10 seconds.

**Terminal 3 — Inspect the Parquet output:**

```bash
python python/read_output.py
python python/read_output.py --raw   # also show raw flagged records
```

Expected output (verified live run — 2026-04-28):

```
+--------------------+-------------+-------+--------+--------+-------------+---------------+
| window_start        | sensor      | avg   | min    | max    | event_count | anomaly_count |
+--------------------+-------------+-------+--------+--------+-------------+---------------+
| 2026-04-28 10:00   | humidity    | 60.24 | 20.63  | 88.29  |     12      |       0       |
| 2026-04-28 10:00   | pressure    |1009.24| 983.68 |1029.06 |     11      |       0       |
| 2026-04-28 10:00   | temperature | 26.03 | 18.80  | 35.78  |      7      |       1       |
| 2026-04-28 10:50   | humidity    | 54.98 | 20.73  | 93.47  |     78      |       6       |
| 2026-04-28 10:50   | pressure    |1007.10| 976.01 |1044.34 |     58      |       0       |
| 2026-04-28 10:50   | temperature | 22.69 |  8.05  | 41.57  |     64      |       4       |
+--------------------+-------------+-------+--------+--------+-------------+---------------+
850 raw records processed · 66 anomalies
```

---

### Step 8 — Compare: Batch vs Streaming

Run the **batch** version of the same pipeline:

```bash
python python/batch_pipeline.py
```

This uses `spark.read` (not `spark.readStream`) and reads ALL current messages
as a bounded dataset. The transformations are **identical** to the streaming
pipeline — only the source/sink API differs.

This demonstrates Spark's unified API: the same code works for both batch and streaming.

---

### Step 9 — Checkpoint Recovery Exercise

1. Start the pipeline and let it process 2–3 batches.
2. Stop it with `Ctrl+C`.
3. Keep producing messages while the pipeline is stopped.
4. Restart: `python python/etl_pipeline.py`

**Observe:** Spark resumes from the last committed Kafka offset (from the checkpoint).
It processes all messages accumulated while it was stopped, without reprocessing
previously handled messages.

> **To start from scratch (Linux/macOS):** `./scripts/clean.sh`
>
> **To start from scratch (Windows):**
> ```bash
> rm -rf /c/tmp/spark-etl
> mkdir -p /c/tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}
> ```
> Use full `rm -rf` on the directory, not `rm -rf *` — the glob skips hidden `.crc`
> files created by Hadoop, which cause `FileAlreadyExistsException` on restart.

---

## 📊 Testing Checklist

```
Environment
  [ ] docker compose ps → all 4 containers Up
  [ ] java -version → 21 (OpenJDK recommended)
  [ ] pip install pyspark==3.5.3 → no errors
  [ ] Output directories exist:
        Linux:   /tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}
        Windows: C:/tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}
  [ ] (Windows only) C:\hadoop\bin\winutils.exe and hadoop.dll present
  [ ] (Windows only) No UnsatisfiedLinkError for NativeIO$Windows.access0

Producer
  [ ] python/producer.py sends messages without errors
  [ ] Messages appear in Kafka UI → http://localhost:8080

Streaming Pipeline
  [ ] python/etl_pipeline.py starts without errors
  [ ] Spark log shows "Batch: 0" within 15 s of starting
  [ ] Subsequent batches appear every 10 s
  [ ] output/ contains .parquet files after batch 0

Output Inspection
  [ ] python/read_output.py displays schema and rows
  [ ] All 3 sensor types appear in the output
  [ ] avg_value is a reasonable number (not null)
  [ ] anomaly_count > 0 for temperature or humidity

Checkpoint Recovery
  [ ] Stop pipeline after 2 batches → restart
  [ ] Spark resumes from last offset (does NOT reprocess batch 0)
  [ ] Messages produced during downtime are processed on restart

Batch Pipeline (comparison)
  [ ] python/batch_pipeline.py completes without errors
  [ ] Results match (same aggregations as streaming)
  [ ] spark.read used instead of spark.readStream

--raw flag
  [ ] python/etl_pipeline.py --raw creates files in raw/
  [ ] python/read_output.py --raw shows individual records with is_anomaly column
  [ ] Anomaly count > 0
```

---

## 🧠 Reflection Questions

1. The `value` column from Kafka arrives as **binary**. What would happen if you
   tried to call `.filter(col("value") > 30)` directly on the raw stream, before parsing?

2. You set `.trigger(processingTime="10 seconds")`. What trade-off does this create
   between latency, throughput, and the number of Parquet files generated?

3. Explain in your own words why the **watermark** is necessary for windowed
   aggregations. What would happen without it?

4. Your Spark job crashes after writing Batch 5. When you restart it, which
   batch does Spark start from, and why?

5. You want to add a new transformation (e.g., convert Celsius → Fahrenheit).
   Where in the code would you insert it, and does it require clearing the
   checkpoint directory?

---

## 📖 ETL vs ELT Quick Reference

| | ETL | ELT |
|---|---|---|
| Transform location | External processing layer | Inside the destination |
| Raw data preserved? | ❌ Only final form stored | ✅ Raw always available |
| Compute | Dedicated ETL server | Powerful cloud DW / Spark |
| Schema | Schema-on-write | Schema-on-read |
| Re-processing | Must re-extract from source | Re-run transforms on existing raw |
| Tools | SSIS, Informatica | dbt, Spark, BigQuery SQL |

**This lab implements ELT:** raw events are stored in Kafka first, then
transformed by Spark. The raw data remains replayable at any time.

---

## ➡️ Preview: Session 4

Next session — Data Lakes:
- Data lake concepts: raw zone, curated zone, consumption zone
- Storage formats in depth: Parquet, ORC, Avro — when to use each
- Partitioning strategies for efficient querying
- **Lab:** Ingest Kafka events into a structured data lake, organise by date
  and sensor type, and query with Spark SQL

---

## 🔗 Further Reading

- [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Spark + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- Chambers & Zaharia (2018). *Spark: The Definitive Guide*. O'Reilly. Chapters 20–22.
- Kleppmann (2017). *Designing Data-Intensive Applications*. O'Reilly. Chapter 11.

---

*Course material – Big Data Engineering Programme 2024–2025*  
*Updated 2026-04-28: pyspark 3.4.1 → 3.5.3; Windows compatibility notes added; outputMode "update" → "append" bug fixed*
