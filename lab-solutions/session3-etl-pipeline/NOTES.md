# Session 3 – ETL Pipeline: Code Review & Run Notes

**Date:** 2026-04-28  

---

## Project Overview

A Kafka + Spark Structured Streaming ETL pipeline that:
1. Reads sensor events (temperature, humidity, pressure) from a Kafka topic
2. Parses, cleans, and flags anomalies
3. Aggregates into 5-minute tumbling windows
4. Writes results to Parquet (streaming + optional batch mode)

---

## Bugs Found and Fixed

### Bug 1 — `etl_pipeline.py`: Unused import (`lit`)
**File:** `python/etl_pipeline.py`  
**Severity:** Minor (lint / unused import)

`lit` was imported from `pyspark.sql.functions` but never referenced. Removed.

---

### Bug 2 — `etl_pipeline.py`: Wrong output mode for Parquet sink
**File:** `python/etl_pipeline.py` — `write_aggregated()`  
**Severity:** High (pipeline crashes on startup)

```python
# BEFORE — crashes with AnalysisException
.outputMode("update")

# AFTER
.outputMode("append")
```

Parquet file sink only supports `append` output mode. `update` mode is only valid
for in-memory/console sinks. With `append` mode + `withWatermark`, Spark emits a
window to the sink once the watermark advances past the window end.

> **Lab note:** With the default 5-minute window and 2-minute watermark, windowed
> rows appear in the output after the watermark advances 7 minutes past the window
> start. Because `startingOffsets="earliest"` reads all historical data first, any
> windows whose end time is already before (max event time − 2 min) are emitted in
> the first batch. Run `read_output.py` after the first batch fires.

---

### Bug 3 — `etl_pipeline.py`: Streaming failure not handled
**File:** `python/etl_pipeline.py` — `main()`  
**Severity:** Medium (resource leak on failure; `--raw` query not monitored)

Two problems in the original termination block:

1. Only `KeyboardInterrupt` was caught. A `StreamingQueryException` (Kafka disconnect,
   OOM, etc.) propagated uncaught, leaving the JVM alive holding ports.
2. Only `query_agg` was awaited. When `--raw` is used, `query_raw` ran in the
   background unmonitored — a failure in `query_raw` would be silent.

**Fix:**
```python
# BEFORE
try:
    query_agg.awaitTermination()
except KeyboardInterrupt:
    ...

# AFTER
try:
    spark.streams.awaitAnyTermination()   # watches BOTH queries
except KeyboardInterrupt:
    ...                                   # graceful stop
except Exception as e:
    print(f"\n❌  Streaming query failed: {e}")
    query_agg.stop()
    ...
    spark.stop()
    raise
```

---

### Bug 4 — `batch_pipeline.py`: Missing `event_time` null filter
**File:** `python/batch_pipeline.py`  
**Severity:** Medium (silent data correctness bug)

The streaming pipeline (`etl_pipeline.py`) drops null `event_time` rows in
`filter_clean()`. The batch pipeline was missing this filter, so messages with
unparseable timestamps would produce null window keys and be miscounted.

**Fix:** Added `.filter(col("event_time").isNotNull())` to the parse chain.

---

### Bug 5 — `read_output.py`: Non-idiomatic boolean filter + unused import
**File:** `python/read_output.py`  
**Severity:** Minor

```python
# BEFORE
import sys   # never used
anomalies = df.filter(col("is_anomaly") == True)

# AFTER
# sys import removed
anomalies = df.filter(col("is_anomaly"))
```

---

## Windows Compatibility Fixes

### Fix W1 — All scripts: Linux paths → Windows paths

All `/tmp/spark-etl/` paths replaced with `C:/tmp/spark-etl/` across all four
Python files.

### Fix W2 — All scripts: PySpark 3.4.1 → 3.5.3

Python 3.12+ removed `typing.io` which PySpark 3.4.x relied on. All
`spark.jars.packages` references updated to `spark-sql-kafka-0-10_2.12:3.5.3`.

### Fix W3 — `producer.py`: Removed unsupported `enable_idempotence` config

`kafka-python 2.0.2` does not support `enable_idempotence=True`. Removed it and
the associated `max_in_flight_requests_per_connection=1`.

### Fix W4 — All PySpark scripts: HADOOP_HOME must be a Windows path

Added at the top of every PySpark script before the PySpark import:

```python
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")
```

Files updated: `etl_pipeline.py`, `batch_pipeline.py`, `read_output.py`.

> **Root cause:** The JVM loads `hadoop.dll` from `%HADOOP_HOME%\bin\`. If
> `HADOOP_HOME` is a bash-style path like `/c/hadoop`, the JVM on Windows cannot
> resolve it and fails with `UnsatisfiedLinkError: NativeIO$Windows.access0`.

---

## Prerequisites (Windows)

| Requirement | Version | Location |
|-------------|---------|----------|
| Java | OpenJDK 21 (Microsoft) | `C:\Program Files\Microsoft\jdk-21.0.10.7-hotspot` |
| winutils.exe | Hadoop 3.3.6 | `C:\hadoop\bin\winutils.exe` |
| hadoop.dll | Hadoop 3.3.6 | `C:\hadoop\bin\hadoop.dll` |
| PySpark | 3.5.3 | `pip install pyspark==3.5.3` |
| kafka-python | 2.0.2 | `pip install kafka-python==2.0.2` |
| Docker | Desktop | For 3-broker Kafka cluster |

**Environment setup for every terminal:**
```bash
export JAVA_HOME="/c/Program Files/Microsoft/jdk-21.0.10.7-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
export PYTHONIOENCODING="utf-8"
```

---

## Architecture

| Component | Detail |
|-----------|--------|
| Kafka cluster | 3-broker KRaft (no ZooKeeper), ports 9092 / 9094 / 9096 |
| Spark | 3.5.3, local[*] mode, Kafka connector via Maven |
| Topic | `sensor-events`, 3 partitions, replication-factor 3 |
| Trigger | 10 seconds (micro-batch) |
| Window | 5-minute tumbling, 2-minute watermark |
| Output mode | `append` (aggregated) + `append` (raw, optional) |
| Sink format | Parquet with checkpoint-aware recovery |

---

## How to Run (Windows)

```bash
# Set up environment (every terminal)
export JAVA_HOME="/c/Program Files/Microsoft/jdk-21.0.10.7-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
export PYTHONIOENCODING="utf-8"

# 1. Ensure Kafka is running
docker compose up -d && docker ps

# 2. Create output directories
mkdir -p /c/tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}

# 3. Terminal 1 – start streaming ETL
cd session3-etl-pipeline
python3 python/etl_pipeline.py

# Optional: also write raw flagged records
python3 python/etl_pipeline.py --raw

# 4. Terminal 2 – produce messages
python3 python/producer.py --count 120 --delay 0.1

# 5. Terminal 3 – read output (after first batch fires)
python3 python/read_output.py
python3 python/read_output.py --raw

# 6. Batch comparison
python3 python/batch_pipeline.py

# 7. Reset
rm -rf /c/tmp/spark-etl
mkdir -p /c/tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}
```

> **Windows note:** Spark prints `IOException: Failed to delete snappy-java jar`
> on shutdown. This is a benign Windows file-lock warning — the output is already
> fully written before the cleanup runs.

---

## Verified Output (Live Test Run — 2026-04-28)

**Streaming pipeline** (`etl_pipeline.py --raw`): 850 raw records processed, 66 anomalies.

**Aggregated output** (`read_output.py`):

| window_start | sensor | avg | min | max | count | anomalies |
|---|---|---|---|---|---|---|
| 2026-04-28 10:00 | humidity | 60.24 | 20.63 | 88.29 | 12 | 0 |
| 2026-04-28 10:00 | pressure | 1009.24 | 983.68 | 1029.06 | 11 | 0 |
| 2026-04-28 10:00 | temperature | 26.03 | 18.8 | 35.78 | 7 | 1 |
| 2026-04-28 10:50 | humidity | 54.98 | 20.73 | 93.47 | 78 | 6 |
| 2026-04-28 10:50 | pressure | 1007.10 | 976.01 | 1044.34 | 58 | 0 |
| 2026-04-28 10:50 | temperature | 22.69 | 8.05 | 41.57 | 64 | 4 |
| 2026-04-28 12:00 | humidity | 59.49 | 20.19 | 94.94 | 174 | 12 |
| 2026-04-28 12:00 | pressure | 1008.73 | 975.59 | 1045.0 | 156 | 0 |
| 2026-04-28 12:00 | temperature | 25.42 | 8.09 | 41.96 | 170 | 33 |

**Batch pipeline** (`batch_pipeline.py`): 880 messages, results match streaming windows.

---

## Design Notes

1. **Only `query_agg` was originally awaited** in `etl_pipeline.py`. Fixed to use
   `spark.streams.awaitAnyTermination()` so both queries are monitored.

2. **`producer.py` sends a `source` field** not in `SENSOR_SCHEMA`. Spark's
   `from_json()` silently ignores unknown fields — intentional schema-on-read behavior.

3. **No dead-letter queue**: malformed records are dropped with `.filter(isNotNull)`.
   In production these should go to a DLQ Kafka topic.

4. **`requirements.txt` pins `kafka-python==2.0.2`** — unmaintained; consider
   `kafka-python-ng` or `confluent-kafka` for new projects.

---

## File Map

```
session3-etl-pipeline/
├── docker-compose.yml          3-broker KRaft Kafka cluster + Kafka UI (port 8080)
├── requirements.txt            pyspark==3.5.3  kafka-python==2.0.2
├── NOTES.md                    ← this file
├── scripts/
│   ├── setup.sh                One-shot: start Docker, create venv, install deps
│   ├── run_pipeline.sh         Launch ETL via spark-submit
│   └── clean.sh                Wipe C:/tmp/spark-etl/ and recreate empty dirs
└── python/
    ├── producer.py        ★    Publish random sensor events to Kafka
    ├── etl_pipeline.py    ★    Kafka → Spark Streaming → Parquet (main pipeline)
    ├── batch_pipeline.py  ★    Same transforms, bounded batch (for comparison)
    └── read_output.py     ★    Read and display Parquet output

★ = modified from original
```
