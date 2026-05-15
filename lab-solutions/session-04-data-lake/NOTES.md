# Session 4 – Data Lake Pipeline: Code Review & Run Notes

**Date:** 2026-04-28  

---

## Project Overview

A Medallion Architecture (Bronze → Silver → Gold) data lake pipeline:

| Zone | Format | Partition key | Purpose |
|------|--------|---------------|---------|
| Raw (Bronze) | JSON | ingestion time (year/month/day/hour) | Immutable, schema-free archive |
| Curated (Silver) | Parquet + Snappy | event time (sensor_type/year/month/day) | Validated, typed, queryable |
| Consumption (Gold) | Parquet | sensor_type/year/month | Daily aggregated stats for BI |

---

## Bugs Found and Fixed

### Bug 1 — `datalake_pipeline.py`: Unused import (`BooleanType`)
**File:** `python/datalake_pipeline.py`  
**Severity:** Minor (lint / unused import)

`BooleanType` was imported from `pyspark.sql.types` but never used. The `is_anomaly`
column is created via `when().otherwise(False)`, which lets Spark infer the
Boolean type automatically.

**Fix:** Removed `BooleanType` from the import statement.

---

### Bug 2 — `datalake_pipeline.py`: Streaming failures not handled
**File:** `python/datalake_pipeline.py`  
**Severity:** Medium (resource leak on failure)

`awaitAnyTermination()` raises a `StreamingQueryException` (not `KeyboardInterrupt`)
when a streaming query fails (e.g., Kafka disconnect, out-of-memory). The original
code only caught `KeyboardInterrupt`, so on unexpected failures the cleanup loop and
`spark.stop()` never ran, leaving the JVM holding ports/memory.

**Fix:** Added an `except Exception as e` block:

```python
except Exception as e:
    print(f"\n❌  Streaming query failed: {e}")
    for name, q in active_queries:
        try:
            q.stop()
        except Exception:
            pass
    spark.stop()
    raise
```

---

### Bug 3 — `query_lake.py`: SQL injection via f-string interpolation
**File:** `python/query_lake.py`  
**Severity:** Low (mitigated by CLI `choices=`, but bad practice)

`target_sensor` was interpolated directly into SQL strings. Added an explicit
allowlist guard at the top of `main()`:

```python
VALID_SENSORS = {"temperature", "humidity", "pressure"}

def main(target_sensor: str = "temperature"):
    if target_sensor not in VALID_SENSORS:
        raise ValueError(f"Unknown sensor '{target_sensor}'. Must be one of: {VALID_SENSORS}")
```

---

### Bug 4 — `compaction.py` / `explore_lake.py`: Duplicated `fmt_bytes` function
**File:** `python/compaction.py` and `python/explore_lake.py`  
**Severity:** Minor (code duplication / maintenance risk)

`fmt_bytes()` was copy-pasted identically in both files.

**Fix:** Extracted to a new shared module `python/utils.py`, replaced both
definitions with `from utils import fmt_bytes`.

---

## Windows Compatibility Fixes

The original code targeted Linux (`/tmp/` paths). All files were updated to run
on Windows. The following changes were required:

### Fix W1 — All scripts: Linux paths → Windows paths

All `/tmp/datalake` and `/tmp/datalake-ckpt` occurrences replaced with
`C:/tmp/datalake` and `C:/tmp/datalake-ckpt` across:
- `datalake_pipeline.py`
- `consumption_zone.py`
- `query_lake.py`
- `compaction.py`
- `explore_lake.py`

### Fix W2 — All scripts: PySpark 3.4.1 → 3.5.3

Python 3.12+ removed `typing.io`, which PySpark 3.4.x relied on. Upgraded to
`pyspark==3.5.3` and updated the Kafka connector reference in all `spark.jars.packages`
configs from `spark-sql-kafka-0-10_2.12:3.4.1` to `spark-sql-kafka-0-10_2.12:3.5.3`.

### Fix W3 — `producer.py`: Remove unsupported `enable_idempotence` config

`kafka-python 2.0.2` does not support `enable_idempotence=True`. Removed it (and
the associated `max_in_flight_requests_per_connection=1`) to fix:
```
AssertionError: Unrecognized configs: {'enable_idempotence': True}
```

### Fix W4 — All PySpark scripts: HADOOP_HOME must be set to Windows path

The JVM on Windows needs to load `hadoop.dll` from `%HADOOP_HOME%\bin\`. If
`HADOOP_HOME` is set to a bash-style path (`/c/hadoop`) the JVM can't resolve it,
causing:
```
UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(...)'
```

**Fix:** Added at the top of every PySpark script, before the PySpark import:

```python
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")
```

This unconditionally overrides any bash-style path inherited from the shell.

Files updated: `datalake_pipeline.py`, `consumption_zone.py`, `query_lake.py`,
`compaction.py`.

### Fix W5 — Checkpoint directory: always wipe completely between runs

Hadoop's `ChecksumFs` creates `.crc` sidecar files alongside every file it writes
(e.g., `metadata` → `.metadata.crc`). On Windows, the `FileContextBasedCheckpointFileManager`
uses `AbstractFileSystem.rename()` which fails if the `.crc` destination already
exists:
```
FileAlreadyExistsException: Rename destination .metadata.crc already exists.
```

**Fix:** Use `rm -rf /c/tmp/datalake-ckpt` (full directory removal) to wipe the
checkpoint tree before a fresh run. **Never** use `rm -rf *` — it skips dot-files
like `.metadata.crc`.

> Note: `RawLocalFs` / `RawLocalFileSystem` Spark configs were tested as a bypass
> but both crash the JVM on Windows. Clean checkpoints are the correct solution.

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
export PYTHONIOENCODING="utf-8"   # required for Unicode/emoji in Python 3.12+ on Windows
```

---

## How to Run (Windows)

```bash
# 0. Ensure Kafka is running (docker compose up)
docker compose up -d
docker ps   # confirm kafka1/kafka2/kafka3 healthy

# 1. Create output directories
mkdir -p /c/tmp/datalake/{raw,curated,consumption}
mkdir -p /c/tmp/datalake-ckpt/{raw,curated}

# 2. Terminal 1 – Start streaming pipeline
export JAVA_HOME="/c/Program Files/Microsoft/jdk-21.0.10.7-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
export PYTHONIOENCODING="utf-8"
cd session4-data-lake
python3 python/datalake_pipeline.py

# 3. Terminal 2 – Produce messages
export PYTHONIOENCODING="utf-8"
python3 python/producer.py --count 500
python3 python/producer.py --count 2000   # larger dataset for pruning demo

# 4. After 2+ batches (30s trigger), stop pipeline (Ctrl+C), then:

# Build Gold / Consumption zone
python3 python/consumption_zone.py

# Run Spark SQL queries + partition pruning benchmark
python3 python/query_lake.py
python3 python/query_lake.py --sensor humidity

# Explore lake structure (no Spark needed)
python3 python/explore_lake.py
python3 python/explore_lake.py --zone curated

# Compact small files
python3 python/compaction.py --dry-run
python3 python/compaction.py --target-files 1

# 5. Reset for a fresh run
rm -rf /c/tmp/datalake-ckpt   # IMPORTANT: full remove, not rm -rf *
mkdir -p /c/tmp/datalake-ckpt/{raw,curated}
rm -rf /c/tmp/datalake/{raw,curated,consumption}/*
```

---

## Verified Output (Live Test Run — 2026-04-28)

**Pipeline:** 2 micro-batches, 730 total records processed (500 produced by producer.py)

**Curated zone:**
```
C:/tmp/datalake/curated/domain=iot/
├── sensor_type=humidity/year=2026/month=4/day=28/   (2 Parquet files)
├── sensor_type=pressure/year=2026/month=4/day=28/   (2 Parquet files)
└── sensor_type=temperature/year=2026/month=4/day=28/ (2 Parquet files)
```

**Raw zone:**
```
C:/tmp/datalake/raw/source=kafka/topic=sensor-events/
├── year=2026/month=4/day=28/hour=10/  (3 JSON files — batch 0, ingestion at 10:xx)
└── year=2026/month=4/day=28/hour=12/  (2 JSON files — batch 1, ingestion at 12:xx)
```

**Gold layer results (consumption_zone.py):**

| sensor_type | records | avg_value | anomalies | anomaly_pct |
|-------------|---------|-----------|-----------|-------------|
| humidity | 264 | 58.19 % | 18 | 6.82% |
| pressure | 225 | 1008.34 hPa | 0 | 0.0% |
| temperature | 241 | 24.71 °C | 38 | 15.77% |

**SQL queries (query_lake.py):** All 4 queries ran. Partition pruning confirmed
in physical plan (`PartitionFilters: [sensor_type = temperature]`), 1.8× speedup
(produce ≥ 2000 messages for a stronger benchmark).

---

## Architecture Notes

```
C:/tmp/datalake/
├── raw/
│   └── source=kafka/
│       └── topic=sensor-events/
│           └── year=YYYY/month=MM/day=DD/hour=HH/   ← ingestion time partitions
│               └── *.json
├── curated/
│   └── domain=iot/
│       └── sensor_type=<X>/year=YYYY/month=MM/day=DD/  ← event time partitions
│           └── *.snappy.parquet
├── consumption/
│   └── use_case=sensor_averages/
│       └── sensor_type=<X>/year=YYYY/month=MM/       ← gold aggregates
│           └── *.parquet
└── curated-compacted/                                 ← compaction output
    └── domain=iot/ …

C:/tmp/datalake-ckpt/
├── raw/      ← Spark checkpoint for raw zone writer
└── curated/  ← Spark checkpoint for curated zone writer
```

**Partitioning strategy:**
- Raw zone uses **ingestion time** → answers "what arrived between 14:00–15:00?"
- Curated zone uses **event time** → answers "what happened on Jan 15?" correctly
  even for late-arriving messages

---

## Design Notes

1. **Small file problem**: Each 30-second streaming batch creates ~3 Parquet files
   (one per sensor type). After 2 hours: ~720 files. Run `compaction.py`
   periodically or increase `TRIGGER_SECS` to reduce file count.

2. **`coalesce` vs `repartition` in compaction**: `coalesce(n)` avoids a full shuffle
   (correct here), but cannot increase file count above the current number.
   `repartition(n)` forces a shuffle and can increase file count if needed.

3. **No schema evolution support**: Adding a new field to `SENSOR_SCHEMA` causes
   reads of old Parquet files to return `null` for that field. The raw JSON zone
   preserves the original payload exactly, enabling full re-processing from source
   if schema changes require it.

4. **Partition pruning benchmark** requires a reasonably large dataset (≥ 500 messages)
   to show measurable speedup. Produce ≥ 2000 messages for a > 2× speedup.

---

## File Map

```
session4-data-lake/
├── docker-compose.yml           3-broker KRaft Kafka cluster + Kafka UI (port 8080)
├── requirements.txt             pyspark==3.5.3  kafka-python==2.0.2
├── NOTES.md                     ← this file
├── scripts/
│   ├── setup.sh                 One-shot: start Docker, venv, create lake dirs
│   └── clean.sh                 Wipe C:/tmp/datalake/ and C:/tmp/datalake-ckpt/
└── python/
    ├── utils.py           ★NEW  Shared fmt_bytes() utility
    ├── producer.py        ★FIX  Removed unsupported enable_idempotence config
    ├── datalake_pipeline.py  ★  Kafka → Raw (JSON) + Curated (Parquet) streaming
    ├── consumption_zone.py   ★  Curated → Gold daily aggregates (batch)
    ├── query_lake.py         ★  Spark SQL queries + partition pruning benchmark
    ├── explore_lake.py       ★  Walk lake directory tree, file counts/sizes
    └── compaction.py         ★  Small file compaction utility

★ = modified from original
```
