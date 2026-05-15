# Session 4 – Designing and Managing Data Lakes
## Three-Zone Data Lake with Spark SQL & Partition Pruning

> **Big Data Engineering Programme · Session 4 of 7 · Duration: ~90 minutes**
>
> **Prerequisites:** Sessions 1–3 completed. Kafka cluster + PySpark working.

---

## 📁 Project Structure

```
session4-data-lake/
├── docker-compose.yml                  # Same Kafka cluster (Sessions 1–4)
├── requirements.txt                    # pyspark==3.5.3  kafka-python==2.0.2
├── scripts/
│   ├── setup.sh                        # One-shot setup
│   └── clean.sh                        # Wipe lake & checkpoints
└── python/
    ├── utils.py                        # Shared utilities (fmt_bytes)
    ├── producer.py                     # Kafka sensor producer (high volume)
    ├── datalake_pipeline.py            # ★ MAIN: Kafka → Raw + Curated zones
    ├── consumption_zone.py             # Build Gold layer from Curated (batch)
    ├── query_lake.py                   # Spark SQL queries + pruning benchmark
    ├── explore_lake.py                 # Browse lake structure & file sizes
    └── compaction.py                   # Solve small file problem
```

---

## 🏗️ Medallion Architecture (Bronze → Silver → Gold)

```
Kafka                  Raw Zone (Bronze)          Curated Zone (Silver)       Consumption Zone (Gold)
sensor-events  ──►   JSON, as-is             ──►  Parquet + Snappy       ──►  Aggregated Parquet
               ──►   Partitioned by           ──►  Partitioned by         ──►  Partitioned by
                     ingestion time               event time                   sensor/year/month
                     year/month/day/hour          sensor_type/year/month/day

                     ← audit trail →              ← analytical queries →       ← BI / dashboards →
                     ← forever →                  ← 1-3 years →               ← rolling →
```

### The three zones explained

| Zone | Also called | Format | Partitioned by | Purpose |
|---|---|---|---|---|
| **Raw** | Bronze / Landing | JSON (as-is) | Ingestion time | Immutable audit trail, replay source |
| **Curated** | Silver / Standardised | Parquet + Snappy | Event time + sensor_type | Validated, queryable data |
| **Consumption** | Gold / Serving | Parquet | sensor_type + date | Pre-aggregated, BI-ready |

---

## 🚀 Quick Start

### Linux / macOS

```bash
# 1. Setup
chmod +x scripts/*.sh
./scripts/setup.sh

# Terminal 1 – start the pipeline
source venv/bin/activate
python python/datalake_pipeline.py

# Terminal 2 – produce 500 messages
source venv/bin/activate
python python/producer.py --count 500

# After 2–3 batches (each ~30 s), stop pipeline (Ctrl+C)

# Build gold layer
python python/consumption_zone.py

# Run SQL queries + pruning benchmark
python python/query_lake.py

# Explore the lake structure
python python/explore_lake.py
```

### Windows

> **Extra requirements:** Java 21 (OpenJDK), `winutils.exe` and `hadoop.dll`
> (Hadoop 3.3.6) placed at `C:\hadoop\bin\`. See NOTES.md for full details.

```bash
# Set environment in every terminal before running any script
export JAVA_HOME="/c/Program Files/Microsoft/jdk-21.0.10.7-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"
export PYTHONIOENCODING="utf-8"

# Create output directories (C:/tmp/ instead of /tmp/)
mkdir -p /c/tmp/datalake/{raw,curated,consumption}
mkdir -p /c/tmp/datalake-ckpt/{raw,curated}

# Install dependencies
pip install pyspark==3.5.3 kafka-python==2.0.2

# Terminal 1 – start pipeline
python python/datalake_pipeline.py

# Terminal 2 – produce messages
python python/producer.py --count 500

# After 2+ batches, stop pipeline then run:
python python/consumption_zone.py
python python/query_lake.py
```

> **Before each fresh run on Windows**, wipe the checkpoint directory completely:
> `rm -rf /c/tmp/datalake-ckpt && mkdir -p /c/tmp/datalake-ckpt/{raw,curated}`
> (Using `rm -rf *` is not enough — it skips dot-files like `.metadata.crc`
> that cause rename errors on the next run.)

---

## 🔬 Lab Steps — Detailed

### Step 0 — Setup

```bash
# Start Kafka cluster
docker compose up -d
docker compose ps    # all 4 containers Up

# Create lake directories (Linux/macOS)
mkdir -p /tmp/datalake/{raw,curated,consumption}
mkdir -p /tmp/datalake-ckpt/{raw,curated}

# Create lake directories (Windows — use C:/tmp/)
mkdir -p /c/tmp/datalake/{raw,curated,consumption}
mkdir -p /c/tmp/datalake-ckpt/{raw,curated}

# Virtual environment (Linux/macOS)
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Install dependencies (Windows — no venv needed if using system Python)
pip install pyspark==3.5.3 kafka-python==2.0.2

# Ensure topic exists
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 --list

# Produce initial data (500+ messages recommended)
python python/producer.py --count 500
```

---

### Step 1 — Raw Zone: Land Raw JSON from Kafka

`datalake_pipeline.py` writes the **exact Kafka payload** as JSON without any
transformation:

```python
raw_for_lake = raw_kafka.select(
    col("value").cast("string").alias("raw_json"),   # original JSON string
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("ingestion_ts"),
    year(col("timestamp")).alias("year"),
    month(col("timestamp")).alias("month"),
    dayofmonth(col("timestamp")).alias("day"),
    hour(col("timestamp")).alias("hour"),
)
```

**Why ingestion time for Raw?**
The raw zone answers: *"when did this data arrive in our system?"*
Useful for debugging pipeline delays and SLA monitoring.

Result directory structure:
```
/tmp/datalake/raw/source=kafka/topic=sensor-events/    (Linux)
C:/tmp/datalake/raw/source=kafka/topic=sensor-events/  (Windows)
  year=2024/
    month=01/
      day=15/
        hour=10/
          part-00000-abc123.json
          part-00001-def456.json
```

---

### Step 2 — Curated Zone: Validate, Parse, Partition by Event Time

```python
# Parse + validate
parsed = raw_kafka \
    .select(from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("d"), ...) \
    .select(col("d.sensor").alias("sensor_type"), col("d.value"), ...,
            to_timestamp(expr("d.timestamp / 1000")).alias("event_time"), ...)
    .filter(col("sensor_type").isNotNull())
    .filter(col("value").between(-100, 2000))   # data quality gate

# Flag anomalies
flagged = parsed.withColumn("is_anomaly", ...)

# Add partition columns from EVENT TIME
curated = flagged \
    .withColumn("year",  year(col("event_time"))) \
    .withColumn("month", month(col("event_time"))) \
    .withColumn("day",   dayofmonth(col("event_time")))
```

Written to Parquet with Snappy compression, **sensor_type as outermost partition**:

```
/tmp/datalake/curated/domain=iot/        (Linux)
C:/tmp/datalake/curated/domain=iot/      (Windows)
  sensor_type=temperature/
    year=2024/month=01/day=15/
      part-00000-abc.snappy.parquet
  sensor_type=humidity/
    ...
  sensor_type=pressure/
    ...
```

> **Key difference from Session 3:** the curated zone uses **event time** (not
> ingestion time). Business queries ask "what was the avg temperature on Jan 15?"
> referring to when the measurement happened, not when it arrived.

---

### Step 3 — Consumption Zone (Gold): Daily Aggregates

Run **after** the streaming pipeline has processed at least one batch:

```bash
python python/consumption_zone.py
```

This is a **batch** job that reads the curated zone and computes:

```python
daily_agg = curated_df \
    .groupBy("sensor_type", "year", "month", "day") \
    .agg(
        count("value").alias("record_count"),
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        sum(col("is_anomaly").cast("int")).alias("anomaly_count"),
    )

daily_agg.write.mode("overwrite") \
    .partitionBy("sensor_type", "year", "month") \
    .parquet(CONSUME_PATH)
```

> **Why `mode("overwrite")`?** The consumption zone is always reproducible from
> the curated zone. It doesn't need a checkpoint — it's re-derived on demand.
> This is the ELT advantage.

---

### Step 4 — Spark SQL + Partition Pruning

```bash
python python/query_lake.py
# or to test a specific sensor:
python python/query_lake.py --sensor humidity
```

**Query 1 — With partition pruning:**

```sql
SELECT sensor_type, day,
       ROUND(AVG(value), 2) AS avg_value,
       COUNT(*) AS total_records
FROM sensor_curated
WHERE sensor_type = 'temperature'   -- prunes all other sensor_type dirs
  AND year = 2024 AND month = 1     -- prunes all other year/month dirs
GROUP BY sensor_type, day
ORDER BY day
```

**Query 2 — Anomaly analysis:**

```sql
SELECT sensor_type,
       COUNT(*) AS total,
       SUM(CAST(is_anomaly AS INT)) AS anomalies,
       ROUND(100.0 * SUM(CAST(is_anomaly AS INT)) / COUNT(*), 2) AS anomaly_pct
FROM sensor_curated
GROUP BY sensor_type
ORDER BY anomaly_pct DESC
```

Verify partition pruning in the physical plan:

```python
df.explain(mode="formatted")
# Look for: PartitionFilters: [isnotnull(sensor_type#X),
#            (sensor_type#X = temperature)]
```

---

### Step 5 — Measure Pruning Benefit

The pruning benchmark in `query_lake.py` compares:

```python
# Full table scan (reads ALL 3 sensor_type directories)
spark.sql("SELECT COUNT(*) FROM sensor_curated").collect()

# Pruned scan (reads ONLY sensor_type=temperature/)
spark.sql("SELECT COUNT(*) FROM sensor_curated WHERE sensor_type = 'temperature'").collect()
```

Expected speedup: ~3× (3 partitions → 1 partition read).

With 100 partitions (10 sensor types × 10 months), expect ~10× speedup.

---

### Step 6 — Explore the Lake

```bash
python python/explore_lake.py          # all zones
python python/explore_lake.py --zone curated
```

Output shows the Hive-style directory tree with file counts and sizes.
If the average file is < 1 MB with many files → **small file problem** detected.

---

### Step 7 — Compaction (Small File Problem)

After many 30-second batches, the curated zone will have many small files.

```bash
# Check the problem
python python/compaction.py --dry-run

# Fix it: merge small files into 1 per partition
python python/compaction.py

# Or specify target file count
python python/compaction.py --target-files 2
```

The compaction reads all small Parquet files and rewrites them with `coalesce(N)`.

---

## 📊 Testing Checklist

```
Environment
  [ ] docker compose ps → 4 containers Up (kafka1, kafka2, kafka3, kafka-ui)
  [ ] java -version → 11+ (21 recommended; required on Windows)
  [ ] pip install pyspark==3.5.3 kafka-python==2.0.2 → ok
  [ ] Output directories exist:
        Linux:   /tmp/datalake/{raw,curated,consumption}
        Windows: C:/tmp/datalake/{raw,curated,consumption}
  [ ] (Windows only) C:\hadoop\bin\winutils.exe and hadoop.dll present

Pipeline (datalake_pipeline.py)
  [ ] Starts without errors
  [ ] "✅  2 streaming queries running." printed
  [ ] After producer sends 500 msgs → batches fire every 30 s
  [ ] raw/ contains JSON files partitioned by year/month/day/hour
  [ ] curated/ contains .snappy.parquet files partitioned by sensor_type/year/month/day
  [ ] (Windows) No UnsatisfiedLinkError for NativeIO$Windows.access0

Consumption Zone (consumption_zone.py)
  [ ] Reads curated Parquet without errors
  [ ] Shows daily aggregate table with avg/min/max/anomaly_count for each sensor
  [ ] Writes Parquet to consumption/use_case=sensor_averages/

Spark SQL (query_lake.py)
  [ ] sensor_curated temp view registered
  [ ] Query 1 returns rows only for 'temperature' (no other sensors)
  [ ] Query 2 shows anomaly_pct for each sensor type
  [ ] explain() output contains "PartitionFilters"
  [ ] Pruned scan faster than full scan (speedup ≥ 1.5×; produce 2000 msgs for ≥ 2×)

Explore (explore_lake.py)
  [ ] Shows correct file counts per zone
  [ ] Detects small file warning if avg < 1 MB

Compaction (compaction.py)
  [ ] --dry-run shows file count without writing
  [ ] After compaction: fewer files in curated-compacted/
  [ ] Data integrity: row count unchanged before/after
```

---

## 🧠 Reflection Questions

1. You have columns: `user_id` (10M distinct), `country` (50 values),
   `event_date`, `event_type` (8 values). Which would you partition on and why?

2. The raw zone uses **ingestion time**, the curated zone uses **event time**.
   When could a large gap between the two indicate a pipeline problem?

3. What are the two main consequences of the **small file problem** on query
   performance?

4. A new field `firmware_version` is added to the sensor payload from Jan 20.
   Old files (before Jan 20) don't have this field. What does Spark do when
   reading both old and new files together?

5. You delete the curated checkpoint and restart the pipeline. What happens
   to the raw zone checkpoint? Is data duplicated in the curated zone?

---

## 📖 Key Concepts Quick Reference

| Concept | Definition |
|---|---|
| **Partition pruning** | Skipping directories that don't match query filters |
| **Hive-style partitioning** | `key=value` directories auto-recognised by Spark/Athena/Presto |
| **Predicate pushdown** | Filter evaluated at storage layer (Parquet row group stats) |
| **Column pruning** | Reading only requested columns — skipping all others |
| **Small file problem** | Thousands of tiny files → high metadata overhead |
| **Compaction** | Merging small files into fewer larger ones |
| **Schema-on-read** | Schema applied at query time, not write time |
| **Data swamp** | Data lake without governance: data exists but is undiscoverable |

---

## ➡️ Preview: Session 5

Next session — REST APIs:
- REST principles and HTTP verbs
- JSON data exchange and status codes
- **Lab:** Build a Flask REST API that queries the Parquet data lake
  and returns sensor statistics as JSON

---

## 🔗 Further Reading

- [Apache Parquet file format](https://parquet.apache.org/docs/file-format/)
- [Delta Lake documentation](https://docs.delta.io/latest/index.html)
- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
- Chambers & Zaharia (2018). *Spark: The Definitive Guide*. Chapters 9 & 19.

---

*Course material – Big Data Engineering Programme 2024–2025*  
*Updated 2026-04-28: pyspark 3.4.1 → 3.5.3; Windows compatibility notes added*
