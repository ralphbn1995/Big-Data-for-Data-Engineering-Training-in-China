# Session 5 – Bug Fixes & Lab Notes

## Bugs Fixed

### Bug 1 — `kafka-python` incompatible with Python 3.12+ (Critical)

**File:** `requirements.txt`

**Symptom:**
```
ModuleNotFoundError: No module named 'kafka.vendor.six.moves'
```
All scripts fail on import when using `kafka-python==2.0.2` on Python 3.12+.

**Fix:** Replace with the maintained fork:
```
# Before
kafka-python==2.0.2

# After
kafka-python-ng>=2.0.2   # producer (Python 3.12+ compatible fork)
confluent-kafka>=2.0.0   # consumer in kafka_utils
```

---

### Bug 2 — `enable_idempotence` not supported in kafka-python-ng (High)

**Files:** `sensor_api/kafka_utils.py`, `scripts/seed_data.py`

**Symptom:**
```
AssertionError: Unrecognized configs: {'enable_idempotence': True}
```
`KafkaProducer` constructor raises immediately on startup.

**Fix:** Remove `enable_idempotence=True` and `max_in_flight_requests_per_connection=1`.
`acks='all'` + `retries=3` provides at-least-once durability without idempotence.

```python
# Before
producer = KafkaProducer(
    ...
    enable_idempotence=True,
    max_in_flight_requests_per_connection=1,
    ...
)

# After
producer = KafkaProducer(
    ...
    acks="all",
    retries=3,
    ...
)
```

---

### Bug 3 — `KafkaConsumer` crashes on Python 3.14 (Critical)

**File:** `sensor_api/kafka_utils.py`

**Symptom:**
```
ValueError: Invalid file descriptor: -1
  File "kafka/client_async.py", line 217, in _selector
```
Python 3.14 changed the `selectors` module; kafka-python-ng's internal selector
setup breaks on `selectors.DefaultSelector()`.

**Fix:** Rewrite `get_latest_readings()` using `confluent-kafka` (`CKConsumer`).
Key API differences:
- Config uses dot-notation strings: `'bootstrap.servers'`, `'enable.auto.commit'`
- `consumer.poll(timeout)` returns one message (not an iterator)
- `msg.partition()` and `msg.offset()` are method calls, not attributes
- Use `consumer.assign([TopicPartition(topic, p, start_offset)])` for mid-stream seeks

---

### Bug 4 — Sort crashes when `timestamp` value is `None` (Medium)

**File:** `sensor_api/kafka_utils.py` — `get_latest_readings()`

**Symptom:**
```
TypeError: '<' not supported between instances of 'NoneType' and 'int'
```
`dict.get("timestamp", 0)` returns `None` (not `0`) when the key exists but has value
`None`. The sort then tries to compare `None` with `int`, which fails in Python 3.

**Fix:**
```python
# Before
results.sort(key=lambda r: r.get("timestamp", 0), reverse=True)

# After
results.sort(key=lambda r: r.get("timestamp") or 0, reverse=True)
```
`or 0` treats both missing key and `None` value as `0`.

---

### Bug 5 — PySpark startup crashes when Java is not installed (High)

**File:** `sensor_api/lake_utils.py` — `_get_spark()` and `get_statistics()`

**Symptom:**
```
pyspark.errors.exceptions.base.PySparkRuntimeError: [JAVA_GATEWAY_EXITED]
Java gateway process exited before sending its port number.
```
`_get_spark()` only caught `ImportError` (PySpark not installed). When PySpark is
installed but Java is absent, a `PySparkRuntimeError` propagated unhandled, causing
`GET /stats` to return 500 instead of a graceful empty response.

Two sub-issues:
1. `except ImportError` was too narrow — should catch any startup failure.
2. The `spark = _get_spark()` call was outside the `try` block in `get_statistics()`,
   so the outer `except Exception` in the Flask route caught it and returned 500.

**Fix — `_get_spark()`:**
```python
# Before
except ImportError:
    _spark = None

# After
except Exception:
    _spark = None   # PySpark/Java unavailable → return [] from get_statistics
```

**Fix — `get_statistics()`:** Move `_get_spark()` inside the try block:
```python
try:
    spark = _get_spark()
    if spark is None:
        return []
    ...
except Exception as exc:
    logging.getLogger(__name__).error("get_statistics error: %s", exc, exc_info=True)
    return []
```

---

### Bug 6 — `HADOOP_HOME` not set; Windows path not platform-aware (Medium)

**Files:** `sensor_api/lake_utils.py`, `run.py`

**Symptom (Windows):**
```
UnsatisfiedLinkError: NativeIO$Windows.access0
```
PySpark's JVM cannot find `hadoop.dll`. The JVM rejects bash-style paths like
`/c/hadoop`; a Windows absolute path is required.

**Fix — `lake_utils.py`** (runs before PySpark import):
```python
if os.name == "nt":
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin" + os.pathsep + os.environ.get("PATH", "")
```

**Fix — default paths:** use `C:/tmp/` on Windows, `/tmp/` elsewhere:
```python
_default_lake = (
    "C:/tmp/datalake/curated/domain=iot" if os.name == "nt"
    else "/tmp/datalake/curated/domain=iot"
)
```

---

### Bug 7 — `UnicodeEncodeError` on Windows for emoji in `seed_data.py` (Medium)

**File:** `scripts/seed_data.py`

**Symptom:**
```
UnicodeEncodeError: 'charmap' codec can't encode character '✅'
```
Windows defaults to cp1252 which cannot encode Unicode emoji (`✅`, `⚡`).

**Fix:**
```python
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
```

---

## Files Modified

| File | Change |
|---|---|
| `requirements.txt` | `kafka-python` → `kafka-python-ng`; add `confluent-kafka`; `pyspark==3.5.3` |
| `sensor_api/kafka_utils.py` | Remove `enable_idempotence`; rewrite consumer with confluent-kafka; fix sort |
| `sensor_api/lake_utils.py` | Broaden Spark exception catch; move `_get_spark()` into try; HADOOP_HOME fix; platform paths |
| `run.py` | Platform-aware default `--lake-path` |
| `scripts/seed_data.py` | Remove `enable_idempotence`; add `sys.stdout.reconfigure` |

---

## Verified Test Results

### pytest (25/25)
```
pytest tests/ -v
...
25 passed in 3.88s
```
All tests use mocked Kafka and Spark — no live cluster needed.

### Live API Tests (port 5002, Python 3.14, Windows 11)

```
GET /api/v1/health
→ 200 {"service":"sensor-data-api","status":"ok","timestamp":"...","version":"1.0"}

GET /api/v1/sensors
→ 200 {"count":3,"data":["humidity","pressure","temperature"],"status":"success"}

GET /api/v1/sensors/temperature/latest   (after seed_data.py --count 100)
→ 200 {"data":{"device_id":"temp-02","event_time":"2026-04-28T11:13:57.155000+00:00",
       "offset":616,"partition":2,"sensor_type":"temperature",
       "timestamp":1777374837155,"unit":"C","value":28.08},"status":"success"}

GET /api/v1/sensors/humidity/latest
→ 200 {"data":{"device_id":"humi-04","unit":"%","value":88.47,...},"status":"success"}

GET /api/v1/sensors/pressure/latest
→ 200 {"data":{"device_id":"pres-03","unit":"hPa","value":1022.67,...},"status":"success"}

GET /api/v1/sensors/temperature/stats?days=7   (no Session 4 Parquet data)
→ 200 {"count":0,"data":[],"days":7,"sensor_type":"temperature","status":"success"}

GET /api/v1/sensors/temperature/stats?days=abc
→ 400 {"code":400,"message":"Invalid 'days' parameter: invalid literal...","status":"error"}

GET /api/v1/sensors/radar/latest
→ 404 {"code":404,"message":"Unknown sensor type 'radar'...","status":"error"}

POST /api/v1/readings  {"sensor":"temperature","value":29.3,"unit":"C"}
→ 201 {"data":{"offset":634,"partition":2,"reading":{...}},"message":"Reading published to Kafka.","status":"success"}

GET /api/v1/readings
→ 405 {"allowed":["OPTIONS","POST"],"code":405,"message":"HTTP method not allowed...","status":"error"}

GET /api/v1/does-not-exist
→ 404 {"code":404,"message":"The requested resource was not found.","status":"error"}
```

### seed_data.py
```
python scripts/seed_data.py --count 100
Seeding Kafka topic 'sensor-events' with 100 messages...
  50/100 sent...
  100/100 sent...
100 messages sent to 'sensor-events'.
```
