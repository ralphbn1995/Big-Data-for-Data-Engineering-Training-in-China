# Session 5 – APIs & Web Services I: REST Fundamentals
## Flask REST API Exposing Sensor Data

> **Big Data Engineering Programme · Session 5 of 7 · Duration: ~90 minutes**
>
> **Prerequisites:** Sessions 1–4 completed. Kafka cluster running. Optional: Parquet data lake from Session 4.

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.12+ | 3.14 tested; kafka-python-ng required for 3.12+ |
| Java | 11 or 21 | Required for PySpark (`/stats` endpoint); set `JAVA_HOME` |
| Docker | 20+ | For the 3-broker Kafka cluster |
| kafka-python-ng | >=2.0.2 | Replaces `kafka-python` (broken on Python 3.12+) |
| confluent-kafka | >=2.0.0 | Required for Kafka consumer on Python 3.14 |
| pyspark | 3.5.3 | 3.4.x removed from Python 3.12+ (`typing.io` removed) |

---

## Project Structure

```
session5-rest-api/
├── docker-compose.yml              # Same Kafka cluster (Sessions 1–5)
├── requirements.txt                # flask, kafka-python-ng, confluent-kafka, pyspark, pytest
├── run.py                          # ★ Entry point: start the API server
├── sensor_api/
│   ├── __init__.py
│   ├── app.py                      # ★ Flask routes & error handlers
│   ├── kafka_utils.py              # Kafka consumer/producer helpers (confluent-kafka + kafka-python-ng)
│   └── lake_utils.py               # Parquet query helpers (PySpark, lazy singleton)
├── scripts/
│   ├── setup.sh                    # One-shot setup (Linux/macOS)
│   ├── seed_data.py                # ★ Populate Kafka with test readings
│   └── test_api.sh                 # Complete curl test suite (17 tests)
├── tests/
│   └── test_api.py                 # pytest unit tests (25 tests, mocked backends)
└── NOTES.md                        # Bug fixes and verified test output
```

---

## API Endpoints

| Verb | URL | Description | Status Codes |
|---|---|---|---|
| `GET` | `/api/v1/health` | Health check | 200 |
| `GET` | `/api/v1/sensors` | List all sensor types | 200 |
| `GET` | `/api/v1/sensors/{type}/latest` | Latest Kafka reading | 200 / 404 |
| `GET` | `/api/v1/sensors/{type}/stats?days=N` | Daily stats from Parquet | 200 / 400 / 404 |
| `POST` | `/api/v1/readings` | Publish reading to Kafka | 201 / 400 / 422 / 500 |

---

## Quick Start — Linux / macOS

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start Kafka
docker compose up -d
docker compose ps

# 3. Seed Kafka with test data
python scripts/seed_data.py --count 100

# 4. Start the API
python run.py

# 5. In a second terminal — run tests
pytest tests/ -v
./scripts/test_api.sh
```

---

## Quick Start — Windows

```bat
REM 1. Install dependencies
pip install -r requirements.txt

REM 2. Start Kafka
docker compose up -d
docker compose ps

REM 3. Seed Kafka with test data
python scripts/seed_data.py --count 100

REM 4. Start the API
python run.py

REM 5. In a second terminal — unit tests (no Kafka needed)
pytest tests/ -v
```

**Windows notes:**
- `HADOOP_HOME` is set automatically in `lake_utils.py` to `C:\hadoop` before PySpark starts.
  Download `winutils.exe` from the hadoop-winutils repo and place it in `C:\hadoop\bin\`.
- The `/stats` endpoint requires Java 11+ installed and `JAVA_HOME` pointing to it.
  Without Java, `/stats` returns `{"count": 0, "data": []}` gracefully — no crash.
- Default data lake path on Windows: `C:/tmp/datalake/curated/domain=iot`
  (override with `python run.py --lake-path D:/mypath`).

---

## Lab Steps — Detailed

### Step 0 — Setup

```bash
pip install -r requirements.txt

# Verify Kafka is running
docker compose up -d
docker compose ps

# Check data lake from Session 4 (optional — needed for /stats)
# Linux/macOS:
ls /tmp/datalake/curated/domain=iot/
# Windows:
dir C:\tmp\datalake\curated\domain=iot\
```

---

### Step 1 — Health Check

**Endpoint:** `GET /api/v1/health`

```bash
curl -s http://localhost:5000/api/v1/health | python3 -m json.tool
```

**Expected response:**

```json
{
  "service": "sensor-data-api",
  "status": "ok",
  "timestamp": "2026-04-28T11:15:15.898569+00:00",
  "version": "1.0"
}
```

> The health check is mandatory in production — load balancers call it every few
> seconds to decide whether to route traffic to this instance.

---

### Step 2 — List Sensors & Latest Reading

```bash
# List all sensor types
curl -s http://localhost:5000/api/v1/sensors | python3 -m json.tool

# Latest reading (requires seed_data.py to have been run)
curl -s http://localhost:5000/api/v1/sensors/temperature/latest | python3 -m json.tool
curl -s http://localhost:5000/api/v1/sensors/humidity/latest    | python3 -m json.tool
curl -s http://localhost:5000/api/v1/sensors/pressure/latest    | python3 -m json.tool

# Invalid sensor type → 404
curl -s http://localhost:5000/api/v1/sensors/radar/latest | python3 -m json.tool
```

**Expected /latest response (after seeding):**

```json
{
  "data": {
    "device_id": "temp-02",
    "event_time": "2026-04-28T11:13:57.155000+00:00",
    "offset": 616,
    "partition": 2,
    "sensor_type": "temperature",
    "timestamp": 1777374837155,
    "unit": "C",
    "value": 28.08
  },
  "status": "success"
}
```

The `/latest` endpoint queries Kafka directly using a temporary consumer that
seeks to `end_offset - 30` per partition, then filters for the requested sensor type.

---

### Step 3 — Statistics from the Parquet Data Lake

Requires Session 4 Parquet data. Without it, returns `count: 0, data: []`.

```bash
# Default (last 7 days)
curl -s "http://localhost:5000/api/v1/sensors/temperature/stats" | python3 -m json.tool

# Custom window (last 3 days)
curl -s "http://localhost:5000/api/v1/sensors/temperature/stats?days=3" | python3 -m json.tool

# Invalid parameter → 400
curl -s "http://localhost:5000/api/v1/sensors/temperature/stats?days=abc" | python3 -m json.tool
```

**Expected response (with Session 4 data):**

```json
{
  "status": "success",
  "sensor_type": "temperature",
  "days": 7,
  "count": 3,
  "data": [
    {
      "date": "2026-04-28",
      "sensor_type": "temperature",
      "record_count": 148,
      "avg_value": 27.34,
      "min_value": 10.20,
      "max_value": 39.80,
      "anomaly_count": 12,
      "anomaly_pct": 8.11
    }
  ]
}
```

**Expected response (no Session 4 data, or no Java):**
```json
{"count": 0, "data": [], "days": 7, "sensor_type": "temperature", "status": "success"}
```

---

### Step 4 — POST: Write a Reading to Kafka

```bash
# Valid request → 201 Created
curl -s -X POST http://localhost:5000/api/v1/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor":"temperature","value":29.3,"unit":"C"}' \
  | python3 -m json.tool

# Missing 'value' field → 400 Bad Request
curl -s -X POST http://localhost:5000/api/v1/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor":"temperature"}' \
  | python3 -m json.tool

# Invalid sensor type → 422 Unprocessable Entity
curl -s -X POST http://localhost:5000/api/v1/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor":"radar","value":29.3}' \
  | python3 -m json.tool

# No Content-Type header → 400
curl -s -X POST http://localhost:5000/api/v1/readings \
  -d '{"sensor":"temperature","value":29.3}'
```

**Expected 201 response:**

```json
{
  "data": {
    "offset": 634,
    "partition": 2,
    "reading": {
      "device_id": "api",
      "sensor": "temperature",
      "source": "api-v1",
      "timestamp": 1777375406267,
      "unit": "C",
      "value": 29.3
    }
  },
  "message": "Reading published to Kafka.",
  "status": "success"
}
```

**Validation order (important for understanding 400 vs 422):**

1. Is the body valid JSON? → `400` if not
2. Are required fields (`sensor`, `value`) present? → `400` if missing
3. Is sensor type in `{temperature, humidity, pressure}`? → `422` if not
4. Is `value` numeric? → `422` if not
5. Is `value` within the physical range for that sensor? → `422` if not

---

### Step 5 — Error Handlers

All errors return JSON (never HTML):

```bash
# Unknown URL → 404
curl -s http://localhost:5000/api/v1/does-not-exist | python3 -m json.tool

# Wrong method (GET on a POST-only endpoint) → 405
curl -s -X GET http://localhost:5000/api/v1/readings | python3 -m json.tool
```

---

### Step 6 — Run the Full Test Suite

```bash
# Python unit tests (no server or Kafka needed — mocks everything)
pytest tests/ -v
# Expected: 25 passed

# Bash test suite (requires server running on port 5000)
./scripts/test_api.sh
# Expected: 17 tests, pass/fail per test
```

---

## Testing Checklist

```
Setup
  [x] pip install -r requirements.txt → kafka-python-ng, confluent-kafka, pyspark==3.5.3
  [x] docker compose ps → all 4 containers Up
  [x] python scripts/seed_data.py --count 100 → 100 messages sent
  [x] python run.py → server starts on port 5000

GET /health
  [x] Returns 200
  [x] Body contains status, version, timestamp, service
  [x] Content-Type is application/json

GET /sensors
  [x] Returns 200 with ["humidity","pressure","temperature"]
  [x] count matches data field length

GET /sensors/{type}/latest
  [x] temperature/latest → 200 with reading dict
  [x] humidity/latest   → 200 with reading dict
  [x] pressure/latest   → 200 with reading dict
  [x] radar/latest      → 404 JSON (not HTML)

GET /sensors/{type}/stats
  [x] ?days=7 → 200 (count=0 if no Session 4 data — not an error)
  [x] ?days=abc → 400 with error message
  [ ] ?days=0 → 400
  [ ] ?days=91 → 400
  [ ] radar/stats → 404

POST /readings
  [x] Valid JSON → 201 with partition + offset
  [ ] Missing 'value' → 400
  [ ] Missing 'sensor' → 400
  [ ] sensor=radar → 422
  [ ] value="hot" → 422
  [ ] value=999.9 (temp) → 422 (out of range)
  [ ] No Content-Type → 400

Error handlers
  [x] /api/v1/nonexistent → 404 JSON (not HTML)
  [x] GET /api/v1/readings → 405 JSON

Unit tests
  [x] pytest tests/ -v → 25 passed
```

---

## Reflection Questions

1. A client sends `GET /api/v1/sensors/temperature/stats` with no `days` parameter.
   The default is 7. Should the server return 200 or 400? Justify your answer.

2. Explain why `POST /readings` is not idempotent but `PUT /readings/42`
   (replacing reading 42) is. Give a concrete scenario where this distinction
   matters in a retry mechanism.

3. A colleague suggests returning `200 OK` for all responses and putting the status
   code in the JSON body (`{"status": 404, ...}`). What are the problems?

4. What is the difference between `400 Bad Request` and `422 Unprocessable Entity`?
   Give one example of each for the `POST /readings` endpoint.

5. The `/sensors/temperature/latest` endpoint queries Kafka. If the Kafka cluster
   is down, what HTTP status code should the API return? What should the body look like?

---

## HTTP Status Codes Quick Reference

| Code | Name | Use |
|---|---|---|
| `200` | OK | Successful GET, PUT, PATCH |
| `201` | Created | Successful POST (new resource created) |
| `204` | No Content | Successful DELETE with no body |
| `400` | Bad Request | Malformed JSON, missing required field |
| `401` | Unauthorized | Authentication required |
| `403` | Forbidden | Authenticated but not authorised |
| `404` | Not Found | Resource does not exist |
| `422` | Unprocessable Entity | Valid JSON but business validation failed |
| `500` | Internal Server Error | Unhandled exception on server |
| `503` | Service Unavailable | Dependency (Kafka, Spark) is down |

---

## Preview: Session 6

Next session — API Security:
- API key and JWT Bearer token authentication
- Rate limiting to protect against abuse
- OpenAPI / Swagger auto-generated documentation
- **Lab:** Secure the Session 5 API; consume a third-party weather API to enrich sensor data

---

## Further Reading

- [Flask documentation](https://flask.palletsprojects.com/en/3.0.x/)
- [HTTP Status Codes (MDN)](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status)
- [Richardson Maturity Model](https://martinfowler.com/articles/richardsonMaturityModel.html)
- Fielding, R.T. (2000). *Architectural Styles...* PhD thesis. Chapter 5 (REST).
- Masse, M. (2011). *REST API Design Rulebook*. O'Reilly.

---

*Course material – Big Data Engineering Programme 2024–2025*
