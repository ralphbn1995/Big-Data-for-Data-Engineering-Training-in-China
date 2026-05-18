"""
app.py – Session 5 Lab: Sensor Data REST API
=============================================
Flask REST API exposing sensor data from:
  - Kafka (live readings via the /latest endpoint)
  - Parquet data lake (historical stats via the /stats endpoint)

Endpoints:
  GET  /api/v1/health
  GET  /api/v1/sensors
  GET  /api/v1/sensors/<sensor_type>/latest
  GET  /api/v1/sensors/<sensor_type>/stats?days=N
  POST /api/v1/readings

Usage:
    python sensor_api/app.py
    # or:
    flask --app sensor_api/app.py run --host 0.0.0.0 --port 5000 --debug

Test with curl:
    curl -s http://localhost:5000/api/v1/health | python3 -m json.tool
"""

import logging
from datetime import datetime, timezone

from flask import Flask, jsonify, request

from .kafka_utils import get_latest_readings, publish_reading
from .lake_utils import get_sensor_types, get_statistics

# ── Application factory ───────────────────────────────────────────────────────
app = Flask(__name__)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)

# ── Constants ─────────────────────────────────────────────────────────────────
API_VERSION  = "1.0"
API_PREFIX   = "/api/v1"
VALID_SENSORS = {"temperature", "humidity", "pressure"}


# ══════════════════════════════════════════════════════════════════════════════
#  ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

# ── GET /health ───────────────────────────────────────────────────────────────
@app.route(f"{API_PREFIX}/health")
def health():
    """
    Health check endpoint.
    Used by load balancers and monitoring systems.
    Always returns 200 if the process is alive.
    """
    return jsonify({
        "status":    "ok",
        "version":   API_VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service":   "sensor-data-api",
    }), 200


# ── GET /sensors ──────────────────────────────────────────────────────────────
@app.route(f"{API_PREFIX}/sensors")
def list_sensors():
    """
    GET /api/v1/sensors
    Returns the list of all known sensor types discovered in the data lake.
    """
    try:
        sensor_types = get_sensor_types()
        return jsonify({
            "status": "success",
            "count":  len(sensor_types),
            "data":   sensor_types,
        }), 200
    except Exception as exc:
        app.logger.error("list_sensors error: %s", exc, exc_info=True)
        return jsonify({
            "status":  "error",
            "message": "Failed to retrieve sensor list.",
        }), 500


# ── GET /sensors/<type>/latest ────────────────────────────────────────────────
@app.route(f"{API_PREFIX}/sensors/<sensor_type>/latest")
def latest_reading(sensor_type: str):
    """
    GET /api/v1/sensors/{sensor_type}/latest
    Returns the most recent Kafka reading for the given sensor type.

    Path parameter:
      sensor_type: one of temperature, humidity, pressure

    Returns:
      200 + reading data
      404 if sensor_type is unknown or no data available
    """
    if sensor_type not in VALID_SENSORS:
        return jsonify({
            "status":  "error",
            "code":    404,
            "message": (
                f"Unknown sensor type '{sensor_type}'. "
                f"Valid types: {sorted(VALID_SENSORS)}"
            ),
        }), 404

    try:
        readings = get_latest_readings(sensor_type, n=1)
        if not readings:
            return jsonify({
                "status":  "error",
                "code":    404,
                "message": f"No readings available for sensor '{sensor_type}'.",
            }), 404

        return jsonify({
            "status": "success",
            "data":   readings[0],
        }), 200

    except Exception as exc:
        app.logger.error("latest_reading error: %s", exc, exc_info=True)
        return jsonify({
            "status":  "error",
            "message": "Failed to retrieve latest reading.",
        }), 500


# ── GET /sensors/<type>/stats ─────────────────────────────────────────────────
@app.route(f"{API_PREFIX}/sensors/<sensor_type>/stats")
def sensor_stats(sensor_type: str):
    """
    GET /api/v1/sensors/{sensor_type}/stats?days=N
    Returns daily statistics from the Parquet data lake.

    Path parameter:
      sensor_type: one of temperature, humidity, pressure

    Query parameter:
      days (int, optional, default 7): number of recent days [1–90]

    Returns:
      200 + stats array
      400 if days parameter is invalid
      404 if sensor_type is unknown
    """
    if sensor_type not in VALID_SENSORS:
        return jsonify({
            "status":  "error",
            "code":    404,
            "message": f"Unknown sensor type '{sensor_type}'.",
        }), 404

    # Validate query parameter
    try:
        days = int(request.args.get("days", 7))
        if not (1 <= days <= 90):
            raise ValueError("days must be between 1 and 90")
    except (ValueError, TypeError) as exc:
        return jsonify({
            "status":  "error",
            "code":    400,
            "message": f"Invalid 'days' parameter: {exc}",
        }), 400

    try:
        stats = get_statistics(sensor_type, days=days)
        return jsonify({
            "status":      "success",
            "sensor_type": sensor_type,
            "days":        days,
            "count":       len(stats),
            "data":        stats,
        }), 200

    except Exception as exc:
        app.logger.error("sensor_stats error: %s", exc, exc_info=True)
        return jsonify({
            "status":  "error",
            "message": "Failed to retrieve statistics.",
        }), 500


# ── POST /readings ────────────────────────────────────────────────────────────
REQUIRED_FIELDS = {"sensor", "value"}

@app.route(f"{API_PREFIX}/readings", methods=["POST"])
def create_reading():
    """
    POST /api/v1/readings
    Publish a new sensor reading to the Kafka topic.

    Request body (JSON):
      {
        "sensor": "temperature",   (required)
        "value":  28.5,            (required, numeric)
        "unit":   "C"              (optional)
      }

    Returns:
      201 + published message metadata
      400 if body is malformed or missing required fields
      422 if field values are semantically invalid
      500 if Kafka publish fails
    """
    # Step 1: Is the body valid JSON?
    body = request.get_json(silent=True)
    if body is None:
        return jsonify({
            "status":  "error",
            "code":    400,
            "message": (
                "Request body must be valid JSON. "
                "Set header: Content-Type: application/json"
            ),
        }), 400

    # Step 2: Are required fields present?
    missing = REQUIRED_FIELDS - set(body.keys())
    if missing:
        return jsonify({
            "status":  "error",
            "code":    400,
            "message": f"Missing required fields: {sorted(missing)}",
        }), 400

    # Step 3: Is sensor type valid?
    sensor = body["sensor"]
    if sensor not in VALID_SENSORS:
        return jsonify({
            "status":  "error",
            "code":    422,
            "message": (
                f"Invalid sensor type '{sensor}'. "
                f"Allowed: {sorted(VALID_SENSORS)}"
            ),
        }), 422

    # Step 4: Is value numeric?
    try:
        value = float(body["value"])
    except (ValueError, TypeError):
        return jsonify({
            "status":  "error",
            "code":    422,
            "message": "'value' must be a numeric type.",
        }), 422

    # Step 5: Business-rule range check
    VALUE_RANGES = {
        "temperature": (-50.0,  80.0),
        "humidity":    (  0.0, 100.0),
        "pressure":    (800.0, 1200.0),
    }
    lo, hi = VALUE_RANGES[sensor]
    if not (lo <= value <= hi):
        return jsonify({
            "status":  "error",
            "code":    422,
            "message": (
                f"Value {value} is out of range for sensor "
                f"'{sensor}' [{lo}, {hi}]."
            ),
        }), 422

    # Step 6: Build message and publish to Kafka
    reading = {
        "sensor":    sensor,
        "value":     value,
        "unit":      body.get("unit", ""),
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        "source":    "api-v1",
        "device_id": body.get("device_id", "api"),
    }

    try:
        meta = publish_reading(reading)
        return jsonify({
            "status":  "success",
            "message": "Reading published to Kafka.",
            "data": {
                "reading":   reading,
                "partition": meta["partition"],
                "offset":    meta["offset"],
            },
        }), 201

    except Exception as exc:
        app.logger.error("create_reading Kafka error: %s", exc, exc_info=True)
        return jsonify({
            "status":  "error",
            "message": "Failed to publish reading to Kafka.",
        }), 500


# ══════════════════════════════════════════════════════════════════════════════
#  GLOBAL ERROR HANDLERS
#  Without these, Flask returns HTML error pages — not useful for API consumers
# ══════════════════════════════════════════════════════════════════════════════

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status":  "error",
        "code":    404,
        "message": "The requested resource was not found.",
    }), 404


@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        "status":  "error",
        "code":    405,
        "message": "HTTP method not allowed for this endpoint.",
        "allowed": error.valid_methods if hasattr(error, "valid_methods") else [],
    }), 405


@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "status":  "error",
        "code":    500,
        "message": "An internal server error occurred.",
    }), 500


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
