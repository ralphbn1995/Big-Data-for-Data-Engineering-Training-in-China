"""
tests/test_api.py – Session 5: Unit tests for the Flask API
============================================================
Tests all endpoints using Flask's built-in test client.
Kafka and PySpark calls are mocked so tests run offline.

Usage:
    pip install pytest
    pytest tests/ -v
"""

import json
import sys
import os
from unittest.mock import patch, MagicMock

import pytest

# Allow importing from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sensor_api.app import app


# ── Test client fixture ───────────────────────────────────────────────────────
@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


# ══════════════════════════════════════════════════════════════════════════════
#  HEALTH
# ══════════════════════════════════════════════════════════════════════════════

class TestHealth:
    def test_health_returns_200(self, client):
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200

    def test_health_json_fields(self, client):
        resp = client.get("/api/v1/health")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "version" in data
        assert "timestamp" in data
        assert "service" in data

    def test_health_content_type(self, client):
        resp = client.get("/api/v1/health")
        assert "application/json" in resp.content_type


# ══════════════════════════════════════════════════════════════════════════════
#  SENSOR LIST
# ══════════════════════════════════════════════════════════════════════════════

class TestSensorList:
    @patch("sensor_api.app.get_sensor_types", return_value=["humidity", "pressure", "temperature"])
    def test_list_sensors_200(self, mock_fn, client):
        resp = client.get("/api/v1/sensors")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["count"] == 3
        assert "temperature" in data["data"]

    @patch("sensor_api.app.get_sensor_types", side_effect=RuntimeError("disk error"))
    def test_list_sensors_500_on_error(self, mock_fn, client):
        resp = client.get("/api/v1/sensors")
        assert resp.status_code == 500
        data = resp.get_json()
        assert data["status"] == "error"


# ══════════════════════════════════════════════════════════════════════════════
#  LATEST READING
# ══════════════════════════════════════════════════════════════════════════════

MOCK_READING = {
    "sensor_type": "temperature",
    "value": 28.5,
    "unit": "C",
    "device_id": "temp-01",
    "timestamp": 1700000000000,
    "event_time": "2024-01-15T10:00:00+00:00",
    "partition": 0,
    "offset": 42,
}


class TestLatestReading:
    @patch("sensor_api.app.get_latest_readings", return_value=[MOCK_READING])
    def test_valid_sensor_returns_200(self, mock_fn, client):
        resp = client.get("/api/v1/sensors/temperature/latest")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["data"]["sensor_type"] == "temperature"
        assert data["data"]["value"] == 28.5

    def test_invalid_sensor_returns_404(self, client):
        resp = client.get("/api/v1/sensors/radar/latest")
        assert resp.status_code == 404
        data = resp.get_json()
        assert data["status"] == "error"

    @patch("sensor_api.app.get_latest_readings", return_value=[])
    def test_no_data_returns_404(self, mock_fn, client):
        resp = client.get("/api/v1/sensors/temperature/latest")
        assert resp.status_code == 404

    @patch("sensor_api.app.get_latest_readings", side_effect=RuntimeError("Kafka down"))
    def test_kafka_error_returns_500(self, mock_fn, client):
        resp = client.get("/api/v1/sensors/temperature/latest")
        assert resp.status_code == 500


# ══════════════════════════════════════════════════════════════════════════════
#  STATS
# ══════════════════════════════════════════════════════════════════════════════

MOCK_STATS = [
    {
        "date": "2024-01-15",
        "sensor_type": "temperature",
        "record_count": 120,
        "avg_value": 27.3,
        "min_value": 10.2,
        "max_value": 39.8,
        "anomaly_count": 8,
        "anomaly_pct": 6.67,
    }
]


class TestStats:
    @patch("sensor_api.app.get_statistics", return_value=MOCK_STATS)
    def test_valid_stats_returns_200(self, mock_fn, client):
        resp = client.get("/api/v1/sensors/temperature/stats")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["sensor_type"] == "temperature"
        assert data["days"] == 7        # default
        assert data["count"] == 1

    @patch("sensor_api.app.get_statistics", return_value=MOCK_STATS)
    def test_custom_days_parameter(self, mock_fn, client):
        resp = client.get("/api/v1/sensors/temperature/stats?days=3")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["days"] == 3
        mock_fn.assert_called_once_with("temperature", days=3)

    def test_invalid_sensor_returns_404(self, client):
        resp = client.get("/api/v1/sensors/radar/stats")
        assert resp.status_code == 404

    def test_non_integer_days_returns_400(self, client):
        resp = client.get("/api/v1/sensors/temperature/stats?days=abc")
        assert resp.status_code == 400
        data = resp.get_json()
        assert data["code"] == 400

    def test_days_zero_returns_400(self, client):
        resp = client.get("/api/v1/sensors/temperature/stats?days=0")
        assert resp.status_code == 400

    def test_days_91_returns_400(self, client):
        resp = client.get("/api/v1/sensors/temperature/stats?days=91")
        assert resp.status_code == 400


# ══════════════════════════════════════════════════════════════════════════════
#  POST /readings
# ══════════════════════════════════════════════════════════════════════════════

MOCK_META = {"topic": "sensor-events", "partition": 1, "offset": 99}


class TestCreateReading:
    @patch("sensor_api.app.publish_reading", return_value=MOCK_META)
    def test_valid_post_returns_201(self, mock_fn, client):
        resp = client.post(
            "/api/v1/readings",
            data=json.dumps({"sensor": "temperature", "value": 29.3, "unit": "C"}),
            content_type="application/json",
        )
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["data"]["partition"] == 1
        assert data["data"]["offset"] == 99

    def test_missing_value_field_returns_400(self, client):
        resp = client.post(
            "/api/v1/readings",
            data=json.dumps({"sensor": "temperature"}),
            content_type="application/json",
        )
        assert resp.status_code == 400
        data = resp.get_json()
        assert "value" in data["message"].lower() or "missing" in data["message"].lower()

    def test_missing_sensor_field_returns_400(self, client):
        resp = client.post(
            "/api/v1/readings",
            data=json.dumps({"value": 29.3}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_invalid_sensor_type_returns_422(self, client):
        resp = client.post(
            "/api/v1/readings",
            data=json.dumps({"sensor": "radar", "value": 29.3}),
            content_type="application/json",
        )
        assert resp.status_code == 422

    def test_non_numeric_value_returns_422(self, client):
        resp = client.post(
            "/api/v1/readings",
            data=json.dumps({"sensor": "temperature", "value": "hot"}),
            content_type="application/json",
        )
        assert resp.status_code == 422

    def test_value_out_of_range_returns_422(self, client):
        resp = client.post(
            "/api/v1/readings",
            data=json.dumps({"sensor": "temperature", "value": 999.9}),
            content_type="application/json",
        )
        assert resp.status_code == 422

    def test_non_json_body_returns_400(self, client):
        resp = client.post(
            "/api/v1/readings",
            data="sensor=temperature&value=29.3",
            content_type="application/x-www-form-urlencoded",
        )
        assert resp.status_code == 400

    @patch("sensor_api.app.publish_reading", side_effect=Exception("Kafka unavailable"))
    def test_kafka_error_returns_500(self, mock_fn, client):
        resp = client.post(
            "/api/v1/readings",
            data=json.dumps({"sensor": "temperature", "value": 29.3}),
            content_type="application/json",
        )
        assert resp.status_code == 500


# ══════════════════════════════════════════════════════════════════════════════
#  GLOBAL ERROR HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

class TestErrorHandlers:
    def test_unknown_url_returns_404_json(self, client):
        resp = client.get("/api/v1/does-not-exist")
        assert resp.status_code == 404
        data = resp.get_json()
        assert data["status"] == "error"
        assert "application/json" in resp.content_type

    def test_wrong_method_returns_405_json(self, client):
        resp = client.get("/api/v1/readings")   # GET not allowed
        assert resp.status_code == 405
        data = resp.get_json()
        assert data["status"] == "error"
        assert "application/json" in resp.content_type
