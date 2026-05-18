#!/usr/bin/env bash
# ============================================================
#  scripts/setup.sh – Session 5 one-shot setup
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================================="
echo "  Session 5 – REST API Setup"
echo "=================================================="
echo ""

# ── Docker ───────────────────────────────────────────────────
echo "▶  Checking Docker…"
docker --version
docker compose version

echo ""
echo "▶  Starting Kafka cluster…"
cd "$PROJECT_DIR"
docker compose up -d
sleep 20
docker compose ps

# ── Python venv ───────────────────────────────────────────────
echo ""
echo "▶  Setting up Python environment…"
cd "$PROJECT_DIR"
python3 -m venv venv
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet flask kafka-python pyspark==3.4.1 requests
echo "✅  Dependencies installed."

# ── Check data lake from Session 4 ──────────────────────────
echo ""
echo "▶  Checking data lake (from Session 4)…"
if ls /tmp/datalake/curated/domain=iot/ 2>/dev/null | grep -q "sensor_type="; then
    echo "  ✅  Curated zone found with data."
else
    echo "  ⚠   No data lake found at /tmp/datalake/curated/"
    echo "      The API will still start, but /stats endpoints"
    echo "      will return empty results."
    echo "      → Run Session 4 pipeline first for full functionality."
fi

# ── Ensure sensor-events topic exists ───────────────────────
echo ""
echo "▶  Ensuring sensor-events topic exists…"
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create --if-not-exists \
  --topic sensor-events \
  --partitions 3 --replication-factor 3 2>/dev/null
echo "  ✅  Topic ready."

echo ""
echo "=================================================="
echo "  ✅  Setup complete!"
echo ""
echo "  1. Activate venv:"
echo "       source venv/bin/activate"
echo ""
echo "  2. (Optional) Populate sensor data:"
echo "       python scripts/seed_data.py"
echo ""
echo "  3. Start the API:"
echo "       python run.py"
echo ""
echo "  4. Test with curl (in another terminal):"
echo "       chmod +x scripts/test_api.sh && ./scripts/test_api.sh"
echo "=================================================="
