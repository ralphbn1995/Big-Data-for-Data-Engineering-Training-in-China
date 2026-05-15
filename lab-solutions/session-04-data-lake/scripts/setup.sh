#!/usr/bin/env bash
# ============================================================
#  scripts/setup.sh – Session 4 one-shot environment setup
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================================="
echo "  Session 4 – Data Lake Setup"
echo "=================================================="
echo ""

# ── Docker ───────────────────────────────────────────────────
echo "▶  Checking Docker…"
docker --version && docker compose version

echo ""
echo "▶  Starting Kafka cluster…"
cd "$PROJECT_DIR"
docker compose up -d
sleep 30
docker compose ps

# ── Kafka topic ──────────────────────────────────────────────
echo ""
echo "▶  Ensuring sensor-events topic exists…"
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create --if-not-exists \
  --topic sensor-events \
  --partitions 3 --replication-factor 3

docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events

# ── Python venv ───────────────────────────────────────────────
echo ""
echo "▶  Python virtual environment…"
cd "$PROJECT_DIR"
python3 -m venv venv
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet pyspark==3.4.1 kafka-python
echo "✅  Python deps installed."

# ── Data lake directories ────────────────────────────────────
echo ""
echo "▶  Creating data lake directory structure…"
mkdir -p /tmp/datalake/{raw,curated,consumption}
mkdir -p /tmp/datalake/raw/source=kafka/topic=sensor-events
mkdir -p /tmp/datalake/curated/domain=iot
mkdir -p /tmp/datalake/consumption/use_case=sensor_averages
mkdir -p /tmp/datalake-ckpt/{raw,curated}

echo "  Directory tree:"
find /tmp/datalake -type d | sort

echo ""
echo "=================================================="
echo "  ✅  Setup complete!"
echo ""
echo "  Workflow:"
echo "  1. Start pipeline:"
echo "       source venv/bin/activate"
echo "       python python/datalake_pipeline.py"
echo ""
echo "  2. Produce messages (another terminal):"
echo "       python python/producer.py --count 500"
echo ""
echo "  3. After 2–3 batches (each 30 s), stop pipeline (Ctrl+C)."
echo ""
echo "  4. Build Gold layer:"
echo "       python python/consumption_zone.py"
echo ""
echo "  5. Run Spark SQL queries:"
echo "       python python/query_lake.py"
echo ""
echo "  6. Explore the lake structure:"
echo "       python python/explore_lake.py"
echo ""
echo "  7. Compact small files:"
echo "       python python/compaction.py"
echo "=================================================="
