#!/usr/bin/env bash
# ============================================================
#  scripts/setup.sh
#  One-shot environment setup for Session 3
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================================="
echo "  Session 3 – Environment Setup"
echo "=================================================="
echo ""

# ── Step 1: Verify Docker ────────────────────────────────────
echo "▶  Checking Docker…"
docker --version
docker compose version
echo ""

# ── Step 2: Start Kafka cluster ──────────────────────────────
echo "▶  Starting Kafka cluster…"
cd "$PROJECT_DIR"
docker compose up -d
echo ""
echo "⏳  Waiting 30 s for brokers…"
sleep 30
docker compose ps

# ── Step 3: Ensure sensor-events topic exists ────────────────
echo ""
echo "▶  Verifying sensor-events topic…"
docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --create --if-not-exists \
  --topic sensor-events \
  --partitions 3 --replication-factor 3

docker exec kafka1 kafka-topics \
  --bootstrap-server kafka1:29092 \
  --describe --topic sensor-events

# ── Step 4: Create Python virtual environment ────────────────
echo ""
echo "▶  Creating Python virtual environment…"
cd "$PROJECT_DIR"
python3 -m venv venv
source venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet pyspark==3.4.1 kafka-python
echo "✅  Dependencies installed."

# ── Step 5: Create output directories ───────────────────────
echo ""
echo "▶  Creating output directories…"
mkdir -p /tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}
echo "✅  Directories ready at /tmp/spark-etl/"

echo ""
echo "=================================================="
echo "  ✅  Setup complete!"
echo ""
echo "  Run the pipeline:"
echo "    source venv/bin/activate"
echo "    python python/etl_pipeline.py"
echo ""
echo "  Send messages (in a second terminal):"
echo "    source venv/bin/activate"
echo "    python python/producer.py"
echo ""
echo "  Read output (after a few batches):"
echo "    python python/read_output.py"
echo "=================================================="
