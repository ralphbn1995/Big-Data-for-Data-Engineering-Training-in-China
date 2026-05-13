#!/usr/bin/env bash
# ============================================================
#  scripts/start_cluster.sh
#  Start the 3-broker Kafka cluster and Kafka UI
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=================================================="
echo "  Session 1 – Starting Kafka Cluster"
echo "=================================================="

cd "$PROJECT_DIR"

echo ""
echo "▶  Pulling images and starting containers (detached)…"
docker compose up -d

echo ""
echo "⏳  Waiting 30 s for brokers to elect a controller…"
sleep 30

echo ""
echo "▶  Container status:"
docker compose ps

echo ""
echo "▶  Checking kafka1 startup log…"
docker logs kafka1 2>&1 | grep -i "started" | tail -5 || true

echo ""
echo "=================================================="
echo "  ✅  Cluster should be ready."
echo "  Open Kafka UI → http://localhost:8080"
echo "=================================================="
