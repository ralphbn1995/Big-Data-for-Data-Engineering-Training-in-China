#!/usr/bin/env bash
# ============================================================
#  scripts/teardown.sh
#  Stop and optionally delete the Kafka cluster
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "=================================================="
echo "  Session 1 – Teardown"
echo "=================================================="
echo ""
echo "Choose an option:"
echo "  1) Stop containers only  (data volumes preserved → fast restart)"
echo "  2) Stop AND delete volumes (clean slate)"
echo ""
read -rp "Enter 1 or 2: " CHOICE

case "$CHOICE" in
  1)
    echo ""
    echo "▶  Stopping containers (volumes kept)…"
    docker compose down
    echo ""
    echo "✅  Containers stopped. Run ./scripts/start_cluster.sh to restart."
    ;;
  2)
    echo ""
    echo "⚠️   This will DELETE all Kafka data (topics, messages, offsets)."
    read -rp "Are you sure? Type 'yes' to confirm: " CONFIRM
    if [[ "$CONFIRM" == "yes" ]]; then
      docker compose down -v
      echo ""
      echo "✅  Containers stopped and all volumes deleted."
    else
      echo "Aborted."
      exit 0
    fi
    ;;
  *)
    echo "Invalid option. Exiting."
    exit 1
    ;;
esac
