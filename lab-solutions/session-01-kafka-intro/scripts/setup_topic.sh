#!/usr/bin/env bash
# ============================================================
#  scripts/setup_topic.sh
#  Create the session1-events topic and inspect it
# ============================================================
set -euo pipefail

BROKER="kafka1:29092"
TOPIC="session1-events"
PARTITIONS=3
REPLICATION=3

echo "=================================================="
echo "  Session 1 – Topic Setup"
echo "  Broker     : $BROKER (inside Docker network)"
echo "  Topic      : $TOPIC"
echo "  Partitions : $PARTITIONS"
echo "  Replication: $REPLICATION"
echo "=================================================="
echo ""

echo "▶  Creating topic '$TOPIC'…"
docker exec kafka1 kafka-topics \
  --bootstrap-server "$BROKER" \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION"

echo ""
echo "▶  Listing all topics:"
docker exec kafka1 kafka-topics \
  --bootstrap-server "$BROKER" \
  --list

echo ""
echo "▶  Describing '$TOPIC' (leaders + ISR per partition):"
docker exec kafka1 kafka-topics \
  --bootstrap-server "$BROKER" \
  --describe \
  --topic "$TOPIC"

echo ""
echo "=================================================="
echo "  ✅  Topic created and ready."
echo "=================================================="
