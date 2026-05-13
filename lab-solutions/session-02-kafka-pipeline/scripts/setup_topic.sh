#!/usr/bin/env bash
# ============================================================
#  scripts/setup_topic.sh
#  Create the sensor-events topic for Session 2
# ============================================================
set -euo pipefail

BROKER="kafka1:29092"
TOPIC="sensor-events"
PARTITIONS=3
REPLICATION=3

echo "=================================================="
echo "  Session 2 – Topic Setup"
echo "  Topic      : $TOPIC"
echo "  Partitions : $PARTITIONS  (one per sensor type)"
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
echo "▶  Topic description:"
docker exec kafka1 kafka-topics \
  --bootstrap-server "$BROKER" \
  --describe \
  --topic "$TOPIC"

echo ""
echo "=================================================="
echo "  ✅  Topic ready."
echo "  Sensor types will route to partitions:"
echo "    temperature → always the same partition"
echo "    humidity    → always the same partition"
echo "    pressure    → always the same partition"
echo "  (determined by murmur2 hash of the key)"
echo "=================================================="
