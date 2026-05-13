#!/usr/bin/env bash
# ============================================================
#  scripts/reset_offsets.sh
#  Reset consumer group offsets (replay all messages from beginning)
#  or skip to latest (discard backlog).
#
#  NOTE: The consumer group must have NO active consumers when
#        resetting offsets, otherwise Kafka will reject the request.
# ============================================================
set -euo pipefail

BROKER="kafka1:29092"
TOPIC="sensor-events"
GROUP="sensor-analytics"

echo "=================================================="
echo "  Session 2 – Reset Consumer Group Offsets"
echo "  Group : $GROUP"
echo "  Topic : $TOPIC"
echo "=================================================="
echo ""
echo "Choose reset strategy:"
echo "  1) --to-earliest   replay ALL messages from offset 0"
echo "  2) --to-latest     skip backlog, consume only new messages"
echo "  3) Cancel"
echo ""
read -rp "Enter 1, 2, or 3: " CHOICE

case "$CHOICE" in
  1)
    echo ""
    echo "▶  Resetting to earliest (replay all)…"
    docker exec kafka1 kafka-consumer-groups \
      --bootstrap-server "$BROKER" \
      --group "$GROUP" \
      --topic "$TOPIC" \
      --reset-offsets \
      --to-earliest \
      --execute
    echo ""
    echo "✅  Done. Next consumer run will replay from offset 0."
    ;;
  2)
    echo ""
    echo "▶  Resetting to latest (skip backlog)…"
    docker exec kafka1 kafka-consumer-groups \
      --bootstrap-server "$BROKER" \
      --group "$GROUP" \
      --topic "$TOPIC" \
      --reset-offsets \
      --to-latest \
      --execute
    echo ""
    echo "✅  Done. Next consumer run will only see new messages."
    ;;
  3)
    echo "Cancelled."
    ;;
  *)
    echo "Invalid choice."
    exit 1
    ;;
esac

echo ""
echo "▶  Current offsets after reset:"
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server "$BROKER" \
  --describe \
  --group "$GROUP" 2>/dev/null
