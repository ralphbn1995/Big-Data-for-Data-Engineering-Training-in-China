#!/usr/bin/env bash
# ============================================================
#  scripts/induce_lag.sh
#  Step 6 lab exercise: produce messages with no consumer running
#  then restart consumer and watch lag drain to 0
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON="${PROJECT_DIR}/venv/bin/python"
if [[ ! -f "$PYTHON" ]]; then PYTHON="python3"; fi

MSG_COUNT=100

echo "=================================================="
echo "  Session 2 – Induce Consumer Lag (Step 6)"
echo "=================================================="
echo ""
echo "Steps:"
echo "  1. Ensure NO consumer is running (stop all python consumer.py)"
echo "  2. Send $MSG_COUNT messages → creates lag"
echo "  3. Check Kafka UI for lag"
echo "  4. Restart consumer → watch lag drain to 0"
echo ""
read -rp "Press Enter to produce $MSG_COUNT messages…"

echo ""
echo "▶  Sending $MSG_COUNT messages…"
"$PYTHON" "$PROJECT_DIR/python/producer.py" --count "$MSG_COUNT"

echo ""
echo "▶  Checking consumer group lag via CLI:"
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:29092 \
  --describe \
  --group sensor-analytics 2>/dev/null || \
  echo "  (no committed offsets yet – group has never consumed)"

echo ""
echo "=================================================="
echo "  Lag induced! Now:"
echo "  1. Open http://localhost:8080 → Consumer Groups → sensor-analytics"
echo "     You should see LAG = ~$MSG_COUNT per partition"
echo ""
echo "  2. Start the consumer:"
echo "     python python/consumer.py"
echo ""
echo "  3. Refresh Kafka UI and watch the LAG drop to 0 in real time."
echo "=================================================="
