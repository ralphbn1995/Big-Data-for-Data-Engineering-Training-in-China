#!/usr/bin/env bash
# ============================================================
#  scripts/rebalance_demo.sh
#  Consumer Group Scaling – Step 4 of the lab
#
#  Demonstrates partition rebalancing by launching multiple
#  consumer instances in background and monitoring assignments.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON="${PROJECT_DIR}/venv/bin/python"
CONSUMER="${PROJECT_DIR}/python/consumer.py"

# Fall back to system python if venv not found
if [[ ! -f "$PYTHON" ]]; then
    PYTHON="python3"
fi

echo "=================================================="
echo "  Session 2 – Consumer Group Rebalance Demo"
echo "  Group: sensor-analytics  |  Topic: sensor-events"
echo "=================================================="
echo ""
echo "This script launches 3 consumer instances."
echo "Watch how Kafka distributes the 3 partitions."
echo ""
echo "⚠  Make sure sensor-events has messages (run producer.py first)."
echo ""
read -rp "Press Enter to start…"

# Launch consumers in separate terminal windows if possible
echo ""
echo "▶  Opening 3 consumer terminals…"
echo "   (if your OS doesn't support this, open 3 terminals manually"
echo "    and run: python python/consumer.py)"
echo ""

# Try opening new terminal tabs / windows
if command -v gnome-terminal &>/dev/null; then
    gnome-terminal --tab --title="Consumer-1" -- bash -c "$PYTHON $CONSUMER; exec bash" &
    sleep 1
    gnome-terminal --tab --title="Consumer-2" -- bash -c "$PYTHON $CONSUMER; exec bash" &
    sleep 1
    gnome-terminal --tab --title="Consumer-3" -- bash -c "$PYTHON $CONSUMER; exec bash" &
elif command -v osascript &>/dev/null; then
    # macOS
    osascript -e "tell app \"Terminal\" to do script \"cd $PROJECT_DIR && $PYTHON $CONSUMER\""
    sleep 1
    osascript -e "tell app \"Terminal\" to do script \"cd $PROJECT_DIR && $PYTHON $CONSUMER\""
    sleep 1
    osascript -e "tell app \"Terminal\" to do script \"cd $PROJECT_DIR && $PYTHON $CONSUMER\""
else
    echo "Could not auto-open terminals."
    echo "Please run in 3 separate terminals:"
    echo ""
    echo "  python python/consumer.py"
    echo ""
fi

echo ""
echo "Monitoring consumer group lag (refresh every 3 s)…"
echo "Press Ctrl+C to stop."
echo ""

# Show CLI lag stats until user stops
sleep 3

docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:29092 \
  --describe \
  --group sensor-analytics 2>/dev/null || \
  echo "  (group not yet active – start a consumer first)"

echo ""
echo "▶  Tip: Check http://localhost:8080 → Consumer Groups → sensor-analytics"
echo "        to see live partition assignments per consumer."
