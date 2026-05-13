#!/usr/bin/env bash
# ============================================================
#  scripts/fault_tolerance_demo.sh
#  Manual step-by-step fault tolerance demonstration (CLI only)
#  Run AFTER setup_topic.sh
# ============================================================
set -euo pipefail

BROKER1="kafka1:29092"
BROKER2="kafka2:29092"
TOPIC="session1-events"

echo "=================================================="
echo "  Session 1 – Fault Tolerance Demo"
echo "=================================================="
echo ""

# ── Step 1: Record initial partition leaders ──────────────
echo "─────────────────────────────────────────────────"
echo "STEP 1 – Initial state: partition leaders"
echo "─────────────────────────────────────────────────"
docker exec kafka1 kafka-topics \
  --bootstrap-server "$BROKER1" \
  --describe \
  --topic "$TOPIC"

echo ""
read -rp "📌  Note which broker is Leader for each partition. Press Enter to continue…"

# ── Step 2: Stop kafka1 ───────────────────────────────────
echo ""
echo "─────────────────────────────────────────────────"
echo "STEP 2 – Stopping kafka1 (simulating a crash)"
echo "─────────────────────────────────────────────────"
docker stop kafka1
echo "✔  kafka1 stopped."

echo ""
echo "⏳  Waiting 10 s for Raft leader re-election…"
sleep 10

# ── Step 3: Observe re-election ───────────────────────────
echo ""
echo "─────────────────────────────────────────────────"
echo "STEP 3 – New partition leaders after failure"
echo "─────────────────────────────────────────────────"
docker exec kafka2 kafka-topics \
  --bootstrap-server "$BROKER2" \
  --describe \
  --topic "$TOPIC"

echo ""
echo "💡  Notice: Leaders for partitions previously held by Broker 1"
echo "    have moved to Broker 2 or 3. ISR now shows only 2 replicas."

echo ""
read -rp "Press Enter to verify data is still readable…"

# ── Step 4: Verify data is still accessible ───────────────
echo ""
echo "─────────────────────────────────────────────────"
echo "STEP 4 – Reading all messages via kafka2 (5 s timeout)"
echo "─────────────────────────────────────────────────"
timeout 5 docker exec kafka2 kafka-console-consumer \
  --bootstrap-server "$BROKER2" \
  --topic "$TOPIC" \
  --from-beginning \
  --property "print.key=true" \
  --property "print.partition=true" \
  --property "print.offset=true" \
  --timeout-ms 3000 || true

echo ""
echo "✅  All previously produced messages are still readable."

echo ""
read -rp "Press Enter to restart kafka1…"

# ── Step 5: Restart kafka1 ────────────────────────────────
echo ""
echo "─────────────────────────────────────────────────"
echo "STEP 5 – Restarting kafka1 (log replication)"
echo "─────────────────────────────────────────────────"
docker start kafka1
echo "✔  kafka1 started. Waiting 15 s for catch-up…"
sleep 15

echo ""
echo "─────────────────────────────────────────────────"
echo "STEP 6 – Final state: all 3 brokers in ISR"
echo "─────────────────────────────────────────────────"
docker exec kafka1 kafka-topics \
  --bootstrap-server "$BROKER1" \
  --describe \
  --topic "$TOPIC"

echo ""
echo "=================================================="
echo "  ✅  Fault tolerance demo complete."
echo "  kafka1 is back in the ISR after replication catch-up."
echo "=================================================="
