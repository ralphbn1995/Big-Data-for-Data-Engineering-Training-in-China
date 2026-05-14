#!/usr/bin/env bash
# ============================================================
#  scripts/clean.sh
#  Reset Spark output and checkpoint directories between runs.
#
#  WARNING: This deletes ALL previously written Parquet files
#  and streaming state. Use before restarting from scratch.
# ============================================================
set -euo pipefail

echo "=================================================="
echo "  Session 3 – Clean Spark State"
echo "=================================================="
echo ""
echo "This will DELETE:"
echo "  /tmp/spark-etl/output/         (aggregated Parquet)"
echo "  /tmp/spark-etl/checkpoint/     (streaming state)"
echo "  /tmp/spark-etl/raw/            (raw Parquet)"
echo "  /tmp/spark-etl/checkpoint-raw/ (raw streaming state)"
echo "  /tmp/spark-etl/batch-output/   (batch Parquet)"
echo ""
read -rp "Are you sure? Type 'yes' to confirm: " CONFIRM

if [[ "$CONFIRM" == "yes" ]]; then
    rm -rf /tmp/spark-etl/
    mkdir -p /tmp/spark-etl/{output,checkpoint,raw,checkpoint-raw,batch-output}
    echo ""
    echo "✅  Cleaned. Fresh directories created at /tmp/spark-etl/"
else
    echo "Aborted."
fi
