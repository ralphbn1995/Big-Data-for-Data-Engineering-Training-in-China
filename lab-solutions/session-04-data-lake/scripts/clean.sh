#!/usr/bin/env bash
# ============================================================
#  scripts/clean.sh – Wipe data lake and checkpoints
# ============================================================
set -euo pipefail

echo "=================================================="
echo "  Session 4 – Clean Data Lake"
echo "=================================================="
echo ""
echo "This will DELETE:"
echo "  /tmp/datalake/          (all zones: raw, curated, consumption)"
echo "  /tmp/datalake-ckpt/     (Spark streaming checkpoints)"
echo ""
read -rp "Are you sure? Type 'yes' to confirm: " CONFIRM

if [[ "$CONFIRM" == "yes" ]]; then
    rm -rf /tmp/datalake/ /tmp/datalake-ckpt/
    mkdir -p /tmp/datalake/{raw,curated,consumption}
    mkdir -p /tmp/datalake/raw/source=kafka/topic=sensor-events
    mkdir -p /tmp/datalake/curated/domain=iot
    mkdir -p /tmp/datalake/consumption/use_case=sensor_averages
    mkdir -p /tmp/datalake-ckpt/{raw,curated}
    echo ""
    echo "✅  Cleaned. Fresh structure recreated."
else
    echo "Aborted."
fi
