#!/usr/bin/env bash
# ============================================================
#  scripts/run_pipeline.sh
#  Launch the Spark Structured Streaming ETL pipeline
#  via spark-submit (recommended for production-style runs)
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

PYTHON="${PROJECT_DIR}/venv/bin/python"
SPARK_SUBMIT="${PROJECT_DIR}/venv/bin/spark-submit"

if [[ ! -f "$SPARK_SUBMIT" ]]; then
    SPARK_SUBMIT="spark-submit"
fi

KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

echo "=================================================="
echo "  Session 3 – Spark ETL Pipeline"
echo "  Method: spark-submit"
echo "=================================================="
echo ""
echo "Tip: In a SECOND terminal, run the producer:"
echo "  source venv/bin/activate && python python/producer.py"
echo ""
echo "Press Ctrl+C to stop the pipeline gracefully."
echo ""

cd "$PROJECT_DIR"

"$SPARK_SUBMIT" \
  --master "local[*]" \
  --packages "$KAFKA_PKG" \
  --conf "spark.sql.shuffle.partitions=3" \
  --conf "spark.streaming.stopGracefullyOnShutdown=true" \
  python/etl_pipeline.py "$@"
