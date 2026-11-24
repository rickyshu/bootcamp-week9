#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime/q7
echo "Q7 Spark diagnostics: .runtime/q7/spark-stderr.log"
set +e
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf spark.driver.host=airflow /opt/de9/part2-price/price_batch.py \
  2>.runtime/q7/spark-stderr.log | tee .runtime/q7/batch.log
result=${PIPESTATUS[0]}
set -e
if (( result != 0 )); then
  tail -n 80 .runtime/q7/spark-stderr.log >&2
fi
exit "$result"
