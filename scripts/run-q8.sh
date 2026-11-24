#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime/q8
echo "Q8 Spark diagnostics: .runtime/q8/spark-stderr.log"
set +e
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  spark-submit --conf spark.driver.host=airflow /opt/de9/part2-price/price_change.py \
  2>.runtime/q8/spark-stderr.log | tee .runtime/q8/change.log
result=${PIPESTATUS[0]}
set -e
if (( result != 0 )); then
  tail -n 80 .runtime/q8/spark-stderr.log >&2
fi
exit "$result"
