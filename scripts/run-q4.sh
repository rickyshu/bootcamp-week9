#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime/q4
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf spark.driver.host=airflow \
  /opt/de9/part1-ev/charger_stream.py --drain "$@" \
  2>&1 | tee .runtime/q4/stream.log
