#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime/q10
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  python /opt/de9/part2-price/kafka_lag_monitor.py "${1:-ev-consumer-lhi}" \
  | tee .runtime/q10/monitor.log
