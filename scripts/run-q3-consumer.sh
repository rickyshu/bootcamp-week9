#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
name=${1:?Usage: bash scripts/run-q3-consumer.sh a_or_b}
case "$name" in a|b) ;; *) exit 2 ;; esac
mkdir -p .runtime/q3
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  python /opt/de9/part1-ev/charger_consumer.py --name "$name" \
  | tee ".runtime/q3/consumer-${name}.log"
