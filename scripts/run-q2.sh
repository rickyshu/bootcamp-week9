#!/usr/bin/env bash
# EC2 호스트에서 한 번만 실행합니다. 기존 메시지가 있으면 Producer가 전송을 거부합니다.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  python /opt/de9/part1-ev/charger_producer.py \
  | tee .runtime/q2-producer.log
