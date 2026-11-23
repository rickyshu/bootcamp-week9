#!/usr/bin/env bash
# EC2 호스트에서 저장소를 clone/pull한 뒤 실행합니다.
set -euo pipefail
cd "$(dirname "$0")/../part1-ev"
if [[ ! -f .env ]]; then
  cp .env.example .env
fi
docker compose config --quiet
docker compose build airflow
docker compose up -d --wait --wait-timeout 900
../scripts/create-ev-topic.sh
