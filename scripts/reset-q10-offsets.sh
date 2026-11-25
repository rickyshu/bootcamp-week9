#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime/q10
compose=(docker compose -f part1-ev/docker-compose.yaml)
# Fail before mutation if Q3 is not drained, consumers are active, or data expired.
"${compose[@]}" exec -T airflow python /opt/de9/scripts/verify-q10.py --phase before \
  | tee .runtime/q10/before.log
reset=("${compose[@]}" exec -T -e KAFKA_HEAP_OPTS=-Xmx128m kafka-1
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092
  --group ev-consumer-lhi --topic ev-charging-sessions
  --reset-offsets --to-earliest --execute)
printf 'RUN:'
printf ' %q' "${reset[@]}"
printf '\n'
"${reset[@]}" | tee .runtime/q10/reset.log
"${compose[@]}" exec -T airflow python /opt/de9/scripts/verify-q10.py --phase after \
  | tee .runtime/q10/after.log
