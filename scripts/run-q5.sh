#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime/q5
test -s .private/slack-webhook
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  bash -c 'umask 077; cat > /tmp/de9-slack-webhook' < .private/slack-webhook
trap 'docker compose -f part1-ev/docker-compose.yaml exec -T airflow rm -f /tmp/de9-slack-webhook >/dev/null 2>&1 || true' EXIT
echo 'Q5 Spark diagnostics: .runtime/q5/spark-stderr.log'
set +e
docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
  spark-submit --conf spark.driver.host=airflow /opt/de9/part1-ev/charger_alerts.py \
  2>.runtime/q5/spark-stderr.log | tee .runtime/q5/stream.log
result=${PIPESTATUS[0]}
set -e
if (( result != 0 )); then
  tail -n 80 .runtime/q5/spark-stderr.log >&2
fi
exit "$result"
