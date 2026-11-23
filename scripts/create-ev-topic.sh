#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../part1-ev"
docker compose exec -T -e 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m' kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create --if-not-exists --topic ev-charging-sessions \
  --partitions 3 --replication-factor 2
docker compose exec -T -e 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m' kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe --topic ev-charging-sessions
