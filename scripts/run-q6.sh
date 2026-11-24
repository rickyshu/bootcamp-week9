#!/usr/bin/env bash
# EC2 호스트에서 실행. 기존 데이터가 있으면 Producer가 중복 전송을 거부합니다.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime/q6
{
  docker compose -f part1-ev/docker-compose.yaml exec -T \
    -e 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m' kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9092 --create --if-not-exists \
    --topic product-prices --partitions 3 --replication-factor 2
  docker compose -f part1-ev/docker-compose.yaml exec -T \
    -e 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m' kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9092 --describe --topic product-prices
  docker compose -f part1-ev/docker-compose.yaml exec -T airflow \
    python /opt/de9/part2-price/price_producer.py
} | tee .runtime/q6/producer.log
