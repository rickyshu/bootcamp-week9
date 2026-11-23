#!/usr/bin/env bash
# 이 출력 파일은 참고 기록입니다. 제출용 PNG 화면 캡처를 대체하지 않습니다.
set -euo pipefail
cd "$(dirname "$0")/.."
date
hostname
git rev-parse HEAD
aws sts get-caller-identity
docker compose -f part1-ev/docker-compose.yaml ps
scripts/create-ev-topic.sh
docker compose -f part1-ev/docker-compose.yaml exec -T airflow python -c \
  'import sys,pyspark; from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator; print(sys.version); print("Spark",pyspark.__version__); print("SparkSubmitOperator import OK")'
docker compose -f part1-ev/docker-compose.yaml exec -T spark-worker python --version
