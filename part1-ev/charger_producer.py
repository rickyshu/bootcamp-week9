"""Q2: 원본 CSV를 시간순으로 Kafka에 한 번 재생합니다.

EC2의 Airflow 컨테이너에서 실행합니다. 기존 메시지가 있으면 전송하지 않습니다.
Kafka의 멱등 전송은 같은 Producer 세션의 내부 재시도 중복을 방지합니다.
프로그램을 다시 실행해 send()를 반복하는 것까지 중복 제거하지는 않습니다.
"""
import argparse
from collections import Counter
import csv
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import time

from kafka import KafkaConsumer, KafkaProducer, TopicPartition


TOPIC = "ev-charging-sessions"
FIELDS = (
    "stationName", "portNumber", "portType", "plugType", "startTime", "endTime",
    "totalDuration", "chargingDuration", "energyKwh", "endedBy",
)
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def load_events(path):
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if tuple(reader.fieldnames or ()) != FIELDS:
            raise ValueError(f"CSV 컬럼이 요구 스키마와 다릅니다: {reader.fieldnames}")
        events = []
        for number, row in enumerate(reader, start=2):
            if None in row or any(value is None or value == "" for value in row.values()):
                raise ValueError(f"CSV {number}행의 필드가 누락됐습니다.")
            datetime.strptime(row["startTime"], TIME_FORMAT)
            datetime.strptime(row["endTime"], TIME_FORMAT)
            event = dict(row)
            event["portNumber"] = int(row["portNumber"])
            event["energyKwh"] = float(row["energyKwh"])
            if not math.isfinite(event["energyKwh"]):
                raise ValueError(f"CSV {number}행의 energyKwh가 유한한 수가 아닙니다.")
            events.append(event)
    if len(events) != 175:
        raise ValueError(f"제공 CSV는 175행이어야 합니다. 현재 {len(events)}행")
    return sorted(events, key=lambda event: datetime.strptime(event["startTime"], TIME_FORMAT))


def check_empty_topic(bootstrap_servers):
    """그룹에 가입하거나 오프셋을 커밋하지 않고 기존 로그의 범위만 확인합니다."""
    probe = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        client_id="de9-lhi-q2-preflight",
        group_id=None,
        enable_auto_commit=False,
        allow_auto_create_topics=False,
    )
    try:
        partitions = probe.partitions_for_topic(TOPIC)
        if partitions != {0, 1, 2}:
            raise RuntimeError(f"Q1 토픽의 파티션 3개가 필요합니다: {partitions}")
        targets = [TopicPartition(TOPIC, partition) for partition in sorted(partitions)]
        starts = probe.beginning_offsets(targets)
        ends = probe.end_offsets(targets)
        offsets = {target.partition: (starts[target], ends[target]) for target in targets}
        print(f"Before send, partition (begin,end): {offsets}", flush=True)
        if any(start != 0 or end != 0 for start, end in offsets.values()):
            raise RuntimeError(
                "전송을 거부합니다: 토픽이 새 빈 토픽이 아닙니다. "
                "다시 send하면 중복됩니다. 원인과 후속 데이터·체크포인트를 확인한 뒤 "
                "필요한 경우에만 문제지의 토픽 재생성 절차를 따르세요."
            )
    finally:
        probe.close()


def produce(events, bootstrap_servers, delay):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="de9-lhi-q2-producer",
        acks="all",
        enable_idempotence=True,
        max_in_flight_requests_per_connection=1,
        delivery_timeout_ms=120_000,
        request_timeout_ms=30_000,
        retry_backoff_ms=300,
        allow_auto_create_topics=False,
        key_serializer=lambda station: station.encode("utf-8"),
        value_serializer=lambda event: json.dumps(
            event, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8"),
    )
    counts = Counter()
    station_partitions = {}
    try:
        print(
            "Producer: acks=all, enable_idempotence=True, "
            "max_in_flight_requests_per_connection=1; key=stationName",
            flush=True,
        )
        for index, event in enumerate(events, start=1):
            # get() 성공은 브로커의 ACK를 받은 뒤이며, send() 호출 횟수와 구분합니다.
            metadata = producer.send(TOPIC, key=event["stationName"], value=event).get(timeout=150)
            previous = station_partitions.setdefault(event["stationName"], metadata.partition)
            if previous != metadata.partition:
                raise RuntimeError("동일 stationName의 파티션이 변경됐습니다.")
            counts[metadata.partition] += 1
            if index % 25 == 0 or index == len(events):
                print(
                    f"ACK {index}/{len(events)} startTime={event['startTime']} "
                    f"partition={metadata.partition} offset={metadata.offset}",
                    flush=True,
                )
            if index < len(events):
                time.sleep(delay)
    finally:
        # 전송 중 예외가 나더라도 flush를 시도하고 반드시 close합니다.
        try:
            producer.flush(timeout=150)
        finally:
            producer.close(timeout=30)
    print(f"Partition ACK counts: {dict(sorted(counts.items()))}", flush=True)
    print(f"Station keys checked: {len(station_partitions)}; one partition per station OK", flush=True)
    print(f"SUCCESS: {sum(counts.values())} messages acknowledged; producer flushed and closed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=Path(__file__).resolve().parents[1] / "data/charger_sessions.csv")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
    )
    parser.add_argument("--delay", type=float, default=0.1, help="각 행 전송 사이 대기 초: 0.05~0.2")
    parser.add_argument("--validate-only", action="store_true", help="CSV만 검증하고 Kafka에 연결하지 않음")
    args = parser.parse_args()
    if not 0.05 <= args.delay <= 0.2:
        parser.error("--delay는 0.05~0.2초여야 합니다.")
    events = load_events(args.csv)
    print(f"CSV rows: {len(events)}; SHA256: {hashlib.sha256(args.csv.read_bytes()).hexdigest()}", flush=True)
    print(f"Sorted startTime: {events[0]['startTime']} -> {events[-1]['startTime']}; delay={args.delay}s", flush=True)
    if args.validate_only:
        print("Validation only: Kafka connection/send not performed", flush=True)
        return
    check_empty_topic(args.bootstrap_servers.split(","))
    produce(events, args.bootstrap_servers.split(","), args.delay)


if __name__ == "__main__":
    main()
