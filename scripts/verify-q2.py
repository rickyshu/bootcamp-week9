"""Q2 전송 결과를 원본 CSV와 대조합니다. 그룹 가입·오프셋 커밋·전송 없음."""
from collections import Counter, defaultdict
import csv
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import time

from kafka import KafkaConsumer, TopicPartition


TOPIC = "ev-charging-sessions"


def canonical(event):
    # 숫자 JSON과 CSV의 문자열 표현을 비교하되 값은 Decimal로 비교합니다.
    return tuple(
        (key, Decimal(str(value)) if key in {"portNumber", "energyKwh"} else value)
        for key, value in sorted(event.items())
    )


def main():
    source = Path(__file__).resolve().parents[1] / "data/charger_sessions.csv"
    with source.open(encoding="utf-8-sig", newline="") as file:
        expected = Counter(canonical(row) for row in csv.DictReader(file))
    consumer = KafkaConsumer(
        bootstrap_servers=["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"],
        client_id="de9-lhi-q2-verifier",
        group_id=None,
        enable_auto_commit=False,
        allow_auto_create_topics=False,
    )
    actual = Counter()
    stations = defaultdict(set)
    counts = Counter()
    previous_times = {}
    try:
        partitions = sorted(consumer.partitions_for_topic(TOPIC) or [])
        assert partitions == [0, 1, 2], partitions
        targets = [TopicPartition(TOPIC, number) for number in partitions]
        begins = consumer.beginning_offsets(targets)
        ends = consumer.end_offsets(targets)
        assert all(value == 0 for value in begins.values()), begins
        assert sum(ends.values()) == 175, ends
        consumer.assign(targets)
        consumer.seek_to_beginning(*targets)
        deadline = time.monotonic() + 60
        while not all(consumer.position(target) >= ends[target] for target in targets):
            if time.monotonic() > deadline:
                raise TimeoutError("175건 검증을 제한 시간 안에 완료하지 못했습니다.")
            for records in consumer.poll(timeout_ms=1000).values():
                for record in records:
                    target = TopicPartition(record.topic, record.partition)
                    if record.offset >= ends[target]:
                        continue
                    event = json.loads(record.value.decode("utf-8"))
                    assert record.key == event["stationName"].encode("utf-8"), "stationName key mismatch"
                    timestamp = datetime.strptime(event["startTime"], "%Y-%m-%d %H:%M:%S")
                    assert timestamp >= previous_times.get(record.partition, datetime.min), "time order mismatch"
                    previous_times[record.partition] = timestamp
                    actual[canonical(event)] += 1
                    stations[event["stationName"]].add(record.partition)
                    counts[record.partition] += 1
        assert actual == expected, f"CSV mismatch: missing={expected-actual}, extra={actual-expected}"
        assert all(len(values) == 1 for values in stations.values()), stations
        print("PASS: 175 Kafka events exactly match all original CSV rows (including multiplicities)")
        print("PASS: all keys match stationName; one partition per station; nondecreasing time per partition")
        print(f"Partition counts: {dict(sorted(counts.items()))}")
        print(f"Stations: {len(stations)}; total: {sum(counts.values())}; no group/offset commit used")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
