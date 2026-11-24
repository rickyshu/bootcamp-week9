"""실제 Kafka 결과를 CSV 및 권역 규칙과 대조합니다. 전송·그룹 가입·커밋 없음."""
from collections import Counter, defaultdict
import csv
from decimal import Decimal
import json
from pathlib import Path
import time

from kafka import KafkaConsumer, TopicPartition


def canonical(event):
    return tuple((key, Decimal(str(value)) if key in {"year", "month", "price"} else value)
                 for key, value in sorted(event.items()))


def main():
    source = Path(__file__).resolve().parents[1] / "data/india_grocery_prices.csv"
    with source.open(encoding="utf-8-sig", newline="") as file:
        expected = Counter(canonical(row) for row in csv.DictReader(file))
    # Producer의 매핑을 재사용하지 않고 요구 규칙으로 독립 대조합니다.
    regions = [{"Delhi", "Jaipur", "Lucknow", "Kanpur"},
               {"Mumbai", "Ahmedabad", "Bhopal"}, {"Hyderabad", "Chennai", "Patna"}]
    consumer = KafkaConsumer(bootstrap_servers=["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"],
                             group_id=None, enable_auto_commit=False, allow_auto_create_topics=False,
                             client_id="de9-lhi-q6-verifier")
    actual, counts, markets = Counter(), Counter(), defaultdict(set)
    try:
        assert consumer.partitions_for_topic("product-prices") == {0, 1, 2}
        targets = [TopicPartition("product-prices", p) for p in range(3)]
        starts, ends = consumer.beginning_offsets(targets), consumer.end_offsets(targets)
        assert all(value == 0 for value in starts.values()), starts
        assert {p.partition: ends[p] for p in targets} == {0: 80, 1: 60, 2: 60}, ends
        consumer.assign(targets)
        consumer.seek_to_beginning(*targets)
        deadline = time.monotonic() + 60
        while not all(consumer.position(p) >= ends[p] for p in targets):
            if time.monotonic() > deadline:
                raise TimeoutError("Kafka verification timed out")
            for records in consumer.poll(timeout_ms=1000).values():
                for record in records:
                    assert record.offset < ends[TopicPartition(record.topic, record.partition)], "Concurrent writer detected"
                    event = json.loads(record.value)
                    assert record.key == event["market"].encode("utf-8"), "market key mismatch"
                    assert event["market"] in regions[record.partition], "region mismatch"
                    assert type(event["year"]) is int and type(event["month"]) is int
                    assert type(event["price"]) is float
                    actual[canonical(event)] += 1
                    counts[record.partition] += 1
                    markets[record.partition].add(event["market"])
        assert actual == expected, f"CSV mismatch: missing={expected-actual}, extra={actual-expected}"
        print("PASS: all 200 Kafka events match original CSV values and multiplicities")
        print("PASS: JSON schema/types, market keys and regional partitions match")
        for partition in range(3):
            print(f"Partition {partition}: count={counts[partition]}; markets={','.join(sorted(markets[partition]))}")
        print("PASS: no consumer group or offset commit used")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
