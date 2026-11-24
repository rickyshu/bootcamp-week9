"""Q6: 제공 CSV 200행을 도시 권역별로 Kafka에 한 번 전송합니다."""
import argparse
from collections import Counter
import csv
import hashlib
import json
import math
import os
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient
from kafka.partitioner.default import DefaultPartitioner


TOPIC = "product-prices"
FIELDS = ("item_name", "market", "year", "month", "price", "currency", "unit")
SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092").split(",")


class RegionPartitioner:
    """Kafka가 key_serializer 적용 후 호출하는 파티셔너 객체입니다."""
    REGIONS = {
        "Delhi": 0, "Jaipur": 0, "Lucknow": 0, "Kanpur": 0,
        "Mumbai": 1, "Ahmedabad": 1, "Bhopal": 1,
        "Hyderabad": 2, "Chennai": 2, "Patna": 2,
    }

    def __call__(self, key_bytes, all_partitions, available_partitions):
        market = key_bytes.decode("utf-8") if key_bytes is not None else None
        if market in self.REGIONS:
            target = self.REGIONS[market]
            if target not in all_partitions:
                raise ValueError(f"Required partition {target} is missing")
            # 일시적으로 leader가 없더라도 다른 권역으로 우회하지 않습니다.
            return target
        return DefaultPartitioner()(key_bytes, all_partitions, available_partitions)


def load_events(path):
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if tuple(reader.fieldnames or ()) != FIELDS:
            raise ValueError(f"CSV schema mismatch: {reader.fieldnames}")
        events = []
        for row in reader:
            if None in row or any(value is None or value == "" for value in row.values()):
                raise ValueError("CSV contains missing fields")
            event = dict(row)
            event.update(year=int(row["year"]), month=int(row["month"]), price=float(row["price"]))
            if not math.isfinite(event["price"]) or not 1 <= event["month"] <= 12:
                raise ValueError("Invalid numeric value in CSV")
            events.append(event)
    if len(events) != 200:
        raise ValueError(f"Expected original 200 rows; got {len(events)}")
    return events


def check_empty_topic():
    admin = KafkaAdminClient(bootstrap_servers=SERVERS, client_id="de9-lhi-q6-topic-check")
    try:
        description = admin.describe_topics([TOPIC])[0]
        partitions = description["partitions"]
        if description.get("error_code") or len(partitions) != 3 or any(len(p["replicas"]) != 2 for p in partitions):
            raise RuntimeError("product-prices must have 3 partitions and replication factor 2")
    finally:
        admin.close()
    probe = KafkaConsumer(bootstrap_servers=SERVERS, group_id=None, enable_auto_commit=False,
                          allow_auto_create_topics=False, client_id="de9-lhi-q6-preflight")
    try:
        if probe.partitions_for_topic(TOPIC) != {0, 1, 2}:
            raise RuntimeError("Create product-prices with 3 partitions first")
        targets = [TopicPartition(TOPIC, p) for p in range(3)]
        starts, ends = probe.beginning_offsets(targets), probe.end_offsets(targets)
        print(f"Before send (begin,end): { {p.partition: (starts[p], ends[p]) for p in targets} }", flush=True)
        if any(starts[p] != 0 or ends[p] != 0 for p in targets):
            raise RuntimeError("Refusing duplicate replay: product-prices must be a new empty topic")
    finally:
        probe.close()


def produce(events):
    producer = KafkaProducer(
        bootstrap_servers=SERVERS, client_id="de9-lhi-q6-producer",
        partitioner=RegionPartitioner(), acks="all", enable_idempotence=True,
        max_in_flight_requests_per_connection=1, allow_auto_create_topics=False,
        key_serializer=lambda market: market.encode("utf-8"),
        value_serializer=lambda event: json.dumps(event, ensure_ascii=False, allow_nan=False).encode("utf-8"),
    )
    counts = Counter()
    markets = {}
    try:
        print("Producer: RegionPartitioner; key=market; acks=all; idempotence=True", flush=True)
        for index, event in enumerate(events, 1):
            metadata = producer.send(TOPIC, key=event["market"], value=event).get(timeout=150)
            expected = RegionPartitioner.REGIONS.get(event["market"])
            if expected is not None and metadata.partition != expected:
                raise RuntimeError(f"Unexpected partition for {event['market']}: {metadata.partition}")
            counts[metadata.partition] += 1
            markets[event["market"]] = metadata.partition
            if index % 50 == 0:
                print(f"ACK {index}/{len(events)} partition={metadata.partition} offset={metadata.offset}", flush=True)
    finally:
        try:
            producer.flush(timeout=150)
        finally:
            producer.close(timeout=30)
    for partition in range(3):
        cities = sorted(city for city, value in markets.items() if value == partition)
        print(f"Partition {partition}: ACK={counts[partition]}; markets={','.join(cities)}", flush=True)
    print(f"SUCCESS: {sum(counts.values())} messages acknowledged; producer flushed and closed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=Path(__file__).resolve().parents[1] / "data/india_grocery_prices.csv")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    events = load_events(args.csv)
    print(f"CSV rows={len(events)}; SHA256={hashlib.sha256(args.csv.read_bytes()).hexdigest()}", flush=True)
    if args.validate_only:
        print("Validation only: no Kafka connection or send performed")
        return
    check_empty_topic()
    produce(events)


if __name__ == "__main__":
    main()
