"""Q7 S3 실제 파일을 원본 CSV·실제 Kafka 바이트와 독립 대조합니다."""
from collections import Counter, defaultdict
import csv
from decimal import Decimal
import importlib.util
import json
import math
import os
from pathlib import Path
import time

from kafka import KafkaConsumer, TopicPartition
from pyspark.sql import SparkSession, types as T


FIELDS = ["item_name", "market", "year", "month", "price", "currency", "unit"]


def canonical(event):
    return tuple(Decimal(str(event[name])) if name in {"year", "month", "price"} else event[name]
                 for name in FIELDS)


def main():
    root = Path(__file__).resolve().parents[1]
    with (root / "data/india_grocery_prices.csv").open(encoding="utf-8-sig", newline="") as stream:
        source = list(csv.DictReader(stream))
    expected = Counter(canonical(row) for row in source)
    prices = defaultdict(list)
    for row in source:
        prices[(row["item_name"], int(row["month"]))].append(Decimal(row["price"]))
    spark = SparkSession.builder.appName("de9-lhi-q7-verify").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    consumer = None
    try:
        base = f"s3a://{os.environ['S3_BUCKET']}"
        bronze = spark.read.parquet(f"{base}/bronze/product-prices/")
        silver = spark.read.parquet(f"{base}/silver/product-prices/")
        bronze_rows = bronze.collect()
        assert len(bronze_rows) == 200
        assert Counter(canonical(json.loads(bytes(row.value))) for row in bronze_rows) == expected
        # CSV 값 대조와 별도로 JSON의 공백·키 순서까지 원본 바이트로 비교합니다.
        consumer = KafkaConsumer(bootstrap_servers=["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"],
                                 group_id=None, enable_auto_commit=False, allow_auto_create_topics=False)
        kafka_bytes = {}
        for partition in range(3):
            target = TopicPartition("product-prices", partition)
            consumer.assign([target])
            consumer.seek_to_beginning(target)
            end = consumer.end_offsets([target])[target]
            deadline = time.monotonic() + 60
            while consumer.position(target) < end:
                if time.monotonic() > deadline:
                    raise TimeoutError("Kafka byte verification timed out")
                for records in consumer.poll(timeout_ms=1000).values():
                    for record in records:
                        kafka_bytes[(record.topic, record.partition, record.offset)] = (record.key, record.value)
        saved_bytes = {(row.topic, row.partition, row.offset): (bytes(row.key), bytes(row.value))
                       for row in bronze_rows}
        assert saved_bytes == kafka_bytes, "Bronze raw Kafka bytes/identifiers mismatch"
        assert isinstance(bronze.schema["value"].dataType, T.BinaryType)
        assert isinstance(silver.schema["price"].dataType, T.DoubleType)
        assert silver.columns == FIELDS, silver.columns
        assert Counter(canonical(row.asDict()) for row in silver.collect()) == expected
        spec = importlib.util.spec_from_file_location("price_batch", root / "part2-price/price_batch.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        summary = module.price_summary(silver).collect()
        assert len(summary) == 20
        for row in summary:
            values = prices[(row.item_name, row.month)]
            for actual, wanted in [(row.avg_price, sum(values)/len(values)),
                                   (row.max_price, max(values)), (row.min_price, min(values))]:
                assert math.isclose(actual, float(wanted), rel_tol=1e-9, abs_tol=1e-9), row
        # 변환 함수의 제외 규칙만 검사하며 Kafka/S3에 테스트 데이터를 쓰지 않습니다.
        cases = [None, "", "0", "-1", "bad", "NaN", "Infinity", "1.25"]
        rows = [(json.dumps({"price": price}),) for price in cases]
        clean = module.silver_events(spark.createDataFrame(rows, "value string")).collect()
        assert len(clean) == 1 and clean[0].price == 1.25, clean
        print("PASS: Bronze 200 rows; original Kafka key/value bytes and offsets match exactly", flush=True)
        print("PASS: Silver 200 rows; all CSV values/multiplicities match; price=double", flush=True)
        print("PASS: all 20 item/month mean/max/min aggregates match independent Decimal calculation", flush=True)
        print("PASS: missing/zero/negative/invalid/NaN/infinite prices excluded by transformation", flush=True)
        print("Q7 VERIFIED: read-only verification; no Kafka group/offset commit or S3 writes", flush=True)
    finally:
        if consumer is not None:
            consumer.close()
        spark.stop()


if __name__ == "__main__":
    main()
