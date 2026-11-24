"""메모리의 입력만 사용해 Spark 정제·집계를 검사합니다. Kafka/S3 접근 없음."""
import argparse
import importlib.util
import json
from pathlib import Path

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path,
                        default=Path(__file__).resolve().parents[1] / "part2-price/price_batch.py")
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("price_batch", args.source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    spark = SparkSession.builder.appName("de9-lhi-q7-transform-check").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        prices = [None, "", "0", "-1", "bad", "NaN", "Infinity", 2.5, "7.5"]
        events = [json.dumps({"item_name": "Test", "market": "Test", "year": 2016,
                              "month": 11, "price": price, "currency": "INR", "unit": "KG"})
                  for price in prices]
        # price 자체가 없는 메시지와 파싱할 수 없는 JSON도 제외돼야 합니다.
        events += ['{"item_name":"missing"}', '{broken']
        raw = spark.createDataFrame([(event,) for event in events], "value string")
        silver = module.silver_events(raw)
        assert sorted(row.price for row in silver.collect()) == [2.5, 7.5]
        assert silver.schema["price"].dataType.simpleString() == "double"
        result = module.price_summary(silver).collect()
        assert len(result) == 1
        row = result[0]
        assert (row.item_name, row.month, row.avg_price, row.max_price, row.min_price) == ("Test", 11, 5.0, 7.5, 2.5)
        print("PASS: numeric/string prices become double; missing/null/empty/zero/negative/invalid/nonfinite values excluded", flush=True)
        print("PASS: item/month average, maximum and minimum match independent expected values", flush=True)
        print("Transformation check only: no Kafka/S3 access or writes", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
