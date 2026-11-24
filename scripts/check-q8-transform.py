"""Spark 메모리 입력으로 Q8 윈도우·반올림·순위·입력 검사를 검증합니다."""
import argparse
import ast
from decimal import Decimal
import importlib.util
from pathlib import Path

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[1] / "part2-price/price_change.py")
    args = parser.parse_args()
    tree = ast.parse(args.source.read_text())
    calls = [node.func.attr for node in ast.walk(tree)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
             and not isinstance(node.func.value, ast.Constant)]
    assert "lag" in calls and "rank" in calls and "join" not in calls
    spec = importlib.util.spec_from_file_location("price_change", args.source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    spark = SparkSession.builder.appName("de9-lhi-q8-transform-check").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        pairs = [("A", 100., 110.), ("B", 100., 60.), ("C", 100., 120.),
                 ("D", 100., 80.), ("E", 20000., 20001.), ("F", 20000., 19999.)]
        # 12월을 먼저 배치해도 윈도우가 월 순서로 전월을 찾아야 합니다.
        data = [("City", item, 2016, month, price) for item, nov, dec in pairs
                for month, price in [(12, dec), (11, nov)]]
        schema = "market string, item_name string, year int, month int, price double"
        frame = spark.createDataFrame(data, schema)
        module.validate_pairs(frame)
        actual = {r.item_name: (r.price_nov, r.price_dec, r.change_rate, r.change_rank)
                  for r in module.change_ranks(frame).collect()}
        expected = {"A": (100., 110., Decimal("10.00"), 4),
                    "B": (100., 60., Decimal("-40.00"), 1),
                    "C": (100., 120., Decimal("20.00"), 2),
                    "D": (100., 80., Decimal("-20.00"), 2),
                    "E": (20000., 20001., Decimal("0.01"), 5),
                    "F": (20000., 19999., Decimal("-0.01"), 5)}
        assert actual == expected, actual
        invalid_cases = [data[:-1], data + [data[0]], [("City", "bad", 2016, 11, 0.)]]
        for rows in invalid_cases:
            try:
                module.validate_pairs(spark.createDataFrame(rows, schema))
            except ValueError:
                pass
            else:
                raise AssertionError("Invalid pair/price was accepted")
        print("PASS: lag across unordered months, signed changes, absolute-value competition rank and ties", flush=True)
        print("PASS: positive/negative half-up rounding; missing/duplicate month and zero price rejected", flush=True)
        print("PASS: source uses lag/rank and no join; memory inputs only, no Kafka/S3 access", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
