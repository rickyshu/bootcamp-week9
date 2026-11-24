"""Q8 Gold 100행을 원본 CSV의 독립 Decimal 계산과 비교합니다. S3 읽기 전용."""
from collections import Counter, defaultdict
import csv
from decimal import Decimal, ROUND_HALF_UP
import os
from pathlib import Path

from pyspark.sql import SparkSession


COLUMNS = ["market", "item_name", "price_nov", "price_dec", "change_rate", "change_rank"]


def expected_rows(path):
    pairs = defaultdict(dict)
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            key = (row["market"], row["item_name"])
            month = int(row["month"])
            assert month not in pairs[key], (key, month)
            pairs[key][month] = Decimal(row["price"])
    rates, by_city = {}, defaultdict(list)
    for (market, item), months in pairs.items():
        rate = ((months[12] - months[11]) / months[11] * 100).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
        rates[market, item] = (months[11], months[12], rate)
        by_city[market].append((item, abs(rate)))
    expected = Counter()
    for market, items in by_city.items():
        for item, magnitude in items:
            rank = 1 + sum(other > magnitude for _, other in items)
            nov, dec, rate = rates[market, item]
            expected[(market, item, nov, dec, rate, rank)] += 1
    return expected


def main():
    expected = expected_rows(Path(__file__).resolve().parents[1] / "data/india_grocery_prices.csv")
    assert sum(expected.values()) == 100
    spark = SparkSession.builder.appName("de9-lhi-q8-verify").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        gold = spark.read.parquet(f"s3a://{os.environ['S3_BUCKET']}/gold/grocery_price_change/")
        assert gold.columns == COLUMNS, gold.columns
        assert gold.schema["change_rate"].dataType.simpleString() == "decimal(18,2)"
        actual = Counter((r.market, r.item_name, Decimal(str(r.price_nov)), Decimal(str(r.price_dec)),
                          r.change_rate, r.change_rank) for r in gold.collect())
        assert actual == expected, f"Gold mismatch: missing={expected-actual}; extra={actual-expected}"
        top = sorted(row for row in actual if row[-1] == 1)
        assert len(top) == 10 and len({row[0] for row in top}) == 10
        print("PASS: all 100 city/item Gold rows, six columns, two prices, rounded rate and competition rank match CSV", flush=True)
        print("PASS: exactly 10 cities with one top1 each; original CSV calculated independently using Decimal", flush=True)
        for row in top:
            print(f"{row[0]}: {row[1]} | {row[2]} -> {row[3]} | {row[4]}% | rank={row[5]}", flush=True)
        print("Q8 VERIFIED: read-only S3 verification; no output writes", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
