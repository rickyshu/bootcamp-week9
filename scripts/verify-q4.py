"""Read-only verification of actual S3 parquet against the complete original CSV."""
import csv
import os
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def seconds(value):
    hours, minutes, secs = map(int, value.split(":"))
    return hours * 3600 + minutes * 60 + secs


def canonical(row, columns):
    result = dict(row)
    result["portNumber"] = int(result["portNumber"])
    result["energyKwh"] = Decimal(str(result["energyKwh"]))
    return tuple(result[name] for name in columns)


def main():
    csv_path = Path("/opt/de9/data/charger_sessions.csv")
    with csv_path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        columns = reader.fieldnames
        original = list(reader)
    expected_bronze = Counter(canonical(row, columns) for row in original)
    expected_silver = Counter()
    expected_counts = Counter()
    silver_columns = columns + ["locationGroup", "idleSec"]
    for row in original:
        row = dict(row)
        row["locationGroup"] = re.sub(r" #\d+$", "", row["stationName"])
        row["totalDuration"] = seconds(row["totalDuration"])
        row["chargingDuration"] = seconds(row["chargingDuration"])
        row["idleSec"] = row["totalDuration"] - row["chargingDuration"]
        expected_counts[row["locationGroup"]] += 1
        expected_silver[canonical(row, silver_columns)] += 1
    spark = SparkSession.builder.appName("de9-lhi-q4-verify").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        bucket = os.environ["S3_BUCKET"]
        frames = {}
        for layer, fields, expected in [("bronze", columns, expected_bronze),
                                         ("silver", silver_columns, expected_silver)]:
            root = f"s3a://{bucket}/{layer}/ev-charging-sessions"
            frame = spark.read.parquet(root)
            actual = Counter(canonical(row.asDict(), fields) for row in frame.select(*fields).collect())
            assert actual == expected, f"{layer}: missing={expected-actual}; extra={actual-expected}"
            assert frame.filter(F.col("dt").cast("string") != F.substring("startTime", 1, 10)).count() == 0
            print(f"VERIFIED {layer}: {sum(actual.values())} rows; all values and multiplicities match CSV", flush=True)
            frames[layer] = frame
        silver = frames["silver"]
        for field in ["totalDuration", "chargingDuration", "idleSec"]:
            assert silver.schema[field].dataType.simpleString() == "bigint", (field, silver.schema)
        actual_counts = Counter({row.locationGroup: row["count"] for row in
                                 silver.groupBy("locationGroup").count().collect()})
        assert actual_counts == expected_counts
        silver.groupBy("locationGroup").count().orderBy("locationGroup").show(1000, truncate=False)
        print(f"Q4 VERIFIED: {len(actual_counts)} location groups; {sum(actual_counts.values())} sessions", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
