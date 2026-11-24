"""Read-only verification of real Gold parquet and acknowledged Slack event receipts."""
import csv
import json
import os
import re
from collections import Counter

from pyspark.sql import SparkSession


def seconds(value):
    hours, minutes, secs = map(int, value.split(":"))
    return hours * 3600 + minutes * 60 + secs


def main():
    expected_events = Counter()
    expected_groups = Counter()
    with open("/opt/de9/data/charger_sessions.csv", newline="", encoding="utf-8-sig") as stream:
        for row in csv.DictReader(stream):
            idle = seconds(row["totalDuration"]) - seconds(row["chargingDuration"])
            if idle > 5400:
                group = re.sub(r" #\d+$", "", row["stationName"])
                expected_events[(row["stationName"], group, row["startTime"], idle)] += 1
                expected_groups[group] += 1
    spark = SparkSession.builder.appName("de9-lhi-q5-verify").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        base = f"s3a://{os.environ['S3_BUCKET']}"
        gold = spark.read.parquet(f"{base}/gold/ev_charging_agg")
        actual_groups = {r.location_group: r.anomaly_count for r in gold.collect()}
        assert gold.count() == len(actual_groups), "Duplicate Gold group rows"
        assert actual_groups == dict(expected_groups), (actual_groups, expected_groups)
        path = spark._jvm.org.apache.hadoop.fs.Path(f"{base}/checkpoints/q5/alerts-sent/*.json")
        filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        actual_events = Counter()
        receipts = filesystem.globStatus(path) or []
        for status in receipts:
            source = filesystem.open(status.getPath())
            scanner = spark._jvm.java.util.Scanner(source, "UTF-8").useDelimiter("\\A")
            try:
                receipt = json.loads(scanner.next())
            finally:
                scanner.close()
            assert receipt["slack_ack"] == "ok"
            for row in receipt["events"]:
                actual_events[(row["stationName"], row["locationGroup"], row["startTime"], row["idleSec"])] += 1
        assert actual_events == expected_events, (actual_events - expected_events, expected_events - actual_events)
        gold.orderBy("location_group").show(1000, truncate=False)
        print(f"Q5 VERIFIED: {sum(expected_events.values())} anomaly events; "
              f"{len(expected_groups)} Gold groups; {len(receipts)} Slack ACK receipts", flush=True)
        print("Slack receipt verification is API acknowledgement; confirm received messages in Slack UI separately.", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
