"""Check the actual Q3 group before or after CLI reset; never commit offsets."""
import argparse
import csv
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "part2-price"))
from kafka_lag_monitor import print_table, snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("before", "after"), required=True)
    args = parser.parse_args()
    report = snapshot("ev-consumer-lhi", ["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"])
    print_table(report)
    assert report["members"] == 0 and report["state"] == "Empty", report
    with (ROOT / "data/charger_sessions.csv").open(encoding="utf-8-sig", newline="") as source:
        expected_count = sum(1 for _ in csv.DictReader(source))
    assert [row["partition"] for row in report["rows"]] == [0, 1, 2], report
    assert sum(row["log_end_offset"] - row["beginning_offset"]
               for row in report["rows"]) == expected_count, report
    for row in report["rows"]:
        assert row["beginning_offset"] == 0, "Original data expired; do not reset"
        target = row["log_end_offset"] if args.phase == "before" else row["beginning_offset"]
        assert row["current_offset"] == target, report
    expected_lag = 0 if args.phase == "before" else expected_count
    assert report["total_lag"] == expected_lag, report
    print(f"PASS Q10 {args.phase}: inactive group; total lag={expected_lag}; source rows={expected_count}")


if __name__ == "__main__":
    main()
