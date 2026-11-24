"""Run on the EC2 host: register actual S3 Gold and verify an Athena SELECT."""
import argparse
import csv
import json
import re
import subprocess
import time
from collections import Counter
from pathlib import Path


def aws(*args):
    return json.loads(subprocess.check_output(["aws", *args, "--region", "ap-northeast-2", "--output", "json"]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", args.bucket):
        parser.error("Invalid S3 bucket name")
    root = Path(__file__).resolve().parents[1]
    output = root / ".runtime/q5/athena"
    output.mkdir(parents=True, exist_ok=True)
    database = "de9_lhi"
    table = "ev_charging_agg"
    gold = f"s3://{args.bucket}/gold/ev_charging_agg/"

    def execute(name, sql):
        (output / f"{name}.sql").write_text(sql + "\n")
        result = aws("athena", "start-query-execution", "--work-group", "de9-lhi", "--query-string", sql)
        query_id = result["QueryExecutionId"]
        print(f"{name} QueryExecutionId={query_id}", flush=True)
        for _ in range(90):
            result = aws("athena", "get-query-execution", "--query-execution-id", query_id)
            state = result["QueryExecution"]["Status"]["State"]
            if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                (output / f"{name}-execution.json").write_text(json.dumps(result, indent=2))
                if state != "SUCCEEDED":
                    raise RuntimeError(result["QueryExecution"]["Status"])
                break
            time.sleep(2)
        else:
            raise TimeoutError(f"Athena query still running: {query_id}")
        return query_id

    execute("create-database", f"CREATE DATABASE IF NOT EXISTS {database}")
    execute("create-table", f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} "
            "(location_group STRING, anomaly_count BIGINT) STORED AS PARQUET "
            f"LOCATION '{gold}'")
    metadata = aws("glue", "get-table", "--database-name", database, "--name", table)["Table"]
    assert metadata["StorageDescriptor"]["Location"].rstrip("/") == gold.rstrip("/")
    query_id = execute("select", f"SELECT location_group, anomaly_count FROM {database}.{table} ORDER BY location_group")
    result = aws("athena", "get-query-results", "--query-execution-id", query_id)
    (output / "select-results.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
    rows = result["ResultSet"]["Rows"][1:]
    actual = {row["Data"][0]["VarCharValue"]: int(row["Data"][1]["VarCharValue"]) for row in rows}
    assert len(rows) == len(actual), "Duplicate Athena group rows"
    expected = Counter()
    def seconds(value):
        h, m, s = map(int, value.split(":"))
        return h * 3600 + m * 60 + s
    with (root / "data/charger_sessions.csv").open(newline="", encoding="utf-8-sig") as stream:
        for row in csv.DictReader(stream):
            if seconds(row["totalDuration"]) - seconds(row["chargingDuration"]) > 5400:
                expected[re.sub(r" #\d+$", "", row["stationName"])] += 1
    assert actual == dict(expected), (actual, expected)
    print(json.dumps(actual, ensure_ascii=False, indent=2))
    print(f"ATHENA VERIFIED: {len(actual)} groups; {sum(actual.values())} anomalies", flush=True)
    print("Open this SELECT in the actual Athena Query Editor for the required console screenshot.", flush=True)


if __name__ == "__main__":
    main()
