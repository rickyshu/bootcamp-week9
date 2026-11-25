"""Read committed offsets and log bounds without joining or consuming the group."""
import argparse
import json

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient


def snapshot(group, bootstrap_servers, topic="ev-charging-sessions"):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers,
                             client_id="de9-lhi-lag-admin", request_timeout_ms=15000)
    consumer = None
    try:
        description = admin.describe_consumer_groups([group])[0]
        if description.error_code:
            raise RuntimeError(f"Group lookup failed: {description.error_code}")
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers, group_id=group,
            client_id="de9-lhi-lag-monitor", enable_auto_commit=False,
            allow_auto_create_topics=False, request_timeout_ms=30000,
        )
        partitions = sorted(consumer.partitions_for_topic(topic) or [])
        if not partitions:
            raise RuntimeError(f"Topic has no partitions: {topic}")
        targets = [TopicPartition(topic, number) for number in partitions]
        begins = consumer.beginning_offsets(targets)
        ends = consumer.end_offsets(targets)
        rows = []
        for target in targets:
            current = consumer.committed(target)
            # An absent commit is unknown, not zero; do not invent its lag.
            lag = None if current is None else ends[target] - current
            if current is not None and not begins[target] <= current <= ends[target]:
                raise RuntimeError(f"Committed offset outside retained log: {target}={current}")
            rows.append({"topic": topic, "partition": target.partition,
                         "beginning_offset": begins[target], "current_offset": current,
                         "log_end_offset": ends[target], "lag": lag})
        return {"group": group, "state": description.state,
                "members": len(description.members), "rows": rows,
                "total_lag": None if any(row["lag"] is None for row in rows)
                else sum(row["lag"] for row in rows)}
    finally:
        if consumer is not None:
            consumer.close(autocommit=False)
        admin.close()


def print_table(report):
    print(f"GROUP: {report['group']}  STATE: {report['state']}  MEMBERS: {report['members']}")
    headers = ("topic", "partition", "current-offset", "log-end-offset", "lag")
    rows = [(row["topic"], row["partition"], row["current_offset"],
             row["log_end_offset"], row["lag"]) for row in report["rows"]]
    cells = [["UNKNOWN" if value is None else str(value) for value in row]
             for row in [headers, *rows]]
    widths = [max(len(row[column]) for row in cells) for column in range(len(headers))]
    for index, row in enumerate(cells):
        print(" | ".join(value.ljust(width) for value, width in zip(row, widths)))
        if index == 0:
            print("-+-".join("-" * width for width in widths))
    print(f"TOTAL LAG: {report['total_lag'] if report['total_lag'] is not None else 'UNKNOWN'}")
    print("READ ONLY: no subscribe, poll, fetch, or offset commit")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("group", help="Existing consumer group name")
    parser.add_argument("--bootstrap-servers", default="kafka-1:9092,kafka-2:9092,kafka-3:9092")
    parser.add_argument("--topic", default="ev-charging-sessions")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = snapshot(args.group, args.bootstrap_servers.split(","), args.topic)
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print_table(report)


if __name__ == "__main__":
    main()
