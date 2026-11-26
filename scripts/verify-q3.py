"""Q3의 두 실제 처리 보고서를 원본 CSV와 읽기 전용으로 대조합니다."""
from collections import Counter
import csv
import json
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition


def main():
    reports = [json.loads(Path(f"/tmp/de9-q3-{name}.json").read_text()) for name in ("a", "b")]
    source = Path(__file__).resolve().parents[1] / "data/charger_sessions.csv"
    with source.open(encoding="utf-8-sig", newline="") as file:
        expected = Counter(row["endedBy"] for row in csv.DictReader(file))
    combined = Counter()
    ids = []
    for report in reports:
        assert report["group"] == "ev-consumer-lhi"
        assert report["total"] > 0, "한 Consumer가 전부 처리한 실행은 인정하지 않습니다."
        assert report["total"] == len(report["records"])
        actual = Counter(row[2] for row in report["records"])
        assert actual == Counter(report["ended_by"]), report["name"]
        for partition in report["partitions"]:
            latest = max(row[1] for row in report["records"] if row[0] == partition)
            assert report["committed"][str(partition)] == latest + 1
        combined.update(actual)
        ids.extend((partition, offset) for partition, offset, _ in report["records"])
        print(f"{report['name'].upper()}: partitions={report['partitions']} total={report['total']} endedBy={report['ended_by']}")
    # 리밸런싱으로 같은 파티션을 시간상 나눠 처리할 수 있습니다.
    # 파티션 이름의 겹침 대신 아래에서 (partition, offset)의 중복을 검사합니다.
    assert set(reports[0]["partitions"]) | set(reports[1]["partitions"]) == {0, 1, 2}
    assert len(ids) == len(set(ids)) == sum(expected.values()) == 175
    assert combined == expected, (combined, expected)
    # 다른 그룹에 가입하지 않는 probe로 현재 토픽의 실제 범위를 조회합니다.
    probe = KafkaConsumer(
        bootstrap_servers=["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"],
        group_id=None, enable_auto_commit=False, allow_auto_create_topics=False,
    )
    try:
        targets = [TopicPartition("ev-charging-sessions", number) for number in range(3)]
        begins = probe.beginning_offsets(targets)
        ends = probe.end_offsets(targets)
        assert set(ids) == {
            (target.partition, offset)
            for target in targets for offset in range(begins[target], ends[target])
        }
    finally:
        probe.close()
    print(f"PASS: combined endedBy matches original CSV: {dict(sorted(combined.items()))}")
    print("PASS: 175 distinct (partition,offset) records; both Consumers processed nonoverlapping records")


if __name__ == "__main__":
    main()
