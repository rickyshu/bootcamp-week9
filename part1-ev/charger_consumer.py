"""Q3: 같은 그룹의 두 Consumer로 분담 소비하고 처리 후 수동 커밋합니다.

start 파일 전에는 할당 파티션을 pause하고 poll로 그룹에만 참여합니다.
두 멤버의 리밸런싱이 끝난 뒤 호스트에서 start 파일을 생성합니다.
소비가 끝나도 poll을 유지하고, stop 파일이 생기면 종료합니다.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import signal
import time

from kafka import KafkaConsumer, TopicPartition
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.structs import OffsetAndMetadata


TOPIC = "ev-charging-sessions"
GROUP = "ev-consumer-lhi"
ENDED_BY = ("CPS Server", "Customer", "Plug Out at Vehicle")


class RebalanceLog(ConsumerRebalanceListener):
    def __init__(self, consumer, label, ends):
        self.consumer = consumer
        self.label = label
        self.ends = ends
        self.history = []
        self.started = False
        self.reported = False
        self.next_offsets = {}

    def record(self, phase, partitions):
        ids = sorted(partition.partition for partition in partitions)
        self.history.append({"phase": phase, "partitions": ids})
        print(f"[{self.label}] REBALANCE {phase}: {ids}", flush=True)

    def on_partitions_revoked(self, revoked):
        # poll 위치 전체를 커밋하면 아직 처리하지 않은 레코드를 건너뛸 수 있으므로
        # 콜백에서 추가 커밋하지 않습니다. 각 레코드 처리 직후 명시적으로 커밋합니다.
        self.record("revoked", revoked)

    def on_partitions_assigned(self, assigned):
        self.reported = False
        if self.started:
            self.activate(assigned, require_empty=False)
            self.record("assigned (resumed from committed offsets)", assigned)
        else:
            if assigned:
                self.consumer.pause(*assigned)
            self.record("assigned (paused)", assigned)

    def activate(self, assigned, require_empty):
        # 새 담당자는 이전 담당자가 Kafka에 커밋한 다음 위치부터 읽습니다.
        # 이전 프로세스의 로컬 집계나 미처리 poll 위치를 가져오지 않습니다.
        next_offsets = {}
        for target in assigned:
            previous = self.consumer.committed(target)
            if require_empty and previous not in (None, 0):
                raise RuntimeError(f"이미 처리된 그룹 오프셋이 있습니다: {target}={previous}")
            offset = 0 if previous is None else previous
            if not 0 <= offset <= self.ends[target]:
                raise RuntimeError(f"실험 범위 밖의 커밋 위치입니다: {target}={offset}")
            self.consumer.seek(target, offset)
            next_offsets[target] = offset
        self.next_offsets = next_offsets
        pending = [tp for tp in assigned if next_offsets[tp] < self.ends[tp]]
        complete = [tp for tp in assigned if next_offsets[tp] == self.ends[tp]]
        if complete:
            self.consumer.pause(*complete)
        if pending:
            self.consumer.resume(*pending)

    def start(self, assigned):
        self.activate(assigned, require_empty=True)
        self.started = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True, choices=("a", "b"))
    parser.add_argument("--start-file", type=Path, default=Path("/opt/de9/.runtime/q3/start"))
    parser.add_argument("--stop-file", type=Path, default=Path("/opt/de9/.runtime/q3/stop"))
    args = parser.parse_args()
    label = args.name.upper()
    report_path = Path(f"/tmp/de9-q3-{args.name}.json")
    if report_path.exists():
        raise RuntimeError(f"이 Consumer의 기존 실행 기록이 있습니다: {report_path}")
    if args.start_file.exists() or args.stop_file.exists():
        raise RuntimeError("start/stop 파일이 이미 있습니다. 기존 실험을 확인하고 새 실행을 준비하세요.")

    stop_requested = False

    def request_stop(signum, frame):
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    consumer = KafkaConsumer(
        bootstrap_servers=["kafka-1:9092", "kafka-2:9092", "kafka-3:9092"],
        group_id=GROUP,
        client_id=f"de9-lhi-q3-{args.name}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        allow_auto_create_topics=False,
        session_timeout_ms=15_000,
        heartbeat_interval_ms=3000,
        max_poll_interval_ms=120_000,
        max_poll_records=10,
    )
    counts = Counter({name: 0 for name in ENDED_BY})
    processed = []
    committed = {}
    try:
        targets = [TopicPartition(TOPIC, number) for number in range(3)]
        ends = consumer.end_offsets(targets)
        if sum(ends.values()) != 175:
            raise RuntimeError(f"Q2의 175건 토픽이 필요합니다: {ends}")
        listener = RebalanceLog(consumer, label, ends)
        consumer.subscribe([TOPIC], listener=listener)
        print(f"[{label}] group={GROUP}; auto_commit=False; per-record manual commit", flush=True)
        print(f"[{label}] Waiting for two members; poll continues while paused", flush=True)
        deadline = time.monotonic() + 300
        while not stop_requested and not args.stop_file.exists():
            batches = consumer.poll(timeout_ms=1000)
            assigned = consumer.assignment()
            if not listener.started:
                if batches:
                    raise RuntimeError("시작 전 레코드가 반환됐습니다. 커밋하지 않고 종료합니다.")
                if not args.start_file.exists():
                    if time.monotonic() > deadline:
                        raise TimeoutError("2개 멤버 확인 후 시작 신호가 300초 안에 필요합니다.")
                    continue
                if not assigned:
                    continue
                listener.start(assigned)
                print(f"[{label}] START partitions={sorted(tp.partition for tp in assigned)}", flush=True)
                continue

            for target, records in batches.items():
                for record in records:
                    if record.offset >= ends[target]:
                        raise RuntimeError("실험 시작 후 새 레코드가 추가됐습니다.")
                    event = json.loads(record.value.decode("utf-8"))
                    ended_by = event["endedBy"]
                    if ended_by not in ENDED_BY:
                        raise ValueError(f"알 수 없는 endedBy: {ended_by}")
                    # 처리(집계)가 끝난 레코드의 다음 오프셋만 동기 커밋합니다.
                    counts[ended_by] += 1
                    consumer.commit({target: OffsetAndMetadata(record.offset + 1, "", -1)})
                    committed[target] = record.offset + 1
                    listener.next_offsets[target] = record.offset + 1
                    processed.append([record.partition, record.offset, ended_by])
                    if len(processed) % 25 == 0:
                        print(f"[{label}] processed={len(processed)}; committed p{record.partition}={record.offset+1}", flush=True)

            if not listener.reported and assigned and all(listener.next_offsets[tp] >= ends[tp] for tp in assigned):
                report = {
                    "name": args.name,
                    "group": GROUP,
                    "topic": TOPIC,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "partitions": sorted({partition for partition, _, _ in processed}),
                    "assigned_partitions": sorted(tp.partition for tp in assigned),
                    "ended_by": dict(counts),
                    "total": len(processed),
                    "committed": {str(tp.partition): offset for tp, offset in committed.items()},
                    "records": processed,
                    "rebalances": listener.history,
                }
                temporary = report_path.with_suffix(".tmp")
                temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2))
                temporary.replace(report_path)
                print(f"[{label}] RESULT total={len(processed)} partitions={report['partitions']}", flush=True)
                for ended_by in ENDED_BY:
                    print(f"[{label}] {ended_by}: {counts[ended_by]}", flush=True)
                print(f"[{label}] committed={report['committed']}", flush=True)
                print(f"[{label}] HOLD: still polling; member stays active for capture", flush=True)
                consumer.pause(*assigned)
                listener.reported = True
    finally:
        consumer.close()
        print(f"[{label}] STOPPED; processed={len(processed)}", flush=True)


if __name__ == "__main__":
    main()
