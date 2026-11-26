"""실제 Q3 함수에 가짜 Kafka I/O를 연결하는 로컬 회귀 검사입니다."""
import argparse
import ast
from collections import Counter, namedtuple
from contextlib import redirect_stdout
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import signal
import tempfile
import time
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
TP = namedtuple("TopicPartition", "topic partition")
OM = namedtuple("OffsetAndMetadata", "offset metadata leader_epoch")
RECORD = namedtuple("Record", "partition offset value")


def load_program():
    # Kafka 패키지/브로커 없이도 프로덕션 함수 본문을 그대로 실행합니다.
    tree = ast.parse((ROOT / "part1-ev/charger_consumer.py").read_text())
    definitions = [node for node in tree.body
                   if isinstance(node, (ast.Assign, ast.ClassDef, ast.FunctionDef))]
    scope = dict(argparse=argparse, Counter=Counter, datetime=datetime,
                 timezone=timezone, json=json, Path=Path, signal=signal,
                 time=time, TopicPartition=TP, OffsetAndMetadata=OM,
                 ConsumerRebalanceListener=object)
    exec(compile(ast.Module(body=definitions, type_ignores=[]),
                 "charger_consumer.py", "exec"), scope)
    return scope


class BrokerDouble:
    def __init__(self):
        self.offsets = {}
        self.positions = {}
        self.paused = set()

    def committed(self, tp):
        return self.offsets.get(tp)

    def seek(self, tp, offset):
        self.positions[tp] = offset

    def pause(self, *parts):
        self.paused.update(parts)

    def resume(self, *parts):
        self.paused.difference_update(parts)


class RebalanceTests(unittest.TestCase):
    def setUp(self):
        self.scope = load_program()
        self.consumer = BrokerDouble()
        self.p0, self.p1 = [TP("ev-charging-sessions", p) for p in (0, 1)]
        self.ends = {self.p0: 2, self.p1: 3}
        self.listener = self.scope["RebalanceLog"](self.consumer, "A", self.ends)

    def test_initial_assignment_waits_for_start(self):
        self.listener.on_partitions_assigned([self.p0])
        self.assertEqual(self.consumer.paused, {self.p0})
        self.assertFalse(self.listener.started)
        self.listener.start([self.p0])
        self.assertTrue(self.listener.started)
        self.assertFalse(self.consumer.paused)
        self.assertEqual(self.consumer.positions[self.p0], 0)

    def test_reassignment_seeks_broker_commit_and_resumes(self):
        self.listener.start([self.p0])
        self.consumer.offsets = {self.p0: 1, self.p1: 2}
        self.consumer.positions[self.p0] = 2  # 아직 처리하지 않은 선조회 위치
        self.listener.on_partitions_revoked([self.p0])
        self.listener.on_partitions_assigned([self.p0, self.p1])
        self.assertEqual(self.consumer.positions, {self.p0: 1, self.p1: 2})
        self.assertFalse(self.consumer.paused)
        self.assertEqual(self.listener.next_offsets, {self.p0: 1, self.p1: 2})

    def test_completed_hold_can_take_unfinished_partition(self):
        self.listener.start([self.p0])
        self.listener.reported = True
        self.consumer.offsets = {self.p0: 2, self.p1: 1}
        self.consumer.pause(self.p0, self.p1)
        self.listener.on_partitions_assigned([self.p0, self.p1])
        self.assertFalse(self.listener.reported)
        self.assertEqual(self.consumer.paused, {self.p0})
        self.assertEqual(self.listener.next_offsets[self.p1], 1)

    def test_empty_assignment_and_invalid_initial_offsets(self):
        self.listener.start([])
        self.listener.on_partitions_assigned([])
        self.assertEqual(self.listener.next_offsets, {})
        self.consumer.offsets[self.p0] = 1
        with self.assertRaises(RuntimeError):
            self.scope["RebalanceLog"](self.consumer, "A", self.ends).start([self.p0])

    def run_main_scenario(self, initial_end):
        # 다른 멤버가 p1 offset 0을 처리한 뒤 남은 offset 1부터 이어 읽습니다.
        scope = load_program()
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            start, stop = tmp / "start", tmp / "stop"
            p0, p1, p2 = [TP("ev-charging-sessions", p) for p in range(3)]

            class Consumer(BrokerDouble):
                def __init__(self, **config):
                    super().__init__()
                    self.step = 0
                    self.assigned = {p0}
                    self.closed = False
                    self.config = config
                    self.commits = []

                def end_offsets(self, targets):
                    return {p0: initial_end, p1: 2, p2: 173 - initial_end}

                def subscribe(self, topics, listener):
                    self.listener = listener

                def assignment(self):
                    return self.assigned

                def poll(self, timeout_ms):
                    self.step += 1
                    if self.step == 1:
                        self.listener.on_partitions_assigned([p0])
                        start.touch()
                        return {}
                    if self.step == 2:
                        return {p0: [RECORD(0, 0, b'{"endedBy":"Customer"}')]}
                    if self.step == 3:
                        self.listener.on_partitions_revoked([p0])
                        self.offsets[p1] = 1  # 다른 멤버가 처리 후 커밋한 다음 위치
                        self.assigned = {p0, p1}
                        self.listener.on_partitions_assigned(self.assigned)
                        assert p1 not in self.paused
                        assert self.positions[p1] == 1
                        records = {p1: [RECORD(1, 1, b'{"endedBy":"CPS Server"}')]}
                        if initial_end == 2:
                            assert p0 not in self.paused
                            records[p0] = [RECORD(0, 1, b'{"endedBy":"Plug Out at Vehicle"}')]
                        return records
                    stop.touch()
                    return {}

                def commit(self, offsets):
                    self.commits.append(offsets)
                    self.offsets.update({tp: value.offset for tp, value in offsets.items()})

                def close(self):
                    self.closed = True

            created = []

            def create(**config):
                instance = Consumer(**config)
                created.append(instance)
                return instance

            scope["KafkaConsumer"] = create
            # /tmp の本番レポートには触れず、全て一時ディレクトリに閉じます。
            scope["Path"] = lambda value: (tmp / "report.json"
                                            if str(value).startswith("/tmp/de9-q3-")
                                            else Path(value))
            args = ["consumer", "--name", "a", "--start-file", str(start), "--stop-file", str(stop)]
            with patch("sys.argv", args), patch.object(signal, "signal"), redirect_stdout(io.StringIO()):
                scope["main"]()
            result = json.loads((tmp / "report.json").read_text())
            self.assertEqual(result["total"], initial_end + 1)
            self.assertEqual(result["ended_by"]["Customer"], 1)
            self.assertEqual(result["ended_by"]["CPS Server"], 1)
            self.assertEqual(result["assigned_partitions"], [0, 1])
            self.assertEqual(result["committed"], {"0": initial_end, "1": 2})
            self.assertNotIn([1, 0, "CPS Server"], result["records"])
            self.assertTrue(created[0].closed)
            self.assertFalse(created[0].config["enable_auto_commit"])

    def test_main_rebalance_mid_consumption(self):
        self.run_main_scenario(initial_end=2)

    def test_main_refreshes_report_after_hold_reassignment(self):
        self.run_main_scenario(initial_end=1)


if __name__ == "__main__":
    unittest.main()
