"""Kafka에 연결하지 않고 파티셔너 계약과 send() 사용법을 검사합니다."""
import ast
from collections import Counter
import importlib.util
from pathlib import Path

from kafka.partitioner.default import DefaultPartitioner


root = Path(__file__).resolve().parents[1]
source = root / "part2-price/price_producer.py"
spec = importlib.util.spec_from_file_location("price_producer", source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
partitioner = module.RegionPartitioner()
groups = [("Delhi", "Jaipur", "Lucknow", "Kanpur"),
          ("Mumbai", "Ahmedabad", "Bhopal"), ("Hyderabad", "Chennai", "Patna")]
for expected, cities in enumerate(groups):
    for city in cities:
        # leader 가용성에 따라 권역이 바뀌지 않아야 합니다.
        assert partitioner(city.encode(), [0, 1, 2], [p for p in range(3) if p != expected]) == expected
for city in ["Unknown", "Seoul", "Kolkata"]:
    for available in [[], [0], [1, 2], [0, 1, 2]]:
        key = city.encode()
        assert partitioner(key, [0, 1, 2], available) == DefaultPartitioner()(key, [0, 1, 2], available)
assert partitioner(None, [0, 1, 2], [2]) == 2
for node in ast.walk(ast.parse(source.read_text())):
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "send":
        assert not any(keyword.arg == "partition" for keyword in node.keywords), "send(partition=) forbidden"
events = module.load_events(root / "data/india_grocery_prices.csv")
counts = Counter(partitioner(e["market"].encode(), [0, 1, 2], [0, 1, 2]) for e in events)
assert counts == {0: 80, 1: 60, 2: 60}, counts
print("PASS: 10 cities, unavailable leaders, unknown/default fallback, null key, no send(partition=)")
print(f"PASS: original CSV 200 rows; expected partition counts={dict(sorted(counts.items()))}")
print("Local validation only: no Kafka connection or message send performed")
