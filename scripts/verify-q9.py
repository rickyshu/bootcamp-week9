"""Airflow 스케줄 성공·XCom·실제 Athena 결과를 읽기 전용으로 교차 검증합니다."""
from collections import Counter, defaultdict
import csv
from decimal import Decimal, ROUND_HALF_UP
import json
from pathlib import Path
from uuid import UUID

import boto3
from botocore.config import Config
from airflow.models import DagBag, DagModel, DagRun, XCom
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.session import create_session


DAG_ID = "price_pipeline_lhi"


def expected_top1(path):
    prices = defaultdict(dict)
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            prices[row["market"], row["item_name"]][int(row["month"])] = Decimal(row["price"])
    cities = defaultdict(list)
    for (city, item), months in prices.items():
        rate = ((months[12]-months[11])/months[11]*100).quantize(Decimal(".01"), rounding=ROUND_HALF_UP)
        cities[city].append((city, item, months[11], months[12], rate, 1))
    return Counter(row for rows in cities.values() for row in rows
                   if abs(row[4]) == max(abs(other[4]) for other in rows))


def main():
    root = Path(__file__).resolve().parents[1]
    bag = DagBag(dag_folder=str(root / "part2-price/dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.get_dag(DAG_ID)
    assert dag and len(dag.tasks) == 3
    assert isinstance(dag.get_task("spark_batch"), SparkSubmitOperator)
    assert dag.get_task("spark_batch").application == "/opt/de9/part2-price/price_batch.py"
    assert dag.get_task("spark_batch").application_args == ["--with-gold"]
    assert dag.max_active_runs == 1 and not dag.catchup
    assert dag.schedule_interval == "*/10 * * * *"
    expected = expected_top1(root / "data/india_grocery_prices.csv")
    assert sum(expected.values()) == 10
    client = boto3.client("athena", region_name="ap-northeast-2", config=Config(
        connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 3, "mode": "standard"}))
    reports = []
    with create_session() as session:
        model = session.query(DagModel).filter(DagModel.dag_id == DAG_ID).one()
        assert model.is_paused, "Pause the DAG after two successful scheduled runs before final verification"
        all_runs = (session.query(DagRun).filter(DagRun.dag_id == DAG_ID)
                    .order_by(DagRun.execution_date).all())
        scheduled = [run for run in all_runs if run.run_type == "scheduled" and run.state == "success"]
        assert len(scheduled) >= 2, [(run.run_id, run.state, run.run_type) for run in all_runs]
        assert not any(run.state in {"running", "queued"} for run in all_runs), "Run still active"
        for previous, current in zip(scheduled, scheduled[1:]):
            assert previous.end_date <= current.start_date, "Scheduled runs overlap"
        for run in scheduled:
            tasks = run.get_task_instances(session=session)
            assert len(tasks) == 3 and all(task.state == "success" for task in tasks)
            query_id = XCom.get_one(key="return_value", dag_id=DAG_ID, task_id="query_top1",
                                   run_id=run.run_id, session=session)
            UUID(query_id)
            execution = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
            assert execution["WorkGroup"] == "de9-lhi"
            assert execution["Status"]["State"] == "SUCCEEDED"
            assert "de9_lhi.grocery_price_change" in execution["Query"]
            assert "WHERE change_rank = 1" in execution["Query"]
            raw = []
            for page in client.get_paginator("get_query_results").paginate(QueryExecutionId=query_id):
                raw.extend(page["ResultSet"]["Rows"])
            header = [cell.get("VarCharValue") for cell in raw[0]["Data"]]
            assert header == ["market", "item_name", "price_nov", "price_dec", "change_rate", "change_rank"]
            actual = Counter()
            for row in raw[1:]:
                values = [cell["VarCharValue"] for cell in row["Data"]]
                actual[(values[0], values[1], Decimal(values[2]), Decimal(values[3]),
                        Decimal(values[4]), int(values[5]))] += 1
            assert actual == expected, f"Athena results differ from original CSV: {query_id}"
            reports.append({"run_id": run.run_id, "run_type": run.run_type, "state": run.state,
                            "start_date": run.start_date, "end_date": run.end_date,
                            "query_execution_id": query_id, "workgroup": execution["WorkGroup"],
                            "query": execution["Query"], "rows": len(raw)-1,
                            "results": [[str(value) for value in row] for row in sorted(actual)]})
    print(json.dumps({"verified": True, "dag_id": DAG_ID, "paused": True,
                      "scheduled_success_count": len(reports), "runs": reports},
                     ensure_ascii=False, default=str, indent=2), flush=True)


if __name__ == "__main__":
    main()
