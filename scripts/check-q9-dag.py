"""Q9 DAG를 파싱해 제출 연산자·의존 관계·스케줄 설정을 확인합니다. 실행 없음."""
import argparse
from pathlib import Path

from airflow.models import DagBag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dag_cycle_tester import check_cycle


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag-file", type=Path,
                        default=Path(__file__).resolve().parents[1] / "part2-price/dags/price_pipeline_dag.py")
    args = parser.parse_args()
    bag = DagBag(dag_folder=str(args.dag_file), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags["price_pipeline_lhi"]
    check_cycle(dag)
    assert set(dag.task_ids) == {"spark_batch", "register_athena", "query_top1"}
    batch = dag.get_task("spark_batch")
    assert isinstance(batch, SparkSubmitOperator)
    assert batch.application == "/opt/de9/part2-price/price_batch.py"
    assert batch.application_args == ["--with-gold"]
    assert batch.py_files == "/opt/de9/part2-price/price_change.py"
    assert batch.downstream_task_ids == {"register_athena"}
    assert dag.get_task("register_athena").downstream_task_ids == {"query_top1"}
    assert dag.schedule_interval == "*/10 * * * *" and not dag.catchup
    assert dag.max_active_runs == 1 and dag.max_active_tasks == 1
    print("PASS: DAG parse/cycle check, SparkSubmitOperator price_batch.py --with-gold, three ordered tasks", flush=True)
    print("PASS: ten-minute schedule, no catchup, max_active_runs=1, max_active_tasks=1", flush=True)
    print("Parse validation only: no Spark task or AWS query executed; no scheduled success evidence", flush=True)


if __name__ == "__main__":
    main()
