"""Q9: 10분 주기의 Spark→Athena 등록→SELECT/XCom. 최초 상태는 pause."""
from datetime import timedelta
import hashlib
import logging
import os
import re
import time

import boto3
from botocore.config import Config
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


DAG_ID = "price_pipeline_lhi"
DATABASE = "de9_lhi"
TABLE = "grocery_price_change"
WORKGROUP = "de9-lhi"
REGION = "ap-northeast-2"
SELECT_SQL = (
    "SELECT market, item_name, price_nov, price_dec, change_rate, change_rank "
    f"FROM {DATABASE}.{TABLE} WHERE change_rank = 1 ORDER BY market"
)
CLIENT_CONFIG = Config(connect_timeout=5, read_timeout=15,
                       retries={"total_max_attempts": 3, "mode": "standard"})


def execute_query(client, sql, label, run_id):
    token = hashlib.sha256(f"{DAG_ID}|{run_id}|{label}|{sql}".encode()).hexdigest()
    query_id = client.start_query_execution(
        QueryString=sql, WorkGroup=WORKGROUP, ClientRequestToken=token)["QueryExecutionId"]
    logging.info("%s QueryExecutionId=%s workgroup=%s", label, query_id, WORKGROUP)
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        execution = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        status = execution["Status"]
        if status["State"] == "SUCCEEDED":
            return query_id
        if status["State"] in {"FAILED", "CANCELLED"}:
            raise RuntimeError(f"Athena {query_id}: {status}")
        time.sleep(2)
    client.stop_query_execution(QueryExecutionId=query_id)
    raise TimeoutError(f"Athena query timed out: {query_id}")


def register_gold():
    bucket = os.environ["S3_BUCKET"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", bucket):
        raise ValueError("Invalid S3 bucket name")
    location = f"s3://{bucket}/gold/grocery_price_change/"
    run_id = get_current_context()["run_id"]
    athena = boto3.client("athena", region_name=REGION, config=CLIENT_CONFIG)
    database_id = execute_query(athena, f"CREATE DATABASE IF NOT EXISTS {DATABASE}", "create-database", run_id)
    sql = (f"CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{TABLE} "
           "(market STRING, item_name STRING, price_nov DOUBLE, price_dec DOUBLE, "
           "change_rate DECIMAL(18,2), change_rank INT) STORED AS PARQUET "
           f"LOCATION '{location}'")
    table_id = execute_query(athena, sql, "create-table", run_id)
    glue = boto3.client("glue", region_name=REGION, config=CLIENT_CONFIG)
    metadata = glue.get_table(DatabaseName=DATABASE, Name=TABLE)["Table"]["StorageDescriptor"]
    expected = [("market", "string"), ("item_name", "string"), ("price_nov", "double"),
                ("price_dec", "double"), ("change_rate", "decimal(18,2)"), ("change_rank", "int")]
    if metadata["Location"].rstrip("/") != location.rstrip("/") or [
            (column["Name"], column["Type"]) for column in metadata["Columns"]] != expected:
        raise ValueError("Existing Athena table location/schema does not match Q8 Gold")
    return {"database_query_id": database_id, "table_query_id": table_id}


def query_top1():
    athena = boto3.client("athena", region_name=REGION, config=CLIENT_CONFIG)
    # PythonOperator automatically stores this return value under XCom key return_value.
    return execute_query(athena, SELECT_SQL, "select-top1", get_current_context()["run_id"])


with DAG(
    dag_id=DAG_ID, start_date=pendulum.datetime(2026, 11, 24, tz="UTC"),
    schedule="*/10 * * * *", catchup=False, max_active_runs=1, max_active_tasks=1,
    is_paused_upon_creation=True, dagrun_timeout=timedelta(minutes=9),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["week9", "lhi", "prices"],
) as dag:
    batch = SparkSubmitOperator(
        task_id="spark_batch", conn_id="spark_default",
        application="/opt/de9/part2-price/price_batch.py", application_args=["--with-gold"],
        py_files="/opt/de9/part2-price/price_change.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        conf={"spark.driver.host": "airflow"}, driver_memory="768m", executor_memory="1g",
        executor_cores=1, total_executor_cores=1, execution_timeout=timedelta(minutes=6),
    )
    register = PythonOperator(task_id="register_athena", python_callable=register_gold,
                              execution_timeout=timedelta(minutes=4))
    select = PythonOperator(task_id="query_top1", python_callable=query_top1,
                            execution_timeout=timedelta(minutes=4))
    batch >> register >> select
