"""Q1: Airflow 이미지의 실제 SparkSubmitOperator 제출 기능 확인.

DAG의 스케줄 실행 증빙이 아니며 Q9를 대신하지 않습니다.
"""
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

operator = SparkSubmitOperator(
    task_id="q1_environment_smoke",
    conn_id="spark_default",
    application="/opt/de9/scripts/spark-smoke.py",
    conf={"spark.driver.host": "airflow"},
)
operator.execute(context={})
