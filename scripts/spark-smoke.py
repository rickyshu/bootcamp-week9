"""Q1 EC2 실행 환경 확인용. Q2 이후의 제공 데이터를 대신하지 않습니다."""
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("de9-lhi-q1-smoke").getOrCreate()
try:
    print("Python:", sys.version)
    print("Spark:", spark.version)
    print("Master:", spark.sparkContext.master)
    count = spark.range(10).repartition(2).count()
    print("Distributed count:", count)
    assert count == 10
finally:
    spark.stop()
