"""Q1 EC2 실행 환경 확인용. Q2 이후의 제공 데이터를 대신하지 않습니다."""
import os
import socket
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
    executors = spark.sparkContext.parallelize([0], 1).map(
        lambda _: (socket.gethostname(), tuple(sys.version_info[:2]))
    ).collect()
    print("Executor host/Python:", executors)
    assert all(host == "spark-worker" and version == (3, 11) for host, version in executors)
    bucket = os.environ["S3_BUCKET"]
    path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/")
    filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    entries = filesystem.listStatus(path)
    print("S3 access through EC2 role OK; root entries:", len(entries))
finally:
    spark.stop()
