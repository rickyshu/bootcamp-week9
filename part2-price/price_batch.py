"""Q7: Kafka의 현재 데이터를 한 번 읽어 S3 Bronze/Silver 스냅샷을 만듭니다."""
import argparse
import os

from pyspark.sql import SparkSession, functions as F, types as T


FIELDS = ["item_name", "market", "year", "month", "price", "currency", "unit"]
EVENT_SCHEMA = T.StructType([
    T.StructField("item_name", T.StringType()),
    T.StructField("market", T.StringType()),
    T.StructField("year", T.IntegerType()),
    T.StructField("month", T.IntegerType()),
    # 문자열 가격과 JSON 숫자를 모두 수용한 뒤 실수로 변환합니다.
    T.StructField("price", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("unit", T.StringType()),
])


def silver_events(raw):
    parsed = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event"))
    parsed = parsed.select("event.*").withColumn("price", F.col("price").cast("double"))
    return parsed.filter(
        F.col("price").isNotNull() & ~F.isnan("price")
        & (F.col("price") > 0) & (F.col("price") < F.lit(float("inf"))))


def price_summary(silver):
    return (silver.groupBy("item_name", "month")
            .agg(F.avg("price").alias("avg_price"), F.max("price").alias("max_price"),
                 F.min("price").alias("min_price"))
            .orderBy("item_name", "month"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--with-gold", action="store_true", help="Q9: also run Q8 Gold after Silver completes")
    args = parser.parse_args()
    base = f"s3a://{os.environ['S3_BUCKET']}"
    spark = SparkSession.builder.appName("de9-lhi-q7-price-batch").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    raw = silver = None
    try:
        # readStream이 아닌 유한한 batch read. 현재 끝 오프셋까지 처리한 뒤 종료합니다.
        raw = (spark.read.format("kafka")
               .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
               .option("subscribe", "product-prices")
               .option("startingOffsets", "earliest").option("endingOffsets", "latest")
               .option("failOnDataLoss", "true").load()
               .select("topic", "partition", "offset", "timestamp", "key", "value").persist())
        source_count = raw.count()
        if source_count == 0:
            raise RuntimeError("product-prices is empty; refusing to replace output with empty data")
        # 원본 key/value 바이트와 Kafka 식별자를 가공 없이 보존합니다.
        bronze_path = f"{base}/bronze/product-prices/"
        raw.coalesce(1).write.mode("overwrite").parquet(bronze_path)
        print(f"BRONZE rows={source_count} path={bronze_path}; original Kafka key/value bytes preserved", flush=True)
        silver = silver_events(raw).persist()
        valid_count = silver.count()
        silver_path = f"{base}/silver/product-prices/"
        silver.coalesce(1).write.mode("overwrite").parquet(silver_path)
        print(f"SILVER rows={valid_count} rejected={source_count-valid_count} path={silver_path}; price=double", flush=True)
        print("Q7 item_name/month price summary (all groups):", flush=True)
        summary = price_summary(silver)
        summary.show(summary.count(), truncate=False)
        if args.with_gold:
            from price_change import write_gold
            write_gold(spark, base)
        print("Q7 SUCCESS: finite batch completed; Spark stopping", flush=True)
    finally:
        if silver is not None:
            silver.unpersist()
        if raw is not None:
            raw.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
