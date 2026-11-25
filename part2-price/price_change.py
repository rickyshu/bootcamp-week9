"""Q8: Silver의 전월 가격을 lag 윈도우로 참조하여 도시별 변동 순위를 저장합니다."""
import os

from pyspark.sql import SparkSession, Window, functions as F


GOLD_COLUMNS = ["market", "item_name", "price_nov", "price_dec", "change_rate", "change_rank"]


def target_months(silver):
    return silver.filter((F.col("year") == 2016) & F.col("month").isin(11, 12))


def validate_pairs(silver):
    selected = target_months(silver)
    if selected.isEmpty():
        raise ValueError("No 2016-11/12 Silver input")
    invalid = selected.filter(
        F.col("market").isNull() | F.col("item_name").isNull() | F.col("price").isNull()
        | F.isnan("price") | (F.col("price") <= 0) | (F.col("price") >= F.lit(float("inf"))))
    if invalid.limit(1).count():
        raise ValueError("Invalid market/item/price in Silver input")
    pairs = selected.groupBy("market", "item_name").agg(
        F.count("month").alias("rows"), F.countDistinct("month").alias("months"))
    if pairs.filter((F.col("rows") != 2) | (F.col("months") != 2)).limit(1).count():
        raise ValueError("Each market/item needs exactly one November and one December row")


def change_ranks(silver):
    history = Window.partitionBy("market", "item_name").orderBy("year", "month")
    # 금액을 십진수로 계산해 이진 실수의 표현 오차가 반올림에 영향을 주지 않게 합니다.
    months = target_months(silver).withColumn("amount", F.col("price").cast("decimal(18,6)"))
    previous = months.withColumn("price_nov", F.lag("amount").over(history))
    # lag를 계산한 뒤 12월만 선택합니다. 두 달을 나눠 조인하지 않습니다.
    changes = (previous.filter(F.col("month") == 12)
               .withColumn("price_dec", F.col("amount"))
               .withColumn("change_rate", F.round(
                   (F.col("price_dec") - F.col("price_nov")) / F.col("price_nov") * 100, 2)
                   .cast("decimal(18,2)")))
    ranking = Window.partitionBy("market").orderBy(F.abs("change_rate").desc())
    return (changes.withColumn("change_rank", F.rank().over(ranking))
            .withColumn("price_nov", F.col("price_nov").cast("double"))
            .withColumn("price_dec", F.col("price_dec").cast("double"))
            .select(*GOLD_COLUMNS))


def write_gold(spark, base):
    """기존 Spark 세션으로 Silver를 읽어 Gold를 저장하고 1위를 출력합니다."""
    silver = gold = None
    try:
        silver = spark.read.parquet(f"{base}/silver/product-prices/").persist()
        validate_pairs(silver)
        gold = change_ranks(silver).persist()
        top1 = gold.filter(F.col("change_rank") == 1).orderBy("market", "item_name")
        city_count = gold.select("market").distinct().count()
        top_count = top1.count()
        if top_count != city_count:
            raise ValueError("Top rank is tied; inspect input before producing one-row-per-city evidence")
        path = f"{base}/gold/grocery_price_change/"
        count = gold.count()
        gold.coalesce(1).write.mode("overwrite").parquet(path)
        print(f"GOLD rows={count}; cities={city_count}; columns={','.join(GOLD_COLUMNS)}; path={path}", flush=True)
        print("Q8: lag by market/item, ROUND HALF_UP to 2 decimals, rank by absolute change_rate", flush=True)
        print(f"Q8 city top1: {top_count} rows ordered by market", flush=True)
        top1.show(top_count, truncate=False)
        print("Q8 SUCCESS: all city/item rows saved; Spark stopping", flush=True)
    finally:
        if gold is not None:
            gold.unpersist()
        if silver is not None:
            silver.unpersist()


def main():
    base = f"s3a://{os.environ['S3_BUCKET']}"
    spark = SparkSession.builder.appName("de9-lhi-q8-price-change").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        write_gold(spark, base)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
