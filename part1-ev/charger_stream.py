"""Q4: Kafka -> original Bronze / cleaned Silver and cumulative console counts."""
import argparse
import os

from pyspark.sql import SparkSession, functions as F, types as T


EVENT_SCHEMA = T.StructType([
    T.StructField("stationName", T.StringType()),
    T.StructField("portNumber", T.IntegerType()),
    T.StructField("portType", T.StringType()),
    T.StructField("plugType", T.StringType()),
    T.StructField("startTime", T.StringType()),
    T.StructField("endTime", T.StringType()),
    T.StructField("totalDuration", T.StringType()),
    T.StructField("chargingDuration", T.StringType()),
    T.StructField("energyKwh", T.DoubleType()),
    T.StructField("endedBy", T.StringType()),
])


def duration_seconds(name):
    """Duration hours may exceed 23; do not interpret them as a clock time."""
    parts = F.split(F.col(name), ":")
    valid = F.col(name).rlike(r"^\d+:[0-5]\d:[0-5]\d$")
    return F.when(valid, parts[0].cast("long") * 3600
                  + parts[1].cast("long") * 60 + parts[2].cast("long"))


def silver_events(events):
    return (events
            .withColumn("locationGroup", F.regexp_replace("stationName", r" #\d+$", ""))
            .withColumn("totalDuration", duration_seconds("totalDuration"))
            .withColumn("chargingDuration", duration_seconds("chargingDuration"))
            .withColumn("idleSec", F.col("totalDuration") - F.col("chargingDuration")))


def parquet_batch_writer(root, label):
    def write_batch(batch, batch_id):
        # A deterministic batch directory replaces this batch alone on retry.
        # Checkpoints must stay paired with their output directories.
        batch = batch.persist()
        try:
            if batch.isEmpty():
                return
            if batch.filter(F.col("stationName").isNull()
                            | F.to_date("startTime", "yyyy-MM-dd HH:mm:ss").isNull()).limit(1).count():
                raise ValueError("Invalid stationName/startTime; refusing silent data loss")
            if label == "SILVER" and batch.filter(
                    F.col("totalDuration").isNull() | F.col("chargingDuration").isNull()
                    | (F.col("idleSec") < 0)).limit(1).count():
                raise ValueError("Invalid duration or negative idleSec")
            dated = batch.withColumn("dt", F.to_date("startTime", "yyyy-MM-dd HH:mm:ss"))
            dates = [row.dt.isoformat() for row in dated.select("dt").distinct().collect()]
            for day in dates:
                path = f"{root}/dt={day}/batch_id={batch_id:08d}"
                rows = dated.filter(F.col("dt") == day).drop("dt")
                count = rows.count()
                rows.coalesce(1).write.mode("overwrite").parquet(path)
                print(f"{label} batch={batch_id} rows={count} path={path}", flush=True)
        finally:
            batch.unpersist()
    return write_batch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--drain", action="store_true",
                        help="Process all available records, then stop all three streams")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=60)
    args = parser.parse_args()
    if args.max_offsets_per_trigger < 1:
        parser.error("max-offsets-per-trigger must be positive")
    bucket = os.environ["S3_BUCKET"]
    base = f"s3a://{bucket}"
    checkpoint = f"{base}/checkpoints/q4"
    spark = SparkSession.builder.appName("de9-lhi-q4-charger-stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    queries = []
    try:
        kafka = (spark.readStream.format("kafka")
                 .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
                 .option("subscribe", "ev-charging-sessions")
                 .option("startingOffsets", "earliest")
                 .option("failOnDataLoss", "true")
                 .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger).load())
        events = kafka.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA,
                                         {"mode": "FAILFAST"}).alias("event")).select("event.*")
        silver = silver_events(events)
        # Independent checkpoints; do not set kafka.group.id to the Q3 consumer group.
        for label, frame in [("bronze", events), ("silver", silver)]:
            query = (frame.writeStream.queryName(f"q4_{label}")
                     .foreachBatch(parquet_batch_writer(
                         f"{base}/{label}/ev-charging-sessions", label.upper()))
                     .option("checkpointLocation", f"{checkpoint}/{label}")
                     .trigger(processingTime="20 seconds").start())
            queries.append(query)
        counts = silver.groupBy("locationGroup").count().orderBy("locationGroup")
        console = (counts.writeStream.queryName("q4_location_counts")
                   .format("console").outputMode("complete")
                   .option("truncate", "false").option("numRows", 1000)
                   .option("checkpointLocation", f"{checkpoint}/counts")
                   .trigger(processingTime="20 seconds").start())
        queries.append(console)
        print("Q4 STREAMING: explicit JSON schema; 20-second triggers; max offsets="
              f"{args.max_offsets_per_trigger}; independent Spark checkpoints", flush=True)
        if args.drain:
            for query in queries:
                query.processAllAvailable()
                print(f"DRAINED {query.name}: {query.lastProgress}", flush=True)
            print("Q4 all available input processed; stopping streams", flush=True)
        else:
            spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
