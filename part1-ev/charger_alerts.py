"""Q5: stream Q4 Silver parquet -> real Slack alerts and cumulative S3 Gold."""
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T


SILVER_SCHEMA = T.StructType([
    T.StructField("stationName", T.StringType()),
    T.StructField("portNumber", T.IntegerType()),
    T.StructField("portType", T.StringType()),
    T.StructField("plugType", T.StringType()),
    T.StructField("startTime", T.StringType()),
    T.StructField("endTime", T.StringType()),
    T.StructField("totalDuration", T.LongType()),
    T.StructField("chargingDuration", T.LongType()),
    T.StructField("energyKwh", T.DoubleType()),
    T.StructField("endedBy", T.StringType()),
    T.StructField("locationGroup", T.StringType()),
    T.StructField("idleSec", T.LongType()),
])
ALERT_FIELDS = ["stationName", "locationGroup", "startTime", "idleSec"]


def post_slack(url, text):
    payload = json.dumps({"text": text}, ensure_ascii=False).encode("utf-8")
    for attempt in range(3):
        request = urllib.request.Request(url, data=payload,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                if response.status != 200 or response.read().strip() != b"ok":
                    raise RuntimeError("Slack response was not an acknowledged success")
            return
        except urllib.error.HTTPError as error:
            if error.code == 429 and attempt < 2:
                time.sleep(max(1, min(int(error.headers.get("Retry-After", "2")), 60)))
                continue
            raise RuntimeError(f"Slack HTTP {error.code}; inspect channel before retrying") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            # An ambiguous timeout may have been accepted by Slack: no blind retry.
            raise RuntimeError("Slack delivery status uncertain; inspect channel before retrying") from None


def main():
    # Read the credential only on the driver, never as a CLI argument or printed value.
    webhook = Path("/tmp/de9-slack-webhook").read_text().strip()
    if not webhook.startswith("https://hooks.slack.com/services/"):
        raise ValueError("Expected a Slack Incoming Webhook")
    base = f"s3a://{os.environ['S3_BUCKET']}"
    checkpoint = f"{base}/checkpoints/q5"
    spark = SparkSession.builder.appName("de9-lhi-q5-idle-alerts").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    queries = []
    try:
        events = (spark.readStream.schema(SILVER_SCHEMA)
                  .option("recursiveFileLookup", "true")
                  .option("pathGlobFilter", "*.parquet")
                  .option("maxFilesPerTrigger", 1)
                  .parquet(f"{base}/silver/ev-charging-sessions"))
        anomalies = events.filter(F.col("idleSec") > 5400)

        def notify(batch, batch_id):
            path = spark._jvm.org.apache.hadoop.fs.Path(
                f"{checkpoint}/alerts-sent/batch-{batch_id:08d}.json")
            filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
            if filesystem.exists(path):
                print(f"SLACK batch={batch_id}: previously acknowledged, skipped", flush=True)
                return
            rows = [r.asDict() for r in batch.select(*ALERT_FIELDS)
                    .orderBy("startTime", "stationName", "idleSec").collect()]
            if not rows:
                return
            text = [f"*Q5 충전 포트 점유 이상 감지* | batch={batch_id} | {len(rows)}건",
                    "제공된 2017년 충전 데이터 처리 결과 · 기준: idleSec > 5400초"]
            for row in rows:
                text.append("\n" + "\n".join(f"{key}: {row[key]}" for key in ALERT_FIELDS))
            post_slack(webhook, "\n".join(text))
            # Receipt is recorded after Slack ACK. It is not an atomic transaction
            # with Slack; a crash between ACK and marker can still cause duplicates.
            filesystem.mkdirs(path.getParent())
            output = filesystem.create(path, False)
            try:
                receipt = {"batch_id": batch_id, "slack_ack": "ok", "events": rows}
                output.write(bytearray(json.dumps(receipt, ensure_ascii=False).encode("utf-8")))
            finally:
                output.close()
            print(f"SLACK batch={batch_id}: {len(rows)} events acknowledged; receipt saved", flush=True)
            time.sleep(1.1)

        def write_gold(batch, batch_id):
            batch = batch.persist()
            try:
                if batch.isEmpty():
                    return
                batch.coalesce(1).write.mode("overwrite").parquet(f"{base}/gold/ev_charging_agg")
                print(f"GOLD cumulative batch={batch_id}", flush=True)
                batch.orderBy("location_group").show(1000, truncate=False)
            finally:
                batch.unpersist()

        alerts = (anomalies.writeStream.queryName("q5_slack_alerts")
                  .foreachBatch(notify)
                  .option("checkpointLocation", f"{checkpoint}/slack")
                  .trigger(availableNow=True).start())
        queries.append(alerts)
        counts = (anomalies.groupBy("locationGroup").count()
                  .select(F.col("locationGroup").alias("location_group"),
                          F.col("count").alias("anomaly_count")))
        gold = (counts.writeStream.queryName("q5_gold_counts")
                .outputMode("complete").foreachBatch(write_gold)
                .option("checkpointLocation", f"{checkpoint}/gold")
                .trigger(availableNow=True).start())
        queries.append(gold)
        for query in queries:
            query.awaitTermination()
            print(f"COMPLETED {query.name}", flush=True)
        print("Q5 available Silver files processed; streams finished", flush=True)
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
