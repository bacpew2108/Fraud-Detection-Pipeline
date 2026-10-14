import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import signal
import time

import pyspark
from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, struct, to_json
from pyspark.sql.functions import sum as sum_
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType, StringType, StructField,
    StructType, TimestampNTZType,
)

import config
from streaming_app import fraud_rules, oracle_sink, redis_store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spark_processor")

config.require("DB_USER", "DB_PASSWORD")
TZ = config.APP_TIMEZONE


def on_sigterm(signum, frame):
    raise KeyboardInterrupt


signal.signal(signal.SIGTERM, on_sigterm)   # kill / docker stop: dừng gọn như Ctrl+C

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession \
    .builder \
    .appName("FraudDetectionProcessor") \
    .master(config.SPARK_MASTER) \
    .config("spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}") \
    .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.ui.retainedJobs", "100") \
    .config("spark.ui.retainedStages", "100") \
    .config("spark.sql.ui.retainedExecutions", "50") \
    .config("spark.executorEnv.PYTHONPATH", str(config.PROJECT_ROOT)) \
    .config("spark.driver.extraJavaOptions", "-Dcom.sun.jndi.ldap.connect.pool=false") \
    .config("spark.executor.extraJavaOptions", "-Dcom.sun.jndi.ldap.connect.pool=false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Kafka Source
# ---------------------------------------------------------------------------
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", config.KAFKA_TOPIC_TRANSACTIONS) \
    .option("startingOffsets", config.STARTING_OFFSETS) \
    .option("maxOffsetsPerTrigger", config.MAX_OFFSETS_PER_TRIGGER) \
    .option("failOnDataLoss", "false") \
    .load()

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp",      StringType(), True),
    StructField("type",           StringType(), True),
    StructField("amount",         DoubleType(), True),
    StructField("idOrig",         StringType(), True),
    StructField("oldbalanceOrg",  DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("idDest",         StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
])

# Message không phải JSON hợp lệ hoặc thiếu trường bắt buộc sẽ bị loại ở đây
parsed_df = df.selectExpr("CAST(value AS STRING) AS value", "timestamp AS kafka_ts") \
    .select(from_json(col("value"), schema).alias("data"), col("kafka_ts")) \
    .select("data.*", "kafka_ts") \
    .filter(
        col("transaction_id").isNotNull()
        & col("idOrig").isNotNull()
        & col("amount").isNotNull()
        & col("timestamp").isNotNull()
    )

# ---------------------------------------------------------------------------
# Enrichment + scoring (chạy trên executor, mỗi partition một lần gọi Redis)
# ---------------------------------------------------------------------------
SCORED_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),           False),
    StructField("event_time",       StringType(),           False),  # chuỗi gốc, dùng cho alert
    StructField("event_ts",         TimestampNTZType(),     False),  # giờ địa phương APP_TIMEZONE
    StructField("ingested_at",      TimestampNTZType(),     True),   # thời điểm vào Kafka
    StructField("transaction_type", StringType(),           True),
    StructField("amount",           DoubleType(),           False),
    StructField("account_id",       StringType(),           False),
    StructField("dest_account_id",  StringType(),           True),
    StructField("age",              IntegerType(),          True),
    StructField("home_city",        StringType(),           True),
    StructField("risk_segment",     StringType(),           False),
    StructField("time_id",          LongType(),             False),
    StructField("hour",             IntegerType(),          False),
    StructField("day",              IntegerType(),          False),
    StructField("month",            IntegerType(),          False),
    StructField("year",             IntegerType(),          False),
    StructField("day_of_week",      StringType(),           False),
    StructField("velocity_count",   IntegerType(),          False),
    StructField("fraud_score",      DoubleType(),           False),
    StructField("is_fraud",         IntegerType(),          False),
    StructField("fraud_reason_id",  IntegerType(),          False),
    StructField("fraud_reason",     StringType(),           False),
    StructField("triggered_rules",  ArrayType(StringType()), False),
])


def score_partition(rows):
    rows = list(rows)
    if not rows:
        return

    client = redis_store.get_client()
    profiles = redis_store.get_profiles(client, [row.idOrig for row in rows])

    parsed = []
    for row in rows:
        event_dt = fraud_rules.parse_event_time(row.timestamp, TZ)
        if event_dt is None:
            logging.warning("Bỏ qua giao dịch %s: timestamp không hợp lệ %r",
                            row.transaction_id, row.timestamp)
            continue
        parsed.append((row, event_dt))

    velocity = redis_store.velocity_counts(
        client,
        [
            (row.transaction_id, row.idOrig, event_dt.timestamp())
            for row, event_dt in parsed
            if fraud_rules.counts_for_velocity(row.amount)
        ],
        fraud_rules.VELOCITY_WINDOW_SECONDS,
    )

    for row, event_dt in parsed:
        profile = profiles.get(row.idOrig, {})
        risk_segment = profile.get("risk_segment", "Unknown")
        velocity_count = velocity.get(row.transaction_id, 0)
        verdict = fraud_rules.evaluate(
            amount=row.amount,
            old_balance=row.oldbalanceOrg,
            risk_segment=risk_segment,
            local_hour=event_dt.hour,
            velocity_count=velocity_count,
        )
        # kafka_ts là datetime naive theo giờ hệ thống -> quy đổi sang APP_TIMEZONE
        ingested_at = (
            row.kafka_ts.astimezone(TZ).replace(tzinfo=None) if row.kafka_ts else None
        )
        yield (
            row.transaction_id,
            row.timestamp,
            event_dt.replace(tzinfo=None),
            ingested_at,
            row.type,
            float(row.amount),
            row.idOrig,
            row.idDest,
            profile.get("age"),
            profile.get("home_city", "Unknown"),
            risk_segment,
            fraud_rules.time_id_for(event_dt),
            event_dt.hour,
            event_dt.day,
            event_dt.month,
            event_dt.year,
            event_dt.strftime("%A"),
            velocity_count,
            verdict.fraud_score,
            1 if verdict.is_fraud else 0,
            verdict.fraud_reason_id,
            fraud_rules.FRAUD_REASONS[verdict.fraud_reason_id][0],
            [fraud_rules.FRAUD_REASONS[r][0] for r in verdict.triggered],
        )


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------
def publish_fraud_alerts(fraud_df):
    """Đẩy giao dịch gian lận sang topic fraud_alerts để hệ thống khác gửi thông báo."""
    fraud_df.select(
        col("account_id").alias("key"),
        to_json(struct(
            "transaction_id", "event_time", "account_id", "dest_account_id",
            "transaction_type", "amount", "risk_segment", "velocity_count",
            "fraud_score", "fraud_reason", "triggered_rules",
        )).alias("value"),
    ).write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", config.KAFKA_TOPIC_FRAUD_ALERTS) \
        .save()


def process_batch(batch_df, batch_id):
    """foreachBatch: score -> alert -> (dim + fact).

    Không bắt exception ở đây: nếu một bước lỗi, query dừng và batch chưa được
    ghi vào checkpoint, lần chạy lại sẽ xử lý lại đúng batch đó (at-least-once).
    Các sink đều idempotent (MERGE theo khóa), riêng fraud_alerts có thể bị trùng
    khi retry nên phía consumer nên khử trùng theo transaction_id.
    """
    started = time.monotonic()
    # persist để Redis chỉ bị gọi một lần cho mỗi giao dịch dù có nhiều action phía sau
    scored = spark.createDataFrame(
        batch_df.rdd.mapPartitions(score_partition), SCORED_SCHEMA
    ).persist()
    try:
        stats = scored.agg(count("*").alias("total"), sum_("is_fraud").alias("fraud")).first()
        if stats.total == 0:
            return
        if stats.fraud:
            publish_fraud_alerts(scored.filter(col("is_fraud") == 1))
        scored.foreachPartition(oracle_sink.write_partition)

        log.info("[Batch %s] %d giao dịch, %d gian lận, xử lý %.2fs",
                 batch_id, stats.total, stats.fraud, time.monotonic() - started)
    finally:
        scored.unpersist()


# ---------------------------------------------------------------------------
# Streaming query
# ---------------------------------------------------------------------------
# Kiểm tra kết nối trước khi start để báo lỗi rõ ràng thay vì fail giữa chừng
redis_store.get_client().ping()
oracle_sink.check_connection()
oracle_sink.seed_fraud_reasons(fraud_rules.FRAUD_REASONS)

os.makedirs(config.CHECKPOINT_DIR, exist_ok=True)

# Query lỗi (vd Oracle tạm mất kết nối) sẽ được khởi động lại từ checkpoint,
# tối đa MAX_RESTARTS lần liên tiếp với backoff tăng dần rồi mới thoát hẳn.
# Spark 4.1.1 còn có lỗi NPE trong KafkaMicroBatchStream.metrics ở batch đầu
# tiên sau khi khôi phục một batch dở dang; lần khởi động lại kế tiếp chạy bình thường.
MAX_RESTARTS = 5
STABLE_RUN_SECONDS = 300   # chạy ổn định lâu hơn mức này thì reset bộ đếm

restarts = 0
while True:
    writer = parsed_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", config.CHECKPOINT_DIR)
    if config.TRIGGER_INTERVAL:
        writer = writer.trigger(processingTime=config.TRIGGER_INTERVAL)
    query = writer.start()
    log.info("Đang lắng nghe topic '%s' (checkpoint: %s)",
             config.KAFKA_TOPIC_TRANSACTIONS, config.CHECKPOINT_DIR)
    started_at = time.monotonic()
    try:
        query.awaitTermination()
        break
    except KeyboardInterrupt:
        log.info("Đang dừng streaming query...")
        query.stop()
        break
    except StreamingQueryException as exc:
        if time.monotonic() - started_at > STABLE_RUN_SECONDS:
            restarts = 0
        restarts += 1
        if restarts > MAX_RESTARTS:
            raise
        delay = min(10 * 2 ** (restarts - 1), 120)
        log.error("Query dừng do lỗi: %s", str(exc).splitlines()[0][:300])
        log.warning("Khởi động lại sau %ds (lần %d/%d)", delay, restarts, MAX_RESTARTS)
        time.sleep(delay)
