import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import oracledb
from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_SERVICE_NAME, REDIS_HOST, REDIS_PORT, KAFKA_BOOTSTRAP_SERVERS
import redis
import json

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = SparkSession \
    .builder \
    .appName("FraudDetectionProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .config("spark.driver.extraJavaOptions", "-Dcom.sun.jndi.ldap.connect.pool=false") \
    .config("spark.executor.extraJavaOptions", "-Dcom.sun.jndi.ldap.connect.pool=false") \
    .getOrCreate()

# ---------------------------------------------------------------------------
# Kafka Source
# ---------------------------------------------------------------------------
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", "transactions") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "latest") \
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

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# ---------------------------------------------------------------------------
# Redis UDF – enrich with risk_segment
# ---------------------------------------------------------------------------
@udf(returnType=StringType())
def get_risk_from_redis(account_id):
    r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), db=0, decode_responses=True)
    user_data = r.get(account_id)
    if user_data:
        return json.loads(user_data).get("risk_segment", "Unknown")
    return "Unknown"

enriched_df = parsed_df.withColumn("risk_segment", get_risk_from_redis(col("idOrig")))

final_df = enriched_df.withColumn(
    "is_fraud",
    when(
        (col("amount") == col("oldbalanceOrg")) |
        ((col("risk_segment") == "High") & (col("amount") > 10000)),
        True
    ).otherwise(False)
)

# ---------------------------------------------------------------------------
# Static fraud-reason catalogue  (fraud_reason_id → seeded once per batch)
#   1 – No Fraud                          | Low
#   2 – Full Balance Drain                | High
#   3 – High-Risk Account Large Transfer  | High
# ---------------------------------------------------------------------------
FRAUD_REASONS = {
    1: ("No Fraud",                         "Low"),
    2: ("Full Balance Drain",               "High"),
    3: ("High Risk Account Large Transfer", "High"),
}

# ---------------------------------------------------------------------------
# Helper: Oracle connection
# ---------------------------------------------------------------------------
def get_db_connection():
    return oracledb.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        dsn=f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}",
    )

# ---------------------------------------------------------------------------
# DIM helpers (all use MERGE to be idempotent / safe on retries)
# ---------------------------------------------------------------------------
def seed_dim_fraud_reason(cursor):
    """Ensure all fraud-reason rows exist in dim_fraud_reason."""
    for fid, (reason_name, risk_level) in FRAUD_REASONS.items():
        cursor.execute("""
            MERGE INTO dim_fraud_reason d
            USING (SELECT :1 AS fraud_reason_id FROM DUAL) s
            ON (d.fraud_reason_id = s.fraud_reason_id)
            WHEN NOT MATCHED THEN
                INSERT (fraud_reason_id, reason_name, risk_level)
                VALUES (:2, :3, :4)
        """, [fid, fid, reason_name, risk_level])


def upsert_dim_account(cursor, account_id_num, age, home_city, risk_segment):
    """Insert account into dim_accounts if it does not already exist."""
    cursor.execute("""
        MERGE INTO dim_accounts d
        USING (SELECT :1 AS account_id FROM DUAL) s
        ON (d.account_id = s.account_id)
        WHEN NOT MATCHED THEN
            INSERT (account_id, age, home_city, risk_segment)
            VALUES (:2, :3, :4, :5)
    """, [account_id_num, account_id_num, age, home_city, risk_segment])


def upsert_dim_time(cursor, hour, day, month, year, day_of_week_str):
    """Insert (hour, day, month, year) into dim_time if not exists; return time_id."""
    cursor.execute("""
        MERGE INTO dim_time t
        USING (SELECT :1 AS hour, :2 AS day, :3 AS month, :4 AS year FROM DUAL) s
        ON (t.hour = s.hour AND t.day = s.day AND t.month = s.month AND t.year = s.year)
        WHEN NOT MATCHED THEN
            INSERT (time_id, hour, day, month, year, day_of_week)
            VALUES (seq_time.NEXTVAL, :5, :6, :7, :8, :9)
    """, [hour, day, month, year, hour, day, month, year, day_of_week_str])

    cursor.execute("""
        SELECT time_id FROM dim_time
        WHERE hour = :1 AND day = :2 AND month = :3 AND year = :4
    """, [hour, day, month, year])
    row = cursor.fetchone()
    return row[0] if row else None


def resolve_fraud_reason_id(row):
    """Map fraud type to a pre-defined fraud_reason_id."""
    if not row.is_fraud:
        return 1  # No Fraud
    if row.amount == row.oldbalanceOrg:
        return 2  # Full Balance Drain
    return 3      # High Risk Account Large Transfer

# ---------------------------------------------------------------------------
# foreachBatch writer
# ---------------------------------------------------------------------------
def write_to_oracle(batch_df, batch_id):
    """Write one micro-batch to the Oracle Star-Schema DWH."""
    if batch_df.count() == 0:
        return

    try:
        conn   = get_db_connection()
        cursor = conn.cursor()

        # Seed static dimension once per batch (cheap MERGE, no-op if rows exist)
        seed_dim_fraud_reason(cursor)

        redis_client = redis.Redis(
            host=REDIS_HOST, port=int(REDIS_PORT), db=0, decode_responses=True
        )

        rows     = batch_df.collect()
        inserted = 0

        for row in rows:
            # ── dim_accounts ────────────────────────────────────────────────
            try:
                account_id_num = int(row.idOrig.split("_")[1]) if "_" in row.idOrig else 0
            except (ValueError, IndexError):
                account_id_num = 0

            try:
                raw = redis_client.get(row.idOrig)
                if raw:
                    info      = json.loads(raw)
                    age       = info.get("age",          0)
                    home_city = info.get("home_city",    "Unknown")
                    risk_seg  = info.get("risk_segment", "Unknown")
                else:
                    age, home_city, risk_seg = 0, "Unknown", "Unknown"
            except Exception:
                age, home_city, risk_seg = 0, "Unknown", "Unknown"

            # ── dim_time ────────────────────────────────────────────────────
            try:
                ts          = datetime.fromisoformat(row.timestamp.replace("Z", "+00:00"))
                hour        = ts.hour
                day         = ts.day
                month       = ts.month
                year        = ts.year
                day_of_week = ts.strftime("%A")   # e.g. "Monday"
            except Exception:
                hour, day, month, year, day_of_week = 0, 1, 1, 2024, "Monday"

            # ── fact_transactions ────────────────────────────────────────────
            try:
                txn_id = (
                    int(row.transaction_id)
                    if row.transaction_id and row.transaction_id.isdigit()
                    else 0
                )
            except Exception:
                txn_id = 0

            fraud_reason_id = resolve_fraud_reason_id(row)
            fraud_flag      = 1 if row.is_fraud else 0

            try:
                upsert_dim_account(cursor, account_id_num, age, home_city, risk_seg)

                time_id = upsert_dim_time(cursor, hour, day, month, year, day_of_week)

                cursor.execute("""
                    MERGE INTO fact_transactions f
                    USING (SELECT :1 AS transaction_id FROM DUAL) s
                    ON (f.transaction_id = s.transaction_id)
                    WHEN NOT MATCHED THEN
                        INSERT (transaction_id, account_id, time_id,
                                fraud_reason_id, amount, is_fraud)
                        VALUES (:2, :3, :4, :5, :6, :7)
                """, [
                    txn_id,
                    txn_id, account_id_num, time_id,
                    fraud_reason_id, float(row.amount), fraud_flag,
                ])
                inserted += 1

            except Exception as e:
                print(f"[WARN] Skipped txn={txn_id}: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        print(f"[Batch {batch_id}] Inserted {inserted}/{len(rows)} rows into Oracle DWH")

    except Exception as e:
        print(f"[ERROR] Oracle batch write failed: {e}")


# ---------------------------------------------------------------------------
# Streaming query
# ---------------------------------------------------------------------------
checkpoint_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".checkpoint")
os.makedirs(checkpoint_dir, exist_ok=True)

query = final_df \
    .writeStream \
    .foreachBatch(write_to_oracle) \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

query.awaitTermination()
