"""Ghi kết quả vào Oracle Data Warehouse (star schema).

Mỗi partition ghi dim + fact của nó bằng executemany trong một transaction.
Mọi câu lệnh đều idempotent (MERGE / bỏ qua khóa trùng) nên ghi lại cùng một
batch khi Spark retry là an toàn.
"""
from datetime import datetime

import oracledb

import config

_pool = None


def get_pool():
    """Connection pool dùng chung trong một process (driver hoặc Python worker)."""
    global _pool
    if _pool is None:
        _pool = oracledb.create_pool(
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            dsn=config.DB_DSN,
            min=1,
            max=4,
            increment=1,
        )
    return _pool


def check_connection():
    with get_pool().acquire() as conn:
        conn.ping()


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
MERGE_FRAUD_REASON_SQL = """
    MERGE INTO dim_fraud_reason d
    USING (SELECT :fraud_reason_id AS fraud_reason_id,
                  :reason_name     AS reason_name,
                  :risk_level      AS risk_level
           FROM dual) s
    ON (d.fraud_reason_id = s.fraud_reason_id)
    WHEN MATCHED THEN
        UPDATE SET d.reason_name = s.reason_name, d.risk_level = s.risk_level
    WHEN NOT MATCHED THEN
        INSERT (fraud_reason_id, reason_name, risk_level)
        VALUES (s.fraud_reason_id, s.reason_name, s.risk_level)
"""

# SCD Type 1: chỉ UPDATE khi profile thực sự thay đổi (DECODE so sánh an toàn với NULL)
MERGE_ACCOUNT_SQL = """
    MERGE INTO dim_accounts d
    USING (SELECT :account_id   AS account_id,
                  :age          AS age,
                  :home_city    AS home_city,
                  :risk_segment AS risk_segment
           FROM dual) s
    ON (d.account_id = s.account_id)
    WHEN MATCHED THEN
        UPDATE SET d.age = s.age, d.home_city = s.home_city, d.risk_segment = s.risk_segment
        WHERE DECODE(d.age, s.age, 0, 1) = 1
           OR DECODE(d.home_city, s.home_city, 0, 1) = 1
           OR DECODE(d.risk_segment, s.risk_segment, 0, 1) = 1
    WHEN NOT MATCHED THEN
        INSERT (account_id, age, home_city, risk_segment)
        VALUES (s.account_id, s.age, s.home_city, s.risk_segment)
"""

# Nhiều partition có thể cùng chèn một time_id mới (vd đầu mỗi giờ). Hint
# IGNORE_ROW_ON_DUPKEY_INDEX bỏ qua dòng trùng khóa thay vì báo ORA-00001
# như MERGE khi hai session chạy song song.
INSERT_TIME_SQL = """
    INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(dim_time(time_id)) */
    INTO dim_time (time_id, hour, day, month, year, day_of_week)
    VALUES (:time_id, :hour_of_day, :day_of_month, :month_of_year, :year_num, :day_of_week)
"""

MERGE_FACT_SQL = """
    MERGE INTO fact_transactions f
    USING (SELECT :transaction_id AS transaction_id FROM dual) s
    ON (f.transaction_id = s.transaction_id)
    WHEN NOT MATCHED THEN
        INSERT (transaction_id, account_id, dest_account_id, time_id, fraud_reason_id,
                transaction_type, amount, fraud_score, is_fraud,
                event_ts, ingested_at, processed_at)
        VALUES (s.transaction_id, :account_id, :dest_account_id, :time_id, :fraud_reason_id,
                :transaction_type, :amount, :fraud_score, :is_fraud,
                :event_ts, :ingested_at, :processed_at)
"""


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------
def seed_fraud_reasons(reasons):
    """Đồng bộ danh mục lý do gian lận (fraud_rules.FRAUD_REASONS) vào dim_fraud_reason."""
    params = [
        {"fraud_reason_id": reason_id, "reason_name": name, "risk_level": level}
        for reason_id, (name, level, _weight) in reasons.items()
    ]
    with get_pool().acquire() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(MERGE_FRAUD_REASON_SQL, params)
        conn.commit()


def write_partition(rows):
    """Ghi dim_accounts, dim_time, fact_transactions cho một partition.

    Chạy trên executor qua foreachPartition, cả ba bảng nằm trong cùng một
    transaction. Kafka key là account_id nên mỗi tài khoản chỉ nằm ở một
    partition, các session không tranh nhau MERGE cùng một dòng dim_accounts.

    Lỗi không bị nuốt: exception làm task fail để Spark retry, batch chỉ được
    đánh dấu hoàn tất trong checkpoint khi mọi partition ghi thành công.
    """
    rows = list(rows)
    if not rows:
        return

    accounts = {
        row.account_id: {
            "account_id": row.account_id,
            "age": row.age,
            "home_city": row.home_city,
            "risk_segment": row.risk_segment,
        }
        for row in rows
    }
    times = {
        row.time_id: {
            "time_id": row.time_id,
            "hour_of_day": row.hour,
            "day_of_month": row.day,
            "month_of_year": row.month,
            "year_num": row.year,
            "day_of_week": row.day_of_week,
        }
        for row in rows
    }
    processed_at = datetime.now(config.APP_TIMEZONE).replace(tzinfo=None)
    facts = [
        {
            "transaction_id": row.transaction_id,
            "account_id": row.account_id,
            "dest_account_id": row.dest_account_id,
            "time_id": row.time_id,
            "fraud_reason_id": row.fraud_reason_id,
            "transaction_type": row.transaction_type,
            "amount": row.amount,
            "fraud_score": row.fraud_score,
            "is_fraud": row.is_fraud,
            "event_ts": row.event_ts,
            "ingested_at": row.ingested_at,
            "processed_at": processed_at,
        }
        for row in rows
    ]

    with get_pool().acquire() as conn:
        with conn.cursor() as cursor:
            cursor.setinputsizes(age=oracledb.DB_TYPE_NUMBER)
            cursor.executemany(MERGE_ACCOUNT_SQL, list(accounts.values()))
        with conn.cursor() as cursor:
            cursor.executemany(INSERT_TIME_SQL, list(times.values()))
        with conn.cursor() as cursor:
            cursor.setinputsizes(
                dest_account_id=oracledb.DB_TYPE_VARCHAR,
                ingested_at=oracledb.DB_TYPE_TIMESTAMP,
            )
            cursor.executemany(MERGE_FACT_SQL, facts)
        conn.commit()
