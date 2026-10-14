"""Cấu hình tập trung cho toàn bộ pipeline.

Giá trị được đọc từ biến môi trường hoặc file `.env` ở thư mục gốc project
(copy từ `.env.example`). Biến nào không khai báo sẽ dùng giá trị mặc định
phù hợp với docker-compose.yml.
"""
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Oracle (Data Warehouse)
# ---------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "1521"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SERVICE_NAME = os.getenv("DB_SERVICE_NAME", "FREEPDB1")
DB_DSN = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}"

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TRANSACTIONS = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")
KAFKA_TOPIC_FRAUD_ALERTS = os.getenv("KAFKA_TOPIC_FRAUD_ALERTS", "fraud_alerts")

# ---------------------------------------------------------------------------
# Redis (Lookup cache + velocity state)
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# ---------------------------------------------------------------------------
# Dữ liệu giả lập
# ---------------------------------------------------------------------------
NUM_ACCOUNTS = int(os.getenv("NUM_ACCOUNTS", "10000"))
PRODUCER_RATE = float(os.getenv("PRODUCER_RATE", "500"))   # giao dịch / giây

# Múi giờ nghiệp vụ: dùng cho timestamp giao dịch, dim_time và luật "giao dịch ban đêm"
APP_TIMEZONE = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Ho_Chi_Minh"))

# ---------------------------------------------------------------------------
# Spark Structured Streaming
# ---------------------------------------------------------------------------
# local[*,4]: dùng mọi core, retry mỗi task tối đa 4 lần trước khi fail query
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*,4]")
CHECKPOINT_DIR = os.getenv(
    "CHECKPOINT_DIR", str(PROJECT_ROOT / "streaming_app" / ".checkpoint")
)
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "5000"))
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "512m")
# Rỗng = chạy micro-batch liên tục (độ trễ thấp nhất). Đặt vd "2 seconds" để
# gom batch lớn hơn, giảm CPU nhưng độ trễ tăng tương ứng.
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "")


def require(*names):
    """Dừng sớm với thông báo rõ ràng nếu thiếu biến cấu hình bắt buộc."""
    missing = [name for name in names if not globals().get(name)]
    if missing:
        raise RuntimeError(
            f"Thiếu cấu hình: {', '.join(missing)}. "
            "Copy .env.example thành .env rồi điền giá trị."
        )
