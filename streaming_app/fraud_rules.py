"""Luật phát hiện gian lận (rule-based).

Module thuần Python, không phụ thuộc Spark/Redis/Oracle để có thể unit test.
Đây cũng là nguồn duy nhất của danh mục lý do gian lận: spark_processor seed
FRAUD_REASONS vào bảng dim_fraud_reason khi khởi động.
"""
from dataclasses import dataclass
from datetime import datetime

# ---------------------------------------------------------------------------
# Danh mục lý do gian lận
#   fraud_reason_id -> (reason_name, risk_level, weight)
#   weight: mức độ nghi ngờ khi luật bị kích hoạt, dùng để tính fraud_score
# ---------------------------------------------------------------------------
NO_FRAUD = 1
FULL_BALANCE_DRAIN = 2
HIGH_RISK_LARGE_AMOUNT = 3
HIGH_VELOCITY = 4
NIGHT_LARGE_AMOUNT = 5

FRAUD_REASONS = {
    NO_FRAUD:               ("No Fraud",                         "Low",    0.0),
    FULL_BALANCE_DRAIN:     ("Full Balance Drain",               "High",   0.9),
    HIGH_RISK_LARGE_AMOUNT: ("High Risk Account Large Transfer", "High",   0.7),
    HIGH_VELOCITY:          ("High Velocity Large Transactions", "High",   0.8),
    NIGHT_LARGE_AMOUNT:     ("Large Transaction At Night",       "Medium", 0.6),
}

# ---------------------------------------------------------------------------
# Ngưỡng của từng luật
# ---------------------------------------------------------------------------
BALANCE_TOLERANCE = 0.01          # sai số khi so sánh tiền kiểu float
HIGH_RISK_AMOUNT = 10_000         # tài khoản risk_segment = High giao dịch > ngưỡng này
NIGHT_HOURS = range(3, 5)         # 03:00 – 04:59 theo APP_TIMEZONE
NIGHT_AMOUNT = 10_000
VELOCITY_MIN_AMOUNT = 10_000      # chỉ đếm giao dịch lớn cho luật velocity
VELOCITY_WINDOW_SECONDS = 30
VELOCITY_MIN_COUNT = 3            # >= 3 giao dịch lớn / 30 giây / tài khoản
FRAUD_SCORE_THRESHOLD = 0.5


@dataclass(frozen=True)
class Verdict:
    is_fraud: bool
    fraud_score: float
    fraud_reason_id: int
    triggered: tuple


def counts_for_velocity(amount):
    """Giao dịch có được tính vào cửa sổ velocity hay không."""
    return amount is not None and amount >= VELOCITY_MIN_AMOUNT


def evaluate(amount, old_balance, risk_segment, local_hour, velocity_count):
    """Chấm điểm một giao dịch.

    fraud_score kết hợp các luật bị kích hoạt theo kiểu noisy-OR:
    score = 1 - Π(1 - weight). Lý do chính là luật có weight cao nhất.
    """
    triggered = []
    if old_balance and abs(amount - old_balance) <= BALANCE_TOLERANCE:
        triggered.append(FULL_BALANCE_DRAIN)
    if risk_segment == "High" and amount > HIGH_RISK_AMOUNT:
        triggered.append(HIGH_RISK_LARGE_AMOUNT)
    if velocity_count >= VELOCITY_MIN_COUNT:
        triggered.append(HIGH_VELOCITY)
    if local_hour in NIGHT_HOURS and amount >= NIGHT_AMOUNT:
        triggered.append(NIGHT_LARGE_AMOUNT)

    not_fraud_prob = 1.0
    for reason_id in triggered:
        not_fraud_prob *= 1.0 - FRAUD_REASONS[reason_id][2]
    score = round(1.0 - not_fraud_prob, 4)

    if score < FRAUD_SCORE_THRESHOLD:
        return Verdict(False, score, NO_FRAUD, tuple(triggered))
    main_reason = max(triggered, key=lambda reason_id: FRAUD_REASONS[reason_id][2])
    return Verdict(True, score, main_reason, tuple(triggered))


def parse_event_time(value, tz):
    """Parse timestamp ISO-8601 và quy đổi sang múi giờ nghiệp vụ.

    Trả về None nếu không parse được (thay vì gán một ngày giả).
    Timestamp không có offset được hiểu là đã ở múi giờ `tz`.
    """
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def time_id_for(event_time):
    """Smart key cho dim_time theo grain giờ: YYYYMMDDHH (vd 2026101415)."""
    return (
        event_time.year * 1_000_000
        + event_time.month * 10_000
        + event_time.day * 100
        + event_time.hour
    )
