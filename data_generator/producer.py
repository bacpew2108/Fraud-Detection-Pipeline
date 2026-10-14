"""Mock Payment Gateway: liên tục sinh giao dịch giả lập và đẩy vào Kafka.

Ngoài giao dịch bình thường, generator cố tình sinh các kịch bản gian lận
để pipeline phát hiện (xem streaming_app/fraud_rules.py):
  - drain    : rút sạch số dư tài khoản
  - velocity : chuỗi giao dịch lớn liên tiếp từ cùng một tài khoản
  - night    : giao dịch lớn lúc 3-4h sáng (giả lập dữ liệu đồng bộ trễ từ
               POS offline, nên timestamp lùi về đêm gần nhất)
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import random
import signal
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta

from kafka import KafkaProducer

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

TZ = config.APP_TIMEZONE

# Tỉ lệ kích hoạt mỗi kịch bản trên mỗi lượt sinh dữ liệu
DRAIN_RATE = 0.02
VELOCITY_BURST_RATE = 0.002
NIGHT_RATE = 0.005

TX_TYPES = ["TRANSFER", "CASH_OUT", "PAYMENT", "DEBIT"]


def random_account():
    return f"ACC_{random.randint(1, config.NUM_ACCOUNTS):05d}"


def make_transaction(id_orig, amount, old_balance, event_time, tx_type=None):
    dest_prefix = random.choice(["C", "M"])   # C: khách hàng, M: merchant
    id_dest = f"{dest_prefix}_{random.randint(1, 50000):05d}"
    amount = round(amount, 2)
    old_balance = round(old_balance, 2)
    old_balance_dest = round(random.uniform(0.0, 20000.0), 2) if dest_prefix == "C" else 0.0
    new_balance_dest = round(old_balance_dest + amount, 2) if dest_prefix == "C" else 0.0
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": event_time.isoformat(),
        "type": tx_type or random.choice(TX_TYPES),
        "amount": amount,
        "idOrig": id_orig,
        "oldbalanceOrg": old_balance,
        "newbalanceOrig": round(old_balance - amount, 2),
        "idDest": id_dest,
        "oldbalanceDest": old_balance_dest,
        "newbalanceDest": new_balance_dest,
    }


def normal_transaction(now):
    old_balance = random.uniform(1000.0, 50000.0)
    amount = random.uniform(10.0, old_balance * 0.3)
    return make_transaction(random_account(), amount, old_balance, now)


def drain_transaction(now):
    old_balance = random.uniform(1000.0, 50000.0)
    return make_transaction(random_account(), old_balance, old_balance, now, "CASH_OUT")


def velocity_burst(now):
    """4-6 giao dịch lớn (>= 10,000) liên tiếp từ cùng một tài khoản."""
    account = random_account()
    size = random.randint(4, 6)
    burst = []
    for i in range(size):
        old_balance = random.uniform(30000.0, 50000.0)
        amount = random.uniform(10000.0, 15000.0)
        event_time = now - timedelta(milliseconds=200 * (size - 1 - i))
        burst.append(make_transaction(account, amount, old_balance, event_time, "TRANSFER"))
    return burst


def night_transaction(now):
    """Giao dịch lớn với timestamp rơi vào 03:00-04:59 của đêm gần nhất."""
    night = now.replace(hour=random.choice([3, 4]),
                        minute=random.randint(0, 59),
                        second=random.randint(0, 59))
    if night > now:
        night -= timedelta(days=1)
    old_balance = random.uniform(30000.0, 50000.0)
    amount = random.uniform(10000.0, old_balance * 0.6)
    return make_transaction(random_account(), amount, old_balance, night, "TRANSFER")


def generate(now):
    """Sinh một lượt giao dịch, trả về (tên kịch bản, list giao dịch)."""
    roll = random.random()
    if roll < VELOCITY_BURST_RATE:
        return "velocity", velocity_burst(now)
    roll -= VELOCITY_BURST_RATE
    if roll < NIGHT_RATE:
        return "night", [night_transaction(now)]
    roll -= NIGHT_RATE
    if roll < DRAIN_RATE:
        return "drain", [drain_transaction(now)]
    return "normal", [normal_transaction(now)]


def main():
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        # Key = tài khoản gửi: mọi giao dịch của một tài khoản vào cùng partition
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
    )
    failed = Counter()

    def on_send_error(exc):
        failed[type(exc).__name__] += 1
        log.error("Gửi Kafka thất bại: %s", exc)

    # docker stop / kill gửi SIGTERM: dừng gọn như Ctrl+C để flush message còn trong buffer
    def on_sigterm(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, on_sigterm)

    interval = 1.0 / config.PRODUCER_RATE
    next_send = time.monotonic()
    sent = 0
    scenarios = Counter()
    log.info("Bắt đầu đẩy giao dịch vào topic '%s' (~%.0f GD/s). Ctrl+C để dừng.",
             config.KAFKA_TOPIC_TRANSACTIONS, config.PRODUCER_RATE)

    try:
        while True:
            scenario, transactions = generate(datetime.now(TZ))
            scenarios[scenario] += 1
            for txn in transactions:
                producer.send(config.KAFKA_TOPIC_TRANSACTIONS, key=txn["idOrig"], value=txn) \
                    .add_errback(on_send_error)
                sent += 1
                if sent % 500 == 0:
                    log.info("Đã đẩy %d GD | kịch bản: %s | lỗi gửi: %d",
                             sent, dict(scenarios), sum(failed.values()))

                # Giới hạn tốc độ; nếu bị trễ quá 1 giây thì bỏ qua phần trễ thay vì dồn burst
                next_send += interval
                delay = next_send - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                elif delay < -1:
                    next_send = time.monotonic()
    except KeyboardInterrupt:
        log.info("Đang dừng producer...")
    finally:
        producer.flush()
        producer.close()
        log.info("Đã đẩy tổng cộng %d GD, kịch bản: %s", sent, dict(scenarios))


if __name__ == "__main__":
    main()
