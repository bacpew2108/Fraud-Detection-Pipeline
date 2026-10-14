from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from data_generator import producer
from streaming_app import fraud_rules as rules

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
DAY_HOUR = 14


def evaluate(amount=500.0, old_balance=20000.0, risk="Low", hour=DAY_HOUR, velocity=0):
    return rules.evaluate(amount, old_balance, risk, hour, velocity)


# ---------------------------------------------------------------------------
# evaluate
# ---------------------------------------------------------------------------
def test_normal_transaction_is_not_fraud():
    verdict = evaluate()
    assert not verdict.is_fraud
    assert verdict.fraud_score == 0.0
    assert verdict.fraud_reason_id == rules.NO_FRAUD


def test_full_balance_drain_tolerates_float_noise():
    verdict = evaluate(amount=12345.67, old_balance=12345.670000001)
    assert verdict.is_fraud
    assert verdict.fraud_reason_id == rules.FULL_BALANCE_DRAIN


def test_zero_balance_is_not_drain():
    assert not evaluate(amount=0.0, old_balance=0.0).is_fraud


@pytest.mark.parametrize("risk, amount, expected", [
    ("High", 10_000.01, True),
    ("High", 10_000.00, False),
    ("Medium", 15_000.00, False),
])
def test_high_risk_large_amount(risk, amount, expected):
    verdict = evaluate(amount=amount, old_balance=50_000, risk=risk)
    assert verdict.is_fraud is expected
    if expected:
        assert verdict.fraud_reason_id == rules.HIGH_RISK_LARGE_AMOUNT


@pytest.mark.parametrize("velocity, expected", [(2, False), (3, True), (6, True)])
def test_high_velocity(velocity, expected):
    verdict = evaluate(amount=12_000, old_balance=50_000, velocity=velocity)
    assert verdict.is_fraud is expected


@pytest.mark.parametrize("hour, expected", [(2, False), (3, True), (4, True), (5, False)])
def test_night_large_amount(hour, expected):
    verdict = evaluate(amount=12_000, old_balance=50_000, hour=hour)
    assert verdict.is_fraud is expected


def test_small_night_transaction_is_not_fraud():
    assert not evaluate(amount=500, hour=3).is_fraud


def test_multiple_rules_raise_score_and_pick_strongest_reason():
    single = evaluate(amount=12_000, old_balance=50_000, hour=3)
    combined = evaluate(amount=12_000, old_balance=50_000, hour=3, velocity=4, risk="High")
    assert combined.fraud_score > single.fraud_score
    assert combined.fraud_reason_id == rules.HIGH_VELOCITY
    assert set(combined.triggered) == {
        rules.HIGH_VELOCITY, rules.HIGH_RISK_LARGE_AMOUNT, rules.NIGHT_LARGE_AMOUNT,
    }
    assert 0.0 <= combined.fraud_score <= 1.0


def test_every_reason_has_a_weight_above_threshold():
    for reason_id, (_name, _level, weight) in rules.FRAUD_REASONS.items():
        if reason_id != rules.NO_FRAUD:
            assert weight >= rules.FRAUD_SCORE_THRESHOLD


# ---------------------------------------------------------------------------
# parse_event_time / time_id_for
# ---------------------------------------------------------------------------
def test_parse_event_time_converts_to_business_timezone():
    parsed = rules.parse_event_time("2026-10-14T20:30:00Z", TZ)
    assert parsed.hour == 3 and parsed.day == 15
    assert parsed.utcoffset() == timedelta(hours=7)


def test_parse_event_time_naive_is_business_timezone():
    parsed = rules.parse_event_time("2026-10-14T03:15:00", TZ)
    assert parsed.hour == 3 and parsed.tzinfo == TZ


@pytest.mark.parametrize("value", [None, "", "not-a-date", "2026-13-45T00:00:00"])
def test_parse_event_time_invalid_returns_none(value):
    assert rules.parse_event_time(value, TZ) is None


def test_time_id_for():
    assert rules.time_id_for(datetime(2026, 10, 14, 9, 59, tzinfo=TZ)) == 2026101409


# ---------------------------------------------------------------------------
# Producer phải sinh đúng kịch bản mà luật bắt được
# ---------------------------------------------------------------------------
NOW = datetime(2026, 10, 14, DAY_HOUR, 0, tzinfo=TZ)


def evaluate_txn(txn, velocity=0):
    event_time = rules.parse_event_time(txn["timestamp"], TZ)
    return rules.evaluate(txn["amount"], txn["oldbalanceOrg"], "Low", event_time.hour, velocity)


def test_producer_normal_transactions_are_not_flagged():
    for _ in range(1000):
        assert not evaluate_txn(producer.normal_transaction(NOW)).is_fraud


def test_producer_drain_is_detected():
    verdict = evaluate_txn(producer.drain_transaction(NOW))
    assert verdict.fraud_reason_id == rules.FULL_BALANCE_DRAIN


def test_producer_night_is_detected_and_not_in_future():
    for now in (NOW, NOW.replace(hour=3, minute=30), NOW.replace(hour=1)):
        txn = producer.night_transaction(now)
        event_time = rules.parse_event_time(txn["timestamp"], TZ)
        assert event_time <= now
        assert evaluate_txn(txn).fraud_reason_id == rules.NIGHT_LARGE_AMOUNT


def test_producer_velocity_burst_is_large_same_account_and_in_window():
    burst = producer.velocity_burst(NOW)
    times = [rules.parse_event_time(t["timestamp"], TZ) for t in burst]
    assert len(burst) >= rules.VELOCITY_MIN_COUNT
    assert len({t["idOrig"] for t in burst}) == 1
    assert all(rules.counts_for_velocity(t["amount"]) for t in burst)
    assert max(times) - min(times) < timedelta(seconds=rules.VELOCITY_WINDOW_SECONDS)
    assert max(times) <= NOW


def test_producer_transaction_ids_are_unique():
    ids = {producer.normal_transaction(NOW)["transaction_id"] for _ in range(10_000)}
    assert len(ids) == 10_000


def test_producer_timestamp_is_current_time():
    now = datetime.now(timezone.utc).astimezone(TZ)
    txn = producer.normal_transaction(now)
    assert rules.parse_event_time(txn["timestamp"], TZ) == now
