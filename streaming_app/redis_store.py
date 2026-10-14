"""Truy cập Redis: lookup profile khách hàng và trạng thái cửa sổ velocity.

Mọi hàm đều làm việc theo lô (MGET / pipeline) để mỗi partition chỉ tốn
vài round-trip tới Redis thay vì một round-trip cho mỗi giao dịch.
"""
import json

import redis

import config

_client = None


def get_client():
    """Redis client dùng chung trong một Python worker (worker được Spark tái sử dụng)."""
    global _client
    if _client is None:
        _client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True,
        )
    return _client


def get_profiles(client, account_ids):
    """Trả về {account_id: profile_dict} cho các account có trong cache."""
    unique_ids = list(dict.fromkeys(account_ids))
    if not unique_ids:
        return {}
    raws = client.mget(unique_ids)
    return {acc: json.loads(raw) for acc, raw in zip(unique_ids, raws) if raw}


def velocity_counts(client, events, window_seconds):
    """Đếm số giao dịch lớn của cùng tài khoản trong cửa sổ trượt theo event time.

    events: list (transaction_id, account_id, epoch_seconds).
    Trả về {transaction_id: số giao dịch trong [t - window, t]}, tính cả chính nó.

    Mỗi tài khoản là một sorted set `velocity:<account_id>` với member là
    transaction_id, nên khi Spark retry batch thì ZADD lại không bị đếm trùng.
    """
    if not events:
        return {}

    pipe = client.pipeline(transaction=False)
    for txn_id, account_id, ts in events:
        pipe.zadd(f"velocity:{account_id}", {txn_id: ts})
    for txn_id, account_id, ts in events:
        pipe.zcount(f"velocity:{account_id}", ts - window_seconds, ts)

    # Dọn các giao dịch đã ra khỏi cửa sổ, sau khi đã đếm xong cả batch
    latest_ts = {}
    for _, account_id, ts in events:
        latest_ts[account_id] = max(ts, latest_ts.get(account_id, ts))
    for account_id, ts in latest_ts.items():
        key = f"velocity:{account_id}"
        pipe.zremrangebyscore(key, "-inf", f"({ts - window_seconds}")
        pipe.expire(key, window_seconds * 2)

    results = pipe.execute()
    counts = results[len(events):2 * len(events)]
    return {txn_id: count for (txn_id, _, _), count in zip(events, counts)}
