"""Throughput math and contract checks; the live section runs only when REDIS_URL is reachable.
Run: uv run --env-file .env python test_throughput.py
"""

from __future__ import annotations

import json
import os
import time
import uuid
from types import SimpleNamespace

import throughput as t

# percentile: nearest rank
assert t.percentile([5], 95) == 5
assert t.percentile(list(range(1, 101)), 50) == 50
assert t.percentile(list(range(1, 101)), 95) == 95
assert t.percentile([3, 1, 2], 100) == 3

# verdict
assert t.verdict(10, 10, 0, 500.0, 100.0) == "pass"
assert t.verdict(10, 9, 0, 500.0, 100.0) == "fail"  # lost
assert t.verdict(10, 10, 1, 500.0, 100.0) == "fail"  # duplicates
assert t.verdict(10, 10, 0, t.MIN_JOBS_PER_S - 1, 100.0) == "warn"
assert t.verdict(10, 10, 0, 500.0, t.MAX_P95_WAIT_MS + 1) == "warn"
assert t.verdict(10, 10, 0, None, None) == "warn"  # finished in zero time: rate unknown

# count validation
assert t.parse_count({}) == t.DEFAULT_COUNT
assert t.parse_count({"count": 1}) == 1 and t.parse_count({"count": t.MAX_COUNT}) == t.MAX_COUNT
for bad in (0, -1, t.MAX_COUNT + 1, 1.5, "10", True, None):
    assert isinstance(t.parse_count({"count": bad}), str), bad

# deadline grows with count and is bounded
assert t.deadline(1) < t.deadline(1000) < t.deadline(t.MAX_COUNT) <= 300

# start() rejects a bad count before touching Redis
t._telemetry = None
assert t.start({"count": 0})[0] == 400

# live: seed a finished run by hand and read it back through status()
url = os.environ.get("REDIS_URL")
store = None
if url:
    import redis

    try:
        store = redis.Redis.from_url(url, decode_responses=True)
        store.ping()
    except redis.RedisError:
        store = None
if store is None:
    print("throughput math checks passed (live section skipped: REDIS_URL not reachable)")
    raise SystemExit(0)

# Own key namespace: never touch a real run's lock or hash.
t.PREFIX = f"lcq-throughput-test-{uuid.uuid4().hex}:"
t.ACTIVE = t.PREFIX + "active"
messages = []
t._telemetry = SimpleNamespace(store=store)
t._log = SimpleNamespace(info=lambda msg, extra=None: messages.append((msg, extra)))
assert t.status("nope" + uuid.uuid4().hex)[0] == 404

run, now = uuid.uuid4().hex, time.time()
key = t.PREFIX + run
fields = {"count": 4, "started_at": now - 2}
for i in range(4):
    fields |= {f"d:j{i}": 1 + (i == 0), f"w:j{i}": 0.1 * (i + 1), f"f:j{i}": now - 1 + i * 0.1}
store.hset(key, mapping=fields)
store.expire(key, 60)
store.set(t.ACTIVE, run, ex=60)
try:
    code, body = t.status(run)
    assert code == 200 and body["state"] == "done" and body["verdict"] == "fail", body  # one duplicate
    assert (body["processed"], body["lost"], body["duplicates"]) == (4, 0, 1), body
    assert body["wait_ms"] == {"p50": 200.0, "p95": 400.0}, body
    assert body["jobs_per_s"] == round(4 / 1.3, 1), body
    assert store.get(t.ACTIVE) is None  # finishing frees the lock
    assert [m for m, _ in messages] == ["throughput finished"]
    store.hset(key, "f:late", now)  # a late job after the verdict must not change it
    assert t.status(run) == (200, body) and len(messages) == 1
    # A second process that loses the freeze returns the stored result, not its own.
    store.hdel(key, "result")
    store.hset(key, "result", json.dumps({**body, "verdict": "pass"}))
    assert t.status(run)[1]["verdict"] == "pass"
    # Lock: NX only; a second start is refused while one is active.
    store.set(t.ACTIVE, "other", ex=60)
    assert t.start({"count": 1})[0] == 409
    assert store.get(t.ACTIVE) == "other"
    # A hash recreated by a late job (no count) reads as unknown.
    store.delete(key)
    store.hset(key, "f:late", now)
    assert t.status(run)[0] == 404
finally:
    store.delete(key, t.ACTIVE)
print("throughput checks passed (live section ran)")
