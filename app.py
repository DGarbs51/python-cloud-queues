"""Plain-Python demo for laravel-cloud-queues.

Web dashboard: uv run python app.py                         (http://127.0.0.1:8000)
Worker:        uv run laravel-cloud-queues work app:registry
"""

from __future__ import annotations

import asyncio
import ctypes
import json
import logging
import math
import mmap
import os
import platform
import random
import re
import socket
import ssl
import sys
import sysconfig
import threading
import time
import uuid
from collections import Counter
from collections.abc import Mapping
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from laravel_cloud_queues import Job, Registry, RetryPolicy, current_job
from redis.exceptions import NoScriptError
from telemetry import (
    BURST_SIZE,
    CHECK_KINDS,
    DELAY_SECONDS,
    TIMEOUT_SECONDS,
    Telemetry,
)

registry = Registry()
telemetry = Telemetry(registry, "Plain Python")
INDEX = Path(__file__).with_name("index.html")


@registry.job(name="demo.quick")
def quick() -> None:
    with telemetry.tracked():
        time.sleep(random.uniform(0.05, 0.3))


@registry.job(name="demo.async")
async def async_job() -> None:
    with telemetry.tracked():
        await asyncio.sleep(random.uniform(0.05, 0.3))


@registry.job(name="demo.slow")
def slow() -> None:
    with telemetry.tracked():
        time.sleep(3)


@registry.job(name="demo.flaky", tries=3, backoff=[2])
def flaky() -> None:
    with telemetry.tracked():
        if current_job().attempt == 1:
            raise RuntimeError("flaky job fails on its first attempt")


@registry.job(name="demo.failing", tries=2, backoff=[1])
def failing() -> None:
    with telemetry.tracked():
        raise RuntimeError("this job always fails")


@registry.job(name="demo.timeout", tries=2, timeout=TIMEOUT_SECONDS)
def timeout() -> None:
    # Exceeds its timeout: the worker exits 124 and the platform restarts it.
    with telemetry.tracked():
        time.sleep(TIMEOUT_SECONDS + 7)


DISPATCHES = {  # kind: (job, delay seconds, how many)
    "quick": (quick, 0, 1),
    "async": (async_job, 0, 1),
    "slow": (slow, 0, 1),
    "delayed": (quick, DELAY_SECONDS, 1),
    "flaky": (flaky, 0, 1),
    "failing": (failing, 0, 1),
    "timeout": (timeout, 0, 1),
    "burst": (quick, 0, BURST_SIZE),
}


def dispatch(kind: str) -> list[str]:
    job, delay, count = DISPATCHES[kind]
    uuids = []
    for _ in range(count):
        at = time.time()
        receipt = job.options(delay=delay).dispatch()
        telemetry.queued(job.name, receipt.uuid, at, delay)
        uuids.append(receipt.uuid)
    return uuids


def run_check() -> None:
    telemetry.save_check({kind: dispatch(kind) for kind in CHECK_KINDS})


TTL = 6 * 60 * 60
PREFIX = "lcq-load:"
ACTIVE = PREFIX + "active"
RUNS = PREFIX + "runs"
SERVER = SERVER_NAME = "stdlib"
MAX_BODY = 64 * 1024
TERMINAL = {"done", "failed", "expired"}
HOLD_LOCK = threading.Lock()


class ValidationError(ValueError):
    """Invalid client input, distinct from failures reading internal state."""


def run_key(run: str) -> str:
    if not isinstance(run, str) or not re.fullmatch(r"[0-9a-f]{32}", run):
        raise ValidationError("invalid run id")
    return PREFIX + "run:" + run


def positive_int(value: object, name: str, cap: int) -> int:
    if type(value) is not int or not 1 <= value <= cap:
        raise ValidationError(f"{name} must be an integer from 1 to {cap}")
    return value


def load_options(data: dict) -> dict:
    kind = data.get("kind")
    if not isinstance(kind, str) or kind not in LOAD_JOBS:
        raise ValidationError("unknown load kind")
    result = dict(kind=kind, count=positive_int(data.get("count"), "count", 10000),
                  ms=positive_int(data.get("ms", 100), "ms", 600000 if kind == "mem" else 30000), mb=None, rows=None)
    if kind == "mem":
        result["mb"] = positive_int(data.get("mb"), "mb", 1800)
    elif "mb" in data:
        raise ValidationError("mb is only valid for mem jobs")
    if kind.startswith("db_"):
        result["rows"] = positive_int(data.get("rows", 10), "rows", 1000)
    elif "rows" in data:
        raise ValidationError("rows is only valid for db jobs")
    if "key" in data and (not isinstance(data["key"], str) or not 1 <= len(data["key"]) <= 64):
        raise ValidationError("key must be a string of 1 to 64 characters")
    return result


def cgroup(name: str):
    try:
        return (Path("/sys/fs/cgroup") / name).read_text().strip()
    except OSError:
        return None


def memory_sample() -> dict:
    rss = None
    try:
        rss = int(Path("/proc/self/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        if sys.platform == "darwin":
            libc = ctypes.CDLL(None)
            info = (ctypes.c_uint32 * 256)()
            count = ctypes.c_uint32(256)
            if libc.task_info(libc.mach_task_self(), 20, info, ctypes.byref(count)) == 0:
                rss = ctypes.cast(info, ctypes.POINTER(ctypes.c_uint64))[1]
    current = cgroup("memory.current")
    events = dict(line.split() for line in (cgroup("memory.events") or "").splitlines())
    return {"rss_mb": round(rss / 1048576, 2) if rss is not None else None,
            "cgroup_mem_mb": round(int(current) / 1048576, 2) if current else None,
            "oom_events": int(events.get("oom_kill", 0))}


@contextmanager
def load_record(run: str, queued_at: float | None = None):
    key = run_key(run)
    store = telemetry.store
    if not store.exists(key):
        raise ValueError("load run not found or expired")
    job = current_job()
    sample = memory_sample()
    record = dict(queued_at=queued_at or time.time(), started_at=time.time(), finished_at=None,
                  worker=f"{socket.gethostname()}:{os.getpid()}", host=socket.gethostname(),
                  python=platform.python_version(), ok=None, error=None, **sample)
    record["oom_start"] = sample["oom_events"]
    pipe = store.pipeline()
    pipe.hincrby(key + ":attempts", job.uuid, 1)
    pipe.expire(key + ":attempts", TTL)
    pipe.hsetnx(key + ":jobs", job.uuid, json.dumps(record))
    pipe.expire(key + ":jobs", TTL)
    pipe.hincrby(key, "revision", 1)
    pipe.hdel(key, "summary")
    pipe.execute()
    cancelled = bool(store.exists(key + ":cancel")) or store.hget(key, "state") == "expired"
    try:
        yield record, cancelled
        record.update(ok=None if cancelled else True, skipped=cancelled)
    except Exception as exc:
        record.update(ok=False, error=f"{type(exc).__name__}: {exc}"[:200])
        raise
    finally:
        end = memory_sample()
        for field in ("rss_mb", "cgroup_mem_mb", "oom_events"):
            values = [v for v in (record[field], end[field]) if v is not None]
            record[field] = max(values) if values else None
        record["finished_at"] = time.time()
        save_load_record(key, job.uuid, record)


def save_load_record(key: str, job_uuid: str, record: dict) -> None:
    # Preserve the first completed delivery while counting redeliveries separately.
    telemetry.store.eval("""
        local old = redis.call('HGET', KEYS[1], ARGV[1])
        if not old or cjson.decode(old).finished_at == cjson.null then
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        end
        redis.call('EXPIRE', KEYS[1], ARGV[3])
        if redis.call('EXISTS', KEYS[2]) == 1 then
            redis.call('HINCRBY', KEYS[2], 'revision', 1)
            redis.call('HDEL', KEYS[2], 'summary')
        end
    """, 2, key + ":jobs", key, job_uuid, json.dumps(record), TTL)


@registry.job(name="load.sync_sleep", tries=1, timeout=90)
def load_sync(run: str, ms: int, queued_at: float | None = None) -> None:
    positive_int(ms, "ms", 30000)
    with load_record(run, queued_at) as (_, cancelled):
        if not cancelled:
            time.sleep(ms / 1000)


@registry.job(name="load.async_sleep", tries=1, timeout=90)
async def load_async(run: str, ms: int, queued_at: float | None = None) -> None:
    positive_int(ms, "ms", 30000)
    with load_record(run, queued_at) as (_, cancelled):
        if not cancelled:
            await asyncio.sleep(ms / 1000)


@registry.job(name="load.cpu", tries=1, timeout=90)
def load_cpu(run: str, ms: int, queued_at: float | None = None) -> None:
    positive_int(ms, "ms", 30000)
    with load_record(run, queued_at) as (_, cancelled):
        if not cancelled:
            # Spend ms of CPU time, not wall time: under cgroup throttling a wall-clock
            # spin finishes on schedule and hides the CPU limit.
            until = time.process_time() + ms / 1000
            while time.process_time() < until:
                pass


@registry.job(name="load.mem", tries=1, timeout=1230)
def load_mem(run: str, mb: int, ms: int, queued_at: float | None = None) -> None:
    positive_int(mb, "mb", 1800)
    positive_int(ms, "ms", 600000)
    with load_record(run, queued_at) as (record, cancelled):
        if not cancelled:
            memory = bytearray(mb * 1048576)
            for offset in range(0, len(memory), 4096):
                memory[offset] = 1
            record.update(memory_sample())
            save_load_record(run_key(run), current_job().uuid, record)
            time.sleep(ms / 1000)
            del memory


@registry.job(name="load.db_write", tries=1, timeout=90)
def load_db_write(run: str, rows: int, queued_at: float | None = None) -> None:
    positive_int(rows, "rows", 1000)
    with load_record(run, queued_at) as (_, cancelled):
        if not cancelled:
            import db
            db.write_rows(run, rows)


@registry.job(name="load.db_read", tries=1, timeout=90)
def load_db_read(run: str, rows: int, queued_at: float | None = None) -> None:
    positive_int(rows, "rows", 1000)
    with load_record(run, queued_at) as (_, cancelled):
        if not cancelled:
            import db
            db.read_rows(rows)


@registry.job(name="load.db_sync", tries=1, timeout=90)
def load_db_sync(run: str, rows: int, queued_at: float | None = None) -> None:
    positive_int(rows, "rows", 1000)
    with load_record(run, queued_at) as (_, cancelled):
        if not cancelled:
            import db
            db.query_rows(rows)


@registry.job(name="load.db_async", tries=1, timeout=90)
async def load_db_async(run: str, rows: int, queued_at: float | None = None) -> None:
    positive_int(rows, "rows", 1000)
    with load_record(run, queued_at) as (_, cancelled):
        if not cancelled:
            import db
            await db.query_rows_async(rows)


LOAD_JOBS = {"sync": load_sync, "async": load_async, "cpu": load_cpu, "mem": load_mem,
             "db_write": load_db_write, "db_read": load_db_read,
             "db_async": load_db_async, "db_sync": load_db_sync}


def compat_probes() -> list:
    try:
        from compat import run_probes
    except ImportError:
        run_probes = None
    if run_probes is None:
        return [{"name": "compat", "group": "runtime", "min_python": "3.10",
                 "status": "info", "detail": "compat module missing"}]
    return run_probes()


@registry.job(name="demo.compat", tries=1, timeout=180)
def worker_compat() -> None:
    telemetry.store.set(PREFIX + "compat:worker",
                        json.dumps({"probes": compat_probes(), "at": time.time()}), ex=TTL)


def release_active(run: str) -> None:
    telemetry.store.eval("""
        if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
        return 0
    """, 1, ACTIVE, run)


def percentiles(values: list) -> dict:
    values.sort()
    return {f"p{p}": round(values[max(0, math.ceil(len(values) * p / 100) - 1)], 2)
            if values else None for p in (50, 95, 99)}


def load_snapshot(run: str) -> dict | None:
    """Return run metrics; oom_events is best-effort across cgroup/container restarts.

    Failed/lost jobs remain the reliable failure signal during OOM tests.
    """
    key = run_key(run)
    summary = telemetry.store.hget(key, "summary")
    if summary:
        release_active(run)
        return json.loads(summary)
    pipe = telemetry.store.pipeline()
    pipe.hgetall(key)
    pipe.hgetall(key + ":accepted")
    pipe.hgetall(key + ":jobs")
    pipe.hvals(key + ":attempts")
    pipe.exists(key + ":cancel")
    meta, accepted, raw_jobs, attempts, cancelled = pipe.execute()
    if not meta:
        return None
    config = json.loads(meta["config"])
    jobs = {uid: json.loads(raw) for uid, raw in raw_jobs.items()}
    # A worker can finish before the dispatch receipt is persisted; its record is proof of acceptance.
    accepted.update({uid: r["queued_at"] for uid, r in jobs.items() if uid not in accepted})
    records = list(jobs.values())
    finished = [r for r in records if r["finished_at"] is not None]
    now = time.time()
    timeout = max(config["ms"] / 1000 * 2 + 30, 60)
    lost = [r for r in records if r["finished_at"] is None and now - r["started_at"] > timeout + 60]
    processed = sum(r["ok"] is True for r in finished)
    skipped = sum(r.get("skipped", False) for r in finished)
    failed = sum(r["ok"] is False for r in finished) + len(lost)
    state = meta["state"]
    error = meta.get("dispatch_error") or ("cancelled" if cancelled else None)
    if state not in TERMINAL:
        if state == "dispatching" and now - float(meta["heartbeat"]) > 60:
            state, error = "draining", "dispatcher stopped before completion"
        if now >= float(meta["deadline"]):
            state = "expired"
        elif state == "draining" and processed + skipped + failed >= len(accepted):
            state = "failed" if failed or error else "done"
        if state != meta["state"]:
            updates = {"state": state, "dispatch_error": error or ""}
            if state in TERMINAL:
                updates["terminal_at"] = now
                meta["terminal_at"] = str(now)
            telemetry.store.hset(key, mapping=updates)
        if state not in TERMINAL:
            telemetry.store.eval("""
                if redis.call('GET', KEYS[1]) == ARGV[1] then
                    redis.call('EXPIRE', KEYS[1], ARGV[2])
                end
            """, 1, ACTIVE, run, max(1, math.ceil(float(meta["deadline"]) - now)))
    first = min((float(at) for at in accepted.values()), default=None)
    last = max((r["finished_at"] for r in finished), default=None)
    if state == "expired":
        failed = len(accepted) - processed - skipped
    end = now
    if state in TERMINAL:
        end = float(meta.get("terminal_at", now)) if lost or state == "expired" else last
    wall = max(0, end - first) if first is not None else 0
    oom_by_host = {}
    for record in records:
        low, high = oom_by_host.get(record["host"], (record["oom_start"], record["oom_events"]))
        oom_by_host[record["host"]] = (min(low, record["oom_start"]), max(high, record["oom_events"]))
    result = dict(run=run, **config, state=state, dispatched=len(accepted), dispatch_error=error,
                processed=processed, skipped=skipped, failed=failed, duplicates=sum(max(0, int(n) - 1) for n in attempts),
                first_queued_at=first, last_finished_at=last, wall_s=round(wall, 3),
                jobs_per_s=round(processed / wall, 2) if wall else 0,
                wait_ms=percentiles([max(0, r["started_at"] - float(accepted[uid])) * 1000
                                     for uid, r in jobs.items()]),
                run_ms=percentiles([(r["finished_at"] - r["started_at"]) * 1000
                                    for r in finished if not r.get("skipped")]),
                workers=dict(Counter(r["worker"] for r in records)),
                replicas=len({r["host"] for r in records}),
                python_versions=dict(Counter(r["python"] for r in records)),
                max_rss_mb=max((r["rss_mb"] for r in records if r["rss_mb"] is not None), default=None),
                max_cgroup_mem_mb=max((r["cgroup_mem_mb"] for r in records
                                       if r["cgroup_mem_mb"] is not None), default=None),
                oom_events=sum(high - low for low, high in oom_by_host.values()))
    if state in TERMINAL:
        release_active(run)
        if config["kind"] == "db_write":
            import db
            # Any replica can reconcile an abandoned run; failure leaves it uncached for retry.
            db.cleanup(run)
        telemetry.store.eval("""
            if redis.call('EXISTS', KEYS[1]) == 1 and
               (redis.call('HGET', KEYS[1], 'revision') or '0') == ARGV[1] then
                redis.call('HSET', KEYS[1], 'summary', ARGV[2])
            end
        """, 1, key, meta.get("revision", "0"), json.dumps(result))
    return result


def dispatch_load(run: str, config: dict) -> None:
    key = run_key(run)
    store = telemetry.store
    job = LOAD_JOBS[config["kind"]]
    # Use the SDK's public Job constructor to put the per-run policy in each envelope.
    job = Job(job.func, registry=registry, name=job.name, queue=job.queue,
              policy=RetryPolicy(tries=1, timeout=max(config["ms"] / 1000 * 2 + 30, 60)))
    kwargs = {k: config[k] for k in ("rows",) if config[k] is not None}
    if not config["kind"].startswith("db_"):
        kwargs["ms"] = config["ms"]
    if config["kind"] == "mem":
        kwargs["mb"] = config["mb"]
    error = ""
    try:
        for _ in range(config["count"]):
            if store.exists(key + ":cancel"):
                error = "cancelled"
                break
            if store.get(ACTIVE) != run:
                error = "admission expired"
                break
            at = time.time()
            receipt = job.dispatch(run, queued_at=at, **kwargs)
            pipe = store.pipeline()
            pipe.hset(key + ":accepted", receipt.uuid, at)
            pipe.expire(key + ":accepted", TTL)
            pipe.hset(key, mapping={"heartbeat": time.time()})
            pipe.hincrby(key, "revision", 1)
            pipe.hdel(key, "summary")
            pipe.execute()
    except Exception as exc:
        logging.exception("Load dispatch failed: %s", run)
        error = f"{type(exc).__name__}: {exc}"[:200]
    finally:
        store.hset(key, mapping={"state": "draining", "dispatch_error": error})
    try:
        while True:
            result = load_snapshot(run)
            if result is None or result["state"] in TERMINAL:
                break
            time.sleep(1)
    except Exception:
        logging.exception("Load monitor failed: %s", run)


def load_deadline_seconds(config: dict) -> float:
    # DB budgets scale with query count (5 ms/query), within the six-hour run retention.
    ms = max(config["ms"], config["rows"] * 5) if config["rows"] is not None else config["ms"]
    return min(TTL - 60, max(600, config["count"] * ms / 1000 + 120))


ADMIT_LOAD = """
-- KEYS: active, new run, recent runs, optional idempotency, optional queue.
-- ARGV: run id, config JSON, now, deadline, record TTL, run key prefix.
if KEYS[4] ~= '' then
    local previous = redis.call('GET', KEYS[4])
    if previous then
        local raw = redis.call('HGET', ARGV[6] .. previous, 'config')
        if raw then
            local saved, requested = cjson.decode(raw), cjson.decode(ARGV[2])
            local matches = true
            for k, v in pairs(saved) do if requested[k] ~= v then matches = false end end
            for k, v in pairs(requested) do if saved[k] ~= v then matches = false end end
            if matches then return {'reused', previous} end
        end
        return {'idempotency key already used', previous}
    end
end
local active = redis.call('GET', KEYS[1])
if active then
    local state = redis.call('HGET', ARGV[6] .. active, 'state')
    local deadline = tonumber(redis.call('HGET', ARGV[6] .. active, 'deadline'))
    if not state or state == 'done' or state == 'failed' or state == 'expired' or
       (deadline and deadline <= tonumber(ARGV[3])) then
        redis.call('DEL', KEYS[1])
    else
        return {'run active', active}
    end
end
if KEYS[5] ~= '' then
    local depth = redis.call('LLEN', KEYS[5]) +
                  redis.call('ZCARD', KEYS[5] .. ':delayed') +
                  redis.call('ZCARD', KEYS[5] .. ':reserved')
    if depth > 20000 then return {'queue depth exceeds 20000', ''} end
end
redis.call('SET', KEYS[1], ARGV[1], 'PXAT', math.ceil(tonumber(ARGV[4]) * 1000))
redis.call('HSET', KEYS[2], 'config', ARGV[2], 'state', 'dispatching',
           'heartbeat', ARGV[3], 'deadline', ARGV[4], 'dispatch_error', '')
redis.call('EXPIRE', KEYS[2], ARGV[5])
redis.call('LPUSH', KEYS[3], ARGV[1])
redis.call('LTRIM', KEYS[3], 0, 19)
redis.call('EXPIRE', KEYS[3], ARGV[5])
if KEYS[4] ~= '' then redis.call('SET', KEYS[4], ARGV[1], 'EX', ARGV[5]) end
return {'created', ARGV[1]}
"""


def admit_load(store, keys: list[str], args: list):
    script = store.register_script(ADMIT_LOAD)
    try:
        return script(keys=keys, args=args)
    except NoScriptError:
        # A cache miss after redis-py's reload is safe to retry; transport failures are not.
        return store.eval(ADMIT_LOAD, len(keys), *keys, *args)


def start_load(data: dict) -> tuple[int, dict]:
    config = load_options(data)
    store = telemetry.store
    idempotency = PREFIX + "key:" + data["key"] if "key" in data else None
    run = uuid.uuid4().hex
    key = run_key(run)
    active = store.get(ACTIVE)
    if active:
        load_snapshot(active)
    redis_config = registry.config.redis
    queue = f"{redis_config.prefix}queues:{redis_config.queue}" if redis_config else ""
    now = time.time()
    # Bound abandoned runs even when the web process dies. Admission lasts through that deadline.
    deadline = now + load_deadline_seconds(config)
    outcome, value = admit_load(
        store, [ACTIVE, key, RUNS, idempotency or "", queue],
        [run, json.dumps(config), now, deadline, TTL, PREFIX + "run:"],
    )
    if outcome == "reused":
        return 202, {"run": value, "requested": config["count"]}
    if outcome != "created":
        return 409, {"error": outcome, **({"run": value} if value else {})}
    try:
        threading.Thread(target=dispatch_load, args=(run, config), daemon=True).start()
    except Exception:
        store.hset(key, mapping={"state": "failed", "dispatch_error": "could not start dispatcher"})
        release_active(run)
        raise
    return 202, {"run": run, "requested": config["count"]}


def hold_memory(mb: int, seconds: int) -> None:
    try:
        # Closing the mapping returns pages to the OS even when Python's allocator caches buffers.
        with mmap.mmap(-1, mb * 1048576) as memory:
            for offset in range(0, len(memory), 4096):
                memory[offset] = 1
            time.sleep(seconds)
    except Exception:
        logging.exception("Memory hold failed")
    finally:
        HOLD_LOCK.release()


def runtime_env() -> dict:
    sample = memory_sample()
    jit = getattr(sys, "_jit", None)
    result = dict(python=platform.python_version(), implementation=platform.python_implementation(),
                  gil_enabled=getattr(sys, "_is_gil_enabled", lambda: True)(),
                  free_threaded_build=bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
                  jit={name: getattr(jit, name, lambda: False)() for name in ("is_available", "is_enabled")},
                  nproc_os=os.cpu_count(), process_cpu_count=getattr(os, "process_cpu_count", lambda: None)(),
                  cgroup_cpu_max=cgroup("cpu.max"), cgroup_memory_max=cgroup("memory.max"),
                  memory_current=cgroup("memory.current"), rss_mb=sample["rss_mb"],
                  pids_current=cgroup("pids.current"), pids_max=cgroup("pids.max"),
                  web_concurrency=os.environ.get("WEB_CONCURRENCY"), server=SERVER, pid=os.getpid(),
                  host=socket.gethostname(), release=os.environ.get("LARAVEL_CLOUD_COMMIT_SHA"),
                  env_name=os.environ.get("LARAVEL_CLOUD_ENV_NAME"))
    try:
        import db
        db.ping()
        result["db"] = "ok"
    except Exception as exc:
        # Driver messages can contain credentials and connection strings.
        result["db"] = "error: " + type(exc).__name__
    return result


def json_response(status: int, data: object) -> tuple[int, list[tuple[str, str]], bytes]:
    body = json.dumps(data, allow_nan=False).encode()
    return status, [("Content-Type", "application/json"), ("Content-Length", str(len(body)))], body


def invalid_json_constant(value: str):
    raise ValueError(f"invalid JSON constant: {value}")


def handle(method: str, path: str, headers: Mapping[str, str], body: bytes) -> tuple[int, list[tuple[str, str]], bytes]:
    path = urlsplit(path).path
    if len(body) > MAX_BODY:
        return json_response(400, {"error": "body exceeds 64 KiB"})
    data = {}
    if method == "POST":
        # JSON forces a CORS preflight, so other sites cannot trigger dispatches.
        headers = {k.lower(): v for k, v in headers.items()}
        if headers.get("content-type", "").split(";", 1)[0].strip() != "application/json":
            return json_response(415, {"error": "expected application/json"})
        try:
            data = json.loads(body, parse_constant=invalid_json_constant) if body else {}
            if not isinstance(data, dict):
                raise ValueError("expected a JSON object")
        except (ValueError, UnicodeError, RecursionError):
            return json_response(400, {"error": "expected a JSON object"})
    try:
        if method == "GET":
            if path == "/":
                content = INDEX.read_bytes()
                return 200, [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(content)))], content
            if path == "/api/ping":
                return json_response(200, {"ok": True})
            if path == "/api/stats":
                return json_response(200, telemetry.snapshot())
            if path == "/api/env":
                return json_response(200, runtime_env())
            if path == "/api/load":
                runs = [load_snapshot(run) for run in telemetry.store.lrange(RUNS, 0, 19)]
                return json_response(200, {"runs": [run for run in runs if run is not None]})
            if path.startswith("/api/load/"):
                result = load_snapshot(path.removeprefix("/api/load/"))
                return json_response(200, result) if result else json_response(404, {"error": "run not found"})
            if path == "/api/compat":
                raw = telemetry.store.get(PREFIX + "compat:worker")
                worker = json.loads(raw) if raw else {}
                return json_response(200, {"web": compat_probes(), "worker": worker.get("probes"), "worker_at": worker.get("at")})
        elif method == "POST":
            if path == "/api/load":
                return json_response(*start_load(data))
            if path.startswith("/api/load/") and path.endswith("/cancel"):
                run = path[len("/api/load/"):-len("/cancel")]
                key = run_key(run)
                if not telemetry.store.exists(key):
                    return json_response(404, {"error": "run not found"})
                pipe = telemetry.store.pipeline()
                pipe.set(key + ":cancel", "1", ex=TTL)
                pipe.hincrby(key, "revision", 1)
                pipe.hdel(key, "summary")
                pipe.execute()
                return json_response(200, {"ok": True})
            if path == "/api/hold":
                mb = positive_int(data.get("mb"), "mb", 1800)
                seconds = positive_int(data.get("seconds"), "seconds", 600)
                if not HOLD_LOCK.acquire(blocking=False):
                    return json_response(409, {"error": "hold active in this process"})
                hold = uuid.uuid4().hex
                try:
                    threading.Thread(target=hold_memory, args=(mb, seconds), daemon=True).start()
                except Exception:
                    HOLD_LOCK.release()
                    raise
                return json_response(202, {"hold": hold})
            if path == "/api/compat/worker":
                return json_response(202, {"uuid": worker_compat.dispatch().uuid})
            if path == "/api/reset":
                telemetry.reset()
                return json_response(200, {"ok": True})
            if path == "/api/check":
                run_check()
                return json_response(200, {"ok": True})
            kind = path.removeprefix("/api/dispatch/")
            if kind in DISPATCHES:
                return json_response(200, {"uuids": dispatch(kind)})
        return json_response(404, {"error": "not found"})
    except ValidationError as exc:
        return json_response(400, {"error": str(exc)})
    except Exception:
        logging.exception("Request failed: %s %s", method, path)
        return json_response(503, {"error": "service unavailable"})


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def _handle(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if self.headers.get("Transfer-Encoding") or not 0 <= length <= MAX_BODY:
                raise ValueError
        except ValueError:
            self.close_connection = True
            response = json_response(400, {"error": "invalid body length (maximum 64 KiB)"})
        else:
            response = handle(self.command, self.path, dict(self.headers), self.rfile.read(length))
        status, headers, body = response
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass  # the dashboard polls every second; keep the console readable


class DualStackServer(ThreadingHTTPServer):
    """Listens on IPv6 and IPv4. Laravel Cloud's cluster network is IPv6."""

    address_family = socket.AF_INET6

    def server_bind(self) -> None:
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


def self_check_admission() -> None:
    """Exercise real Lua in configured Valkey using isolated, temporary keys.

    Run with the local Redis environment: python app.py --self-check-admission.
    """
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import patch

    store = telemetry.store
    namespace = PREFIX + "self-check:" + uuid.uuid4().hex + ":"
    active, runs, queue = (namespace + name for name in ("active", "runs", "queue"))
    run_prefix = namespace + "run:"
    config = load_options({"kind": "sync", "count": 1})

    def submit(idempotency="", settings=None):
        run = uuid.uuid4().hex
        now = time.time()
        return admit_load(store, [active, run_prefix + run, runs, idempotency, queue],
                          [run, json.dumps(settings or config), now, now + 600, TTL, run_prefix])

    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: submit(), range(8)))
        assert sum(result[0] == "created" for result in results) == 1
        assert sum(result[0] == "run active" for result in results) == 7
        run = store.get(active)
        assert store.llen(runs) == 1
        assert 0 < store.pttl(active) <= 600000
        assert abs(time.time() + store.pttl(active) / 1000 - float(store.hget(run_prefix + run, "deadline"))) < 1
        for state in ("done", "failed", "expired"):
            store.hset(run_prefix + run, "state", state)
            outcome, run = submit()
            assert outcome == "created"
        store.hset(run_prefix + run, "deadline", time.time() - 1)
        outcome, run = submit()
        assert outcome == "created"
        store.delete(run_prefix + run)
        assert submit()[0] == "created"  # Orphaned active key must not block admission.
        store.delete(active)
        idempotency = namespace + "idempotency"
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: submit(idempotency), range(8)))
        assert sum(result[0] == "created" for result in results) == 1
        assert sum(result[0] == "reused" for result in results) == 7
        assert len({result[1] for result in results}) == 1
        run = store.get(active)
        store.hset(run_prefix + run, "config", json.dumps(dict(reversed(list(config.items())))))
        assert submit(idempotency) == ["reused", run]  # Compare values, not JSON key order.
        assert submit(idempotency, {**config, "count": 2}) == ["idempotency key already used", run]
        store.hset(run_prefix + run, "state", "done")
        other = submit()[1]
        assert submit(idempotency) == ["reused", run] and store.get(active) == other
        store.delete(active)
        store.rpush(queue, *(["test"] * 19999))
        store.zadd(queue + ":delayed", {"delayed": 1})
        assert submit()[0] == "created"  # Existing cap is strictly greater than 20000.
        store.delete(active)
        store.zadd(queue + ":reserved", {"reserved": 1})
        assert submit() == ["queue depth exceeds 20000", ""]
        assert not store.exists(active)
        store.delete(queue, queue + ":delayed", queue + ":reserved")
        with patch.object(store, "evalsha", side_effect=NoScriptError("forced cache miss")), \
                patch.object(store, "eval", wraps=store.eval) as fallback:
            assert submit()[0] == "created"
            fallback.assert_called_once()
        assert all(0 < store.ttl(key) <= TTL for key in store.scan_iter(namespace + "*"))
        print("Valkey Lua admission checks passed (concurrency, idempotency, stale active, depth, TTL, EVAL fallback)")
    finally:
        keys = list(store.scan_iter(namespace + "*"))
        if keys:
            store.delete(*keys)


def self_check() -> None:
    """Run contract checks without services: python app.py --self-check."""
    assert handle("GET", "/api/ping?x=1", {}, b"")[0] == 200
    assert handle("POST", "/api/load", {}, b"{}")[0] == 415
    for data in ({"kind": "async", "count": True}, {"kind": "async", "count": 1.0},
                 {"kind": "async", "count": "1"}, {"kind": "async", "count": 10001},
                 {"kind": "async", "count": 1, "ms": float("nan")},
                 {"kind": "async", "count": 1, "ms": 30001},
                 {"kind": "mem", "count": 1, "mb": 1, "ms": 600001},
                 {"kind": "mem", "count": 1, "mb": 1801}):
        assert handle("POST", "/api/load", {"Content-Type": "application/json"}, json.dumps(data).encode())[0] == 400
    assert handle("POST", "/api/load", {"content-type": "application/json"}, b"[]")[0] == 400
    assert handle("POST", "/api/load", {}, b" " * (MAX_BODY + 1))[0] == 400
    assert percentiles([1, 2, 3, 4]) == {"p50": 2, "p95": 4, "p99": 4}
    assert load_options({"kind": "db_async", "count": 1})["rows"] == 10
    assert load_options({"kind": "mem", "count": 1, "mb": 1, "ms": 600000})["ms"] == 600000
    import db
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch
    with patch.dict(os.environ, {"DATABASE_URL": "mysql://u%40x:p%3Ass@remote.example/test", "DB_SSL": "0"}):
        url = db.database_url("pymysql")
        assert url.username == "u@x" and url.password == "p:ss"
        assert url.drivername == "mysql+pymysql"
        assert db.database_url("aiomysql").drivername == "mysql+aiomysql"
        assert db.engine_options(url)["connect_args"]["ssl"].check_hostname
        assert db.engine_options(url, asynchronous=True)["connect_args"]["ssl"].check_hostname
        with patch.dict(os.environ, {"DB_SSL_VERIFY": "0"}):
            assert db.engine_options(url)["connect_args"]["ssl"].verify_mode == ssl.CERT_NONE
        async def engine_reuse() -> None:
            with patch.object(db, "create_async_engine") as create:
                assert db.async_engine(1) is db.async_engine(10)
                assert create.call_count == 1
                assert create.call_args.kwargs["pool_size"] == 5
                assert create.call_args.kwargs["max_overflow"] == 0
        asyncio.run(engine_reuse())
        assert db.engine_options(url)["pool_size"] == 1
    assert SERVER == SERVER_NAME == "stdlib"
    assert load_deadline_seconds(load_options({"kind": "db_sync", "count": 10000, "rows": 1000})) == TTL - 60
    assert load_deadline_seconds(load_options({"kind": "sync", "count": 10000})) == 1120
    store = MagicMock()
    run = "a" * 32
    with patch.dict(telemetry.__dict__, {"store": store}):
        store.hget.return_value = json.dumps({"run": run, "state": "done"})
        assert load_snapshot(run)["state"] == "done"
        store.pipeline.assert_not_called()  # Terminal polling must not fetch full job hashes.
        store.hget.return_value = "corrupt JSON"
        with patch.object(logging, "exception"):
            assert handle("GET", f"/api/load/{run}", {}, b"")[0] == 503
        store.hget.return_value = "draining"
        with patch(__name__ + ".current_job", return_value=SimpleNamespace(uuid="job")), \
                patch(__name__ + ".save_load_record") as save, patch.object(time, "sleep") as sleep:
            load_sync(run, 1)
            record = save.call_args.args[2]
            assert record["ok"] is None and record["skipped"] is True
            sleep.assert_not_called()
        store.hget.return_value = None
        meta = {"config": json.dumps(load_options({"kind": "db_write", "count": 1})),
                "state": "draining", "heartbeat": str(time.time()), "deadline": str(time.time() + 600)}
        store.pipeline.return_value.execute.return_value = [meta, {"job": record["queued_at"]},
                                                            {"job": json.dumps(record)}, ["1"], True]
        with patch.object(db, "cleanup") as cleanup:
            result = load_snapshot(run)
            assert result["skipped"] == 1 and result["processed"] == result["jobs_per_s"] == 0
            assert result["run_ms"] == {"p50": None, "p95": None, "p99": None}
            cleanup.assert_called_once_with(run)
            meta["state"] = "expired"
            load_snapshot(run)
            assert cleanup.call_count == 2
    print("Router contract checks passed")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-check"]:
        self_check()
        raise SystemExit(0)
    if sys.argv[1:] == ["--self-check-admission"]:
        self_check_admission()
        raise SystemExit(0)
    SERVER = SERVER_NAME = os.environ.get("SERVER", "stdlib")
    port = int(os.environ.get("PORT", "8000"))
    if SERVER == "gunicorn":
        print(f"gunicorn workers={os.environ.get('WEB_CONCURRENCY', '1')}", flush=True)
        os.execvp("gunicorn", ["gunicorn", "wsgi:app", "-b", f"[::]:{port}"])
    elif SERVER == "uvicorn":
        args = ["uvicorn", "asgi:app", "--host", "::", "--port", str(port)]
        if "WEB_CONCURRENCY" in os.environ:
            args += ["--workers", os.environ["WEB_CONCURRENCY"]]
        os.execvp("uvicorn", args)
    elif SERVER != "stdlib":
        raise SystemExit("SERVER must be stdlib, gunicorn or uvicorn")
    telemetry.store.ping()  # fail fast without a Valkey cache
    host = os.environ.get("HOST") or ("::" if "PORT" in os.environ else "127.0.0.1")
    server = DualStackServer if ":" in host else ThreadingHTTPServer
    print(f"Dashboard on {host} port {port}", flush=True)
    server((host, port), Handler).serve_forever()
