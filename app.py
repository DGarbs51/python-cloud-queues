"""Plain-Python demo for laravel-cloud-queues (Redis backend).

Web dashboard: uv run python app.py                         (http://127.0.0.1:8000)
Worker:        uv run laravel-cloud-queues work app:registry

The package emits no lifecycle events in redis mode, so jobs record their own telemetry
into Redis under ``lcq-demo:``; queue depth is read from the package's Redis keys.
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import random
import socket
import time
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import redis

from laravel_cloud_queues import Registry, __version__, current_job

registry = Registry()

EVENTS = "lcq-demo:events"
STATS = "lcq-demo:stats"
KEEP_EVENTS = 200
INDEX = Path(__file__).with_name("index.html")


@functools.cache
def store() -> redis.Redis:
    if registry.config.redis is None:
        raise SystemExit("Set LARAVEL_CLOUD_QUEUES_BACKEND=redis; this demo targets Redis.")
    return redis.Redis.from_url(registry.config.redis.url, decode_responses=True)


def record(event: str, **fields: object) -> None:
    entry = {"event": event, "at": time.time(), **fields}
    pipe = store().pipeline()
    pipe.lpush(EVENTS, json.dumps(entry))
    pipe.ltrim(EVENTS, 0, KEEP_EVENTS - 1)
    pipe.hincrby(STATS, event, 1)
    pipe.execute()


@contextmanager
def tracked() -> Iterator[None]:
    """Record started/processed/released/failed for the current delivery."""
    job = current_job()
    base = {
        "job": job.job_name,
        "uuid": job.uuid,
        "attempt": job.attempt,
        "worker": f"{socket.gethostname()}:{os.getpid()}",
    }
    record("started", **base)
    start = time.monotonic()
    try:
        yield
    except Exception as exc:
        final = job.attempt >= job.max_tries
        ms = round((time.monotonic() - start) * 1000)
        record("failed" if final else "released", **base, ms=ms, error=str(exc)[:200])
        raise
    record("processed", **base, ms=round((time.monotonic() - start) * 1000))


@registry.job(name="demo.quick")
def quick() -> None:
    with tracked():
        time.sleep(random.uniform(0.05, 0.3))


@registry.job(name="demo.async")
async def async_job() -> None:
    with tracked():
        await asyncio.sleep(random.uniform(0.05, 0.3))


@registry.job(name="demo.slow")
def slow() -> None:
    with tracked():
        time.sleep(3)


@registry.job(name="demo.flaky", tries=3, backoff=[2])
def flaky() -> None:
    with tracked():
        if current_job().attempt == 1:
            raise RuntimeError("flaky job fails on its first attempt")


@registry.job(name="demo.failing", tries=2, backoff=[1])
def failing() -> None:
    with tracked():
        raise RuntimeError("this job always fails")


DISPATCHES = {  # kind: (job, delay seconds, how many)
    "quick": (quick, 0, 1),
    "async": (async_job, 0, 1),
    "slow": (slow, 0, 1),
    "delayed": (quick, 5, 1),
    "flaky": (flaky, 0, 1),
    "failing": (failing, 0, 1),
    "burst": (quick, 0, 25),
}


def dispatch(kind: str) -> list[str]:
    job, delay, count = DISPATCHES[kind]
    uuids = []
    for _ in range(count):
        at = time.time()
        receipt = job.options(delay=delay).dispatch()
        record("queued", at=at, job=job.name, uuid=receipt.uuid, delay=delay)
        uuids.append(receipt.uuid)
    return uuids


def snapshot() -> dict[str, object]:
    cfg = registry.config.redis
    pending = f"{cfg.prefix}queues:{cfg.queue}"
    pipe = store().pipeline()
    pipe.llen(pending)
    pipe.zcard(f"{pending}:delayed")
    pipe.zcard(f"{pending}:reserved")
    pipe.hgetall(STATS)
    pipe.lrange(EVENTS, 0, 99)
    ready, delayed, reserved, counts, events = pipe.execute()
    parsed = sorted((json.loads(e) for e in events), key=lambda e: e["at"], reverse=True)
    return {
        "framework": "Plain Python",
        "version": __version__,
        "queue": cfg.queue,
        "depth": {"ready": ready, "delayed": delayed, "reserved": reserved},
        "counts": {k: int(v) for k, v in counts.items()},
        "events": parsed,
    }


def reset() -> None:
    store().delete(EVENTS, STATS)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/":
            self._send(200, INDEX.read_bytes(), "text/html; charset=utf-8")
        elif self.path == "/api/stats":
            self._json(200, snapshot())
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        # A JSON content type forces a CORS preflight, so other sites cannot trigger dispatches.
        if self.headers.get("Content-Type") != "application/json":
            self._json(415, {"error": "expected application/json"})
            return
        kind = self.path.removeprefix("/api/dispatch/")
        if self.path == "/api/reset":
            reset()
            self._json(200, {"ok": True})
        elif kind in DISPATCHES:
            self._json(200, {"uuids": dispatch(kind)})
        else:
            self._json(404, {"error": "not found"})

    def _json(self, status: int, body: object) -> None:
        self._send(status, json.dumps(body).encode(), "application/json")

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass  # the dashboard polls every second; keep the console readable


if __name__ == "__main__":
    store()  # fail fast on a missing backend
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))
    print(f"Dashboard on http://{host}:{port}")
    ThreadingHTTPServer((host, port), Handler).serve_forever()
