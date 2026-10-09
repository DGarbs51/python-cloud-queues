"""Vanilla-Python app for checking that Python works on Laravel Cloud.

Web (ASGI): uvicorn asgi:app --host :: --port $PORT
Web (WSGI): gunicorn wsgi:app --bind [::]:$PORT
Worker:     laravel-cloud-queues work app:registry

wsgi.py routes through handle() here; asgi.py has its own async router. Both share the
validation, response and logging helpers below, so they serve the same pages and checks.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import socket
import sys
import threading
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

from laravel_cloud_logging import ALERT, EMERGENCY, NOTICE
from laravel_cloud_queues import Registry, current_job

import checks
import logs
import throughput
from telemetry import BURST_SIZE, CHECK_DEADLINE, CHECK_KINDS, DELAY_SECONDS, KEY_PREFIX, TIMEOUT_SECONDS, Telemetry

# The queue CLI imports app:registry, so a worker process is the one running `work`.
LOG_ROLE = "worker" if "work" in sys.argv else "web"
log = logs.setup(LOG_ROLE)
registry = Registry()
telemetry = Telemetry(registry, "Plain Python")
throughput.install(registry, telemetry, log)
INDEX = Path(__file__).with_name("index.html")
MAX_BODY = 64 * 1024
SERVER = "unknown"  # set by started()

Response = tuple[int, list[tuple[str, str]], bytes]
MARKER = re.compile(r"[A-Za-z0-9-]{1,64}")
# /api/log-test levels. DEBUG is below the default LOG_LEVEL=INFO, so it must not reach Cloud.
LOG_TEST_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "notice": NOTICE, "warning": logging.WARNING,
                   "error": logging.ERROR, "critical": logging.CRITICAL, "alert": ALERT, "emergency": EMERGENCY}
LOG_TEST_UNICODE = "日本語 · émoji 🚀"
STREAM_CHUNKS = 6  # /api/stream sends one a second; web.streaming times when each arrives (#8)
SLOW_MAX = 300  # /api/slow cap in seconds: above Cloud's 60 s HTTP timeout, so the timeout test (#9) can cross it
# /api/upload's own cap (#10): above Cloudflare's 500 MiB edge limit, so the edge is what an upload test hits first.
UPLOAD_MAX = 512 * 1024 * 1024
UPLOAD_CHUNK = 1024 * 1024
CPU_ROUNDS = 100_000  # /api/cpu: chained sha256 rounds, ~20 ms on one core; fixed so every run does the same work
REDIS_HITS = KEY_PREFIX + "redis-hits"  # /api/redis INCRs this


class Boom(Exception):
    """Raised by /api/boom past handle()'s catch-all, so the server's own error path has to log it."""


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


async def adispatch(kind: str) -> list[str]:
    job, delay, count = DISPATCHES[kind]

    async def one() -> str:
        at = time.time()
        receipt = await job.options(delay=delay).dispatch_async()
        await telemetry.aqueued(job.name, receipt.uuid, at, delay)
        return receipt.uuid

    return list(await asyncio.gather(*(one() for _ in range(count))))


CHECK_LOCK = KEY_PREFIX + "check-lock"


def run_check() -> bool:
    """Start the queue check at most once per CHECK_DEADLINE across every web process and replica.

    Its timeout case restarts a worker, so repeated starts would keep workers cycling. Within the
    cooldown the page just shows the latest check.
    """
    if not telemetry.store.set(CHECK_LOCK, "1", nx=True, ex=CHECK_DEADLINE):
        return False
    telemetry.save_check({kind: dispatch(kind) for kind in CHECK_KINDS})
    return True


async def arun_check() -> bool:
    """run_check() for the ASGI path: the same lock, every case dispatched concurrently."""
    if not await telemetry.astore.set(CHECK_LOCK, "1", nx=True, ex=CHECK_DEADLINE):
        return False
    uuids = await asyncio.gather(*(adispatch(kind) for kind in CHECK_KINDS))
    await telemetry.asave_check(dict(zip(CHECK_KINDS, uuids)))
    return True


def started(server: str) -> None:
    """Called once per web worker process by asgi.py / wsgi.py."""
    global SERVER
    SERVER = server
    log.info("startup", extra=dict(server=server, pid=os.getpid(), web_concurrency=os.environ.get("WEB_CONCURRENCY")))


def json_response(status: int, data: object) -> Response:
    body = json.dumps(data, allow_nan=False).encode()
    return status, [("Content-Type", "application/json"), ("Content-Length", str(len(body)))], body


def _reject_constant(value: str):
    raise ValueError(f"invalid JSON constant: {value}")


def header(headers: Mapping[str, str], name: str) -> str:
    return next((v for k, v in headers.items() if k.lower() == name), "")


def request_id(headers: Mapping[str, str]) -> str:
    supplied = header(headers, "x-request-id")
    return supplied if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", supplied) else uuid.uuid4().hex


def failed(method: str, path: str) -> Response:
    """The response for an exception a route didn't handle (Boom escapes before this)."""
    log.exception("Request failed: %s %s", method, path)
    return json_response(503, {"error": "service unavailable"})


def finish(method: str, path: str, response: Response, started: float, request_id: str) -> Response:
    """Write the one access log line and add X-Request-ID. Call inside logs.context(request_id=...)."""
    status, headers, payload = response
    # Dashboard polling would drown the log at INFO.
    level = logging.DEBUG if method == "GET" and path == "/api/stats" else logging.INFO
    if status >= 400:
        level = logging.ERROR if status >= 500 else logging.WARNING
    log.log(level, "access", extra=dict(method=method, path=path, status=status, bytes=len(payload),
                                        duration_ms=round((time.monotonic() - started) * 1000, 2)))
    return status, [*headers, ("X-Request-ID", request_id)], payload


def handle(method: str, path: str, headers: Mapping[str, str], body: bytes, uploaded: int | None = None) -> Response:
    """Route one request (WSGI). Adds X-Request-ID and writes one access log line.

    uploaded: POST /api/upload's body size, counted by asgi.py / wsgi.py instead of passing the body (#10).
    """
    started, rid, (path, query) = time.monotonic(), request_id(headers), urlsplit(path)[2:4]
    with logs.context(request_id=rid):
        try:
            response = _handle(method, path, query, headers, body, uploaded)
        except Boom:
            raise
        except Exception:
            response = failed(method, path)
        return finish(method, path, response, started, rid)


def validate(method: str, path: str, headers: Mapping[str, str], body: bytes, uploaded: int | None) -> Response | dict:
    """Upload counting, the body cap and POST's JSON object, for both routers.

    Returns the response when that ends the request, else the POST body ({} for other methods).
    """
    if method == "POST" and path == "/api/upload" and uploaded is not None:
        # Side-effect free, so any Content-Type is fine: the upload limit test (#10) posts raw bytes.
        if uploaded < 0:
            return json_response(400, {"error": "invalid Content-Length"})
        if uploaded > UPLOAD_MAX:
            return json_response(413, {"error": f"body exceeds {UPLOAD_MAX // 2**20} MiB"})
        return json_response(200, {"bytes": uploaded})
    if len(body) > MAX_BODY:
        return json_response(400, {"error": "body exceeds 64 KiB"})
    if method == "POST":
        # JSON forces a CORS preflight, so other sites cannot trigger dispatches.
        content_type = header(headers, "content-type")
        if content_type.split(";", 1)[0].strip() != "application/json":
            return json_response(415, {"error": "expected application/json"})
        try:
            data = json.loads(body, parse_constant=_reject_constant) if body else {}
        except (ValueError, UnicodeError, RecursionError):
            data = None
        if not isinstance(data, dict):
            return json_response(400, {"error": "expected a JSON object"})
        return data
    return {}


def slow_seconds(query: str) -> int | None:
    seconds = next((v for k, v in parse_qsl(query) if k == "seconds"), "")
    return int(seconds) if seconds.isdigit() and 1 <= int(seconds) <= SLOW_MAX else None


SLOW_INVALID = json_response(400, {"error": f"seconds must be 1-{SLOW_MAX}"})


def _handle(method: str, path: str, query: str, headers: Mapping[str, str], body: bytes, uploaded: int | None) -> Response:
    data = validate(method, path, headers, body, uploaded)
    if isinstance(data, tuple):
        return data
    if method == "GET":
        if path == "/api/slow":
            # Blocks this worker, like a real slow request (#7, #9). asgi.py awaits asyncio.sleep instead.
            if (seconds := slow_seconds(query)) is None:
                return SLOW_INVALID
            time.sleep(seconds)
            return json_response(200, {"ok": True, "slept": seconds, **instance()})
        if path == "/api/stats":
            return json_response(200, {**telemetry.snapshot(), "server": SERVER})
        if path == "/api/redis":
            return json_response(200, {"ok": True, "hits": telemetry.store.incr(REDIS_HITS), **instance()})
        if path == "/api/checks":
            return json_response(200, checks.run(headers))
        if path == "/api/packages":
            return json_response(200, checks.inventory())
        if path.startswith("/api/throughput/"):
            return json_response(*throughput.status(path.removeprefix("/api/throughput/")))
    elif method == "POST":
        if path == "/api/check":
            if not run_check():
                return json_response(409, {"error": "a queue check is already running"})
            return json_response(200, {"ok": True})
        if path == "/api/throughput":
            return json_response(*throughput.start(data))
        if path == "/api/suite-results":
            return json_response(*checks.save_suite(data))
        if path == "/api/reset":
            telemetry.reset()
            return json_response(200, {"ok": True})
    return route(method, path, query)


def route(method: str, path: str, query: str) -> Response:
    """The routes with no I/O to wait on, for both routers; 404 for anything else."""
    if method == "GET":
        if path == "/":
            content = INDEX.read_bytes()
            return 200, [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(content)))], content
        if path == "/api/ping":
            return json_response(200, {"ok": True, **instance()})
        if path == "/api/cpu":
            # Holds the worker (and the event loop on ASGI) for fixed CPU work: the load tests' CPU-bound route.
            digest = b"lcq"
            for _ in range(CPU_ROUNDS):
                digest = hashlib.sha256(digest).digest()
            return json_response(200, {"ok": True, "rounds": CPU_ROUNDS, "digest": digest.hex(), **instance()})
        if path in ("/api/log-test", "/api/boom", "/api/boom-thread"):
            marker = next((v for k, v in parse_qsl(query) if k == "marker"), "")
            if not MARKER.fullmatch(marker):
                return json_response(400, {"error": "marker must be 1-64 letters, digits or -"})
            if path == "/api/log-test":
                log_test(marker)
            elif path == "/api/boom":
                _boom_outer(marker)
            else:
                thread = threading.Thread(target=_boom_outer, args=(marker,), name="boom-thread")
                thread.start()
                thread.join()
            return json_response(200, {"ok": True, "marker": marker})
    return json_response(404, {"error": "not found"})


def instance() -> dict:
    """Which deployment, instance and worker answered: DEPLOY_MARKER is set before each test deploy (#7)."""
    return {"deploy": os.environ.get("DEPLOY_MARKER", ""), "pod": socket.gethostname(), "pid": os.getpid()}


def stream_headers(query: str) -> list[tuple[str, str]]:
    """GET /api/stream: server-sent events, no Content-Length. ?accel=no adds nginx's per-response buffering opt-out."""
    headers = [("Content-Type", "text/event-stream")]
    if ("accel", "no") in parse_qsl(query):
        headers.append(("X-Accel-Buffering", "no"))
    return headers


def stream_chunk(i: int) -> bytes:
    return f"data: {json.dumps(dict(i=i, sent=round(time.time(), 3)))}\n\n".encode()


def log_test(marker: str) -> None:
    """One line per level, plus context, multi-line, non-English and a chained exception. Used by logs_check.py (#17)."""
    for name, level in LOG_TEST_LEVELS.items():
        log.log(level, "log-test %s", name, extra=dict(marker=marker, case=name))
    log.info("log-test extra", extra=dict(marker=marker, case="extra", order={"id": 42, "items": ["a", "b"]}))
    log.info("log-test multi-line\nsecond line\nthird line", extra=dict(marker=marker, case="multiline"))
    log.info("log-test unicode %s", LOG_TEST_UNICODE, extra=dict(marker=marker, case="unicode"))
    try:
        try:
            raise KeyError("inner cause")
        except KeyError as exc:
            raise ValueError("log-test outer") from exc
    except ValueError:
        log.exception("log-test exception", extra=dict(marker=marker, case="exception"))


def _boom_outer(marker: str) -> None:
    _boom_middle(marker)


def _boom_middle(marker: str) -> None:
    _boom_inner(marker)


def _boom_inner(marker: str) -> None:
    raise Boom(f"uncaught boom {marker}")


def self_check() -> None:
    """Router contract without Redis: run with `python app.py --self-check`."""
    from unittest.mock import patch

    with patch.dict(os.environ, DEPLOY_MARKER="m-1"):
        status, headers, payload = handle("GET", "/api/ping?x=1", {"X-Request-ID": "abc"}, b"")
    assert status == 200 and ("X-Request-ID", "abc") in headers, headers
    assert json.loads(payload) == {"ok": True, "deploy": "m-1", "pod": socket.gethostname(), "pid": os.getpid()}
    with patch("time.sleep") as sleep:
        status, _, payload = handle("GET", "/api/slow?seconds=2", {}, b"")
        assert status == 200 and json.loads(payload)["slept"] == 2 and sleep.call_args.args == (2,)
        for bad in ("0", "301", "-1", "1.5", "x", ""):
            assert handle("GET", f"/api/slow?seconds={bad}", {}, b"")[0] == 400, bad
        assert sleep.call_count == 1
    upload = {"Content-Type": "application/octet-stream"}
    status, _, payload = handle("POST", "/api/upload", upload, b"", uploaded=5)
    assert status == 200 and json.loads(payload) == {"bytes": 5}, payload
    assert handle("POST", "/api/upload", upload, b"", uploaded=UPLOAD_MAX + 1)[0] == 413
    assert handle("POST", "/api/upload", upload, b"", uploaded=-1)[0] == 400
    assert handle("POST", "/api/upload", upload, b"x")[0] == 415  # only the entrypoints' count reaches the route
    assert handle("GET", "/", {}, b"")[0] == 200
    assert handle("GET", "/nope", {}, b"")[0] == 404
    assert handle("POST", "/api/check", {"Content-Type": "text/plain"}, b"{}")[0] == 415
    for bad in (b"[]", b"NaN", b"{", b"\xff"):
        assert handle("POST", "/api/check", {"Content-Type": "application/json"}, bad)[0] == 400, bad
    assert handle("POST", "/api/check", {"Content-Type": "application/json"}, b"x" * (MAX_BODY + 1))[0] == 400
    assert handle("GET", "/", {"X-Request-ID": "bad id!"}, b"")[1][-1][1] != "bad id!"
    with patch.object(telemetry, "snapshot", side_effect=RuntimeError("redis down")):
        assert handle("GET", "/api/stats", {}, b"")[0] == 503
    with patch(__name__ + ".run_check", return_value=True):
        assert handle("POST", "/api/check", {"content-type": "application/json"}, b"")[0] == 200
    with patch(__name__ + ".run_check", return_value=False):
        assert handle("POST", "/api/check", {"Content-Type": "application/json"}, b"{}")[0] == 409
    assert handle("POST", "/api/dispatch/timeout", {"Content-Type": "application/json"}, b"{}")[0] == 404
    assert handle("GET", "/api/log-test?marker=bad marker!", {}, b"")[0] == 400
    assert handle("GET", "/api/log-test?marker=m-1", {}, b"")[0] == 200
    try:
        handle("GET", "/api/boom?marker=m-1", {}, b"")
        raise AssertionError("/api/boom must escape handle()")
    except Boom:
        pass
    assert logs.CONTEXT.get() == {}
    print("Router contract checks passed")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-check"]:
        logs.self_check()
        self_check()
        raise SystemExit(0)
    raise SystemExit(__doc__)
