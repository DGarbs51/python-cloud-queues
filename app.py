"""Vanilla-Python app for checking that Python works on Laravel Cloud.

Web (ASGI): uvicorn asgi:app --host :: --port $PORT
Web (WSGI): gunicorn wsgi:app --bind [::]:$PORT
Worker:     laravel-cloud-queues work app:registry

Both web entrypoints route through handle() here, so they serve the same pages and checks.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
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
from telemetry import BURST_SIZE, CHECK_DEADLINE, CHECK_KINDS, DELAY_SECONDS, TIMEOUT_SECONDS, Telemetry

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


CHECK_LOCK = "lcq-demo:check-lock"


def run_check() -> bool:
    """Start the queue check at most once per CHECK_DEADLINE across every web process and replica.

    Its timeout case restarts a worker, so repeated starts would keep workers cycling. Within the
    cooldown the page just shows the latest check.
    """
    if not telemetry.store.set(CHECK_LOCK, "1", nx=True, ex=CHECK_DEADLINE):
        return False
    telemetry.save_check({kind: dispatch(kind) for kind in CHECK_KINDS})
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


def handle(method: str, path: str, headers: Mapping[str, str], body: bytes) -> Response:
    """Route one request. Adds X-Request-ID and writes one access log line."""
    started = time.monotonic()
    supplied = next((v for k, v in headers.items() if k.lower() == "x-request-id"), "")
    request_id = supplied if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", supplied) else uuid.uuid4().hex
    path, query = urlsplit(path).path, urlsplit(path).query
    with logs.context(request_id=request_id):
        try:
            status, response_headers, payload = _handle(method, path, query, headers, body)
        except Boom:
            raise
        except Exception:
            log.exception("Request failed: %s %s", method, path)
            status, response_headers, payload = json_response(503, {"error": "service unavailable"})
        # Dashboard polling would drown the log at INFO.
        level = logging.DEBUG if method == "GET" and path == "/api/stats" else logging.INFO
        if status >= 400:
            level = logging.ERROR if status >= 500 else logging.WARNING
        log.log(level, "access", extra=dict(method=method, path=path, status=status, bytes=len(payload),
                                            duration_ms=round((time.monotonic() - started) * 1000, 2)))
    return status, [*response_headers, ("X-Request-ID", request_id)], payload


def _handle(method: str, path: str, query: str, headers: Mapping[str, str], body: bytes) -> Response:
    if len(body) > MAX_BODY:
        return json_response(400, {"error": "body exceeds 64 KiB"})
    if method == "POST":
        # JSON forces a CORS preflight, so other sites cannot trigger dispatches.
        content_type = next((v for k, v in headers.items() if k.lower() == "content-type"), "")
        if content_type.split(";", 1)[0].strip() != "application/json":
            return json_response(415, {"error": "expected application/json"})
        try:
            data = json.loads(body, parse_constant=_reject_constant) if body else {}
        except (ValueError, UnicodeError, RecursionError):
            data = None
        if not isinstance(data, dict):
            return json_response(400, {"error": "expected a JSON object"})
    if method == "GET":
        if path == "/":
            content = INDEX.read_bytes()
            return 200, [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(content)))], content
        if path == "/api/ping":
            return json_response(200, {"ok": True})
        if path == "/api/stats":
            return json_response(200, {**telemetry.snapshot(), "server": SERVER})
        if path == "/api/checks":
            return json_response(200, checks.run(headers))
        if path.startswith("/api/throughput/"):
            return json_response(*throughput.status(path.removeprefix("/api/throughput/")))
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
    return json_response(404, {"error": "not found"})


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

    assert handle("GET", "/api/ping?x=1", {"X-Request-ID": "abc"}, b"") == (
        200, [("Content-Type", "application/json"), ("Content-Length", "12"), ("X-Request-ID", "abc")], b'{"ok": true}')
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
