"""Plain-Python demo for laravel-cloud-queues.

Web dashboard: uv run python app.py                         (http://127.0.0.1:8000)
Worker:        uv run laravel-cloud-queues work app:registry
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import socket
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from laravel_cloud_queues import Registry, current_job
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


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/":
            self._send(200, INDEX.read_bytes(), "text/html; charset=utf-8")
        elif self.path == "/api/stats":
            self._json(200, telemetry.snapshot())
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:
        # A JSON content type forces a CORS preflight, so other sites cannot trigger dispatches.
        if self.headers.get("Content-Type") != "application/json":
            self._json(415, {"error": "expected application/json"})
            return
        kind = self.path.removeprefix("/api/dispatch/")
        if self.path == "/api/reset":
            telemetry.reset()
            self._json(200, {"ok": True})
        elif self.path == "/api/check":
            run_check()
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


class DualStackServer(ThreadingHTTPServer):
    """Listens on IPv6 and IPv4. Laravel Cloud's cluster network is IPv6."""

    address_family = socket.AF_INET6

    def server_bind(self) -> None:
        self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


if __name__ == "__main__":
    telemetry.store.ping()  # fail fast without a Valkey cache
    # Laravel Cloud sets PORT and proxies to it, so listen on every interface there.
    host = os.environ.get("HOST") or ("::" if "PORT" in os.environ else "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))
    server = DualStackServer if ":" in host else ThreadingHTTPServer
    print(f"Dashboard on {host} port {port}", flush=True)
    server((host, port), Handler).serve_forever()
