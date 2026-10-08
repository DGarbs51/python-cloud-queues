"""ASGI entrypoint: uvicorn asgi:app --host :: --port $PORT (other ASGI servers: see serve.py)

Async-first: every route runs on the event loop and awaits its I/O (redis.asyncio, async queue
dispatch, the async checks). Validation, responses and the access log are shared with wsgi.py
through app.py.
"""

from __future__ import annotations

import asyncio
import collections
import os
import sys
import time
from collections.abc import Mapping
from urllib.parse import urlsplit

from laravel_cloud_logging import asgi_middleware

import app as app_module
import checks
import logs
import throughput
from app import Response, json_response

app_module.started(os.environ.get("WEB_SERVER", "uvicorn"))

IO_TIMEOUT = 10  # asyncio.timeout around each route's Redis and queue calls
LAG_INTERVAL = 0.1
LAG = collections.deque(maxlen=600)  # the last minute of asyncio.sleep(0.1) overshoots, in ms
TASKS: set[asyncio.Task] = set()  # background tasks, cancelled on shutdown
START_LOCK = asyncio.Lock()
STARTED = False


async def app(scope: dict, receive, send) -> None:
    if scope["type"] == "lifespan":
        await _lifespan(receive, send)
        return
    if scope["type"] == "websocket":
        await _websocket(scope, receive, send)
        return
    if scope["type"] != "http":
        return
    if scope["method"] == "GET" and scope["path"] == "/api/stream":
        await _stream(scope, send)
        return
    if not STARTED:
        await _startup()  # daphne has no lifespan support: the first request opens the clients instead
    # The address nginx connected from: ::1 when it reaches the app over IPv6, 127.0.0.1 over IPv4.
    checks.PEER.set((scope.get("client") or ("",))[0])
    body, too_large, uploaded = b"", False, None
    if scope["method"] == "POST" and scope["path"] == "/api/upload":
        # Count the body without keeping it, so a big upload can't push the worker out of memory (#10).
        uploaded = 0
        while uploaded <= app_module.UPLOAD_MAX:
            message = await receive()
            uploaded += len(message.get("body", b""))
            if not message.get("more_body"):
                break
    while uploaded is None:
        message = await receive()
        body += message.get("body", b"")
        if len(body) > app_module.MAX_BODY:
            too_large = True
            break
        if not message.get("more_body"):
            break
    if too_large:
        status, headers, payload = json_response(400, {"error": "body exceeds 64 KiB"})
    else:
        path = scope["path"] + ("?" + scope["query_string"].decode("latin-1") if scope.get("query_string") else "")
        headers = {k.decode("latin-1"): v.decode("latin-1") for k, v in scope["headers"]}
        # A client that hangs up cancels its request, so an abandoned /api/slow stops holding a slot.
        handler = asyncio.create_task(handle(scope["method"], path, headers, body, uploaded))
        disconnect = asyncio.create_task(_disconnected(receive))
        try:
            await asyncio.wait((handler, disconnect), return_when=asyncio.FIRST_COMPLETED)
        finally:
            disconnect.cancel()
            if not handler.done():
                handler.cancel()
        if not handler.done():
            return
        status, headers, payload = handler.result()  # re-raises Boom for the server's error path
    await send({"type": "http.response.start", "status": status,
                "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers]})
    await send({"type": "http.response.body", "body": payload})


async def _disconnected(receive) -> None:
    while (await receive())["type"] != "http.disconnect":
        pass


async def handle(method: str, path: str, headers: Mapping[str, str], body: bytes, uploaded: int | None) -> Response:
    """app.handle() for the event loop: same request ID, error handling and access log line."""
    started, rid, (path, query) = time.monotonic(), app_module.request_id(headers), urlsplit(path)[2:4]
    with logs.context(request_id=rid):
        try:
            response = await _handle(method, path, query, headers, body, uploaded)
        except app_module.Boom:
            raise
        except Exception:
            response = app_module.failed(method, path)
        return app_module.finish(method, path, response, started, rid)


async def _handle(method: str, path: str, query: str, headers: Mapping[str, str], body: bytes,
                  uploaded: int | None) -> Response:
    data = app_module.validate(method, path, headers, body, uploaded)
    if isinstance(data, tuple):
        return data
    telemetry = app_module.telemetry
    if method == "GET":
        if path == "/api/slow":
            # Holds the request, not the worker: the loop serves others meanwhile, like a slow upstream (#7, #9).
            if (seconds := app_module.slow_seconds(query)) is None:
                return app_module.SLOW_INVALID
            await asyncio.sleep(seconds)
            return json_response(200, {"ok": True, "slept": seconds, **app_module.instance()})
        if path == "/api/stats":
            async with asyncio.timeout(IO_TIMEOUT):
                stats = await telemetry.asnapshot()
            return json_response(200, {**stats, "server": app_module.SERVER, "loop_lag_ms": loop_lag()})
        if path == "/api/redis":
            async with asyncio.timeout(IO_TIMEOUT):
                hits = await telemetry.astore.incr(app_module.REDIS_HITS)
            return json_response(200, {"ok": True, "hits": hits, **app_module.instance()})
        if path == "/api/checks":
            return json_response(200, await checks.arun(headers))  # per-check timeouts live in checks
        if path == "/api/packages":
            # Deliberately off the loop: importlib.metadata reads every installed package from disk.
            return json_response(200, await asyncio.to_thread(checks.inventory))
        if path.startswith("/api/throughput/"):
            async with asyncio.timeout(IO_TIMEOUT):
                return json_response(*await throughput.astatus(path.removeprefix("/api/throughput/")))
    elif method == "POST":
        if path == "/api/check":
            async with asyncio.timeout(IO_TIMEOUT):
                if not await app_module.arun_check():
                    return json_response(409, {"error": "a queue check is already running"})
            return json_response(200, {"ok": True})
        if path == "/api/throughput":
            async with asyncio.timeout(IO_TIMEOUT):
                return json_response(*await throughput.astart(data))
        if path == "/api/suite-results":
            async with asyncio.timeout(IO_TIMEOUT):
                return json_response(*await checks.asave_suite(data))
        if path == "/api/reset":
            async with asyncio.timeout(IO_TIMEOUT):
                await telemetry.areset()
            return json_response(200, {"ok": True})
    return app_module.route(method, path, query)


async def _lifespan(receive, send) -> None:
    await receive()  # lifespan.startup
    try:
        await _startup()
    except Exception as exc:
        app_module.log.exception("startup failed")
        await send({"type": "lifespan.startup.failed", "message": str(exc)})
        return
    await send({"type": "lifespan.startup.complete"})
    await receive()  # lifespan.shutdown
    # Stop the lag probe and any throughput dispatch before closing the clients they write through.
    tasks = [*TASKS, *throughput._tasks]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await app_module.telemetry.aclose()
    await checks.aclose()
    await send({"type": "lifespan.shutdown.complete"})


async def _startup() -> None:
    """Open this worker's Redis, database and HTTP clients on its loop, and start the loop-lag probe."""
    global STARTED
    async with START_LOCK:
        if not STARTED:
            await app_module.telemetry.aopen()
            await checks.aopen()
            TASKS.add(asyncio.create_task(_probe_lag()))
            STARTED = True


async def _probe_lag() -> None:
    while True:
        started = time.monotonic()
        await asyncio.sleep(LAG_INTERVAL)
        LAG.append((time.monotonic() - started - LAG_INTERVAL) * 1000)


def loop_lag() -> dict | None:
    """p50/p99 of how late asyncio.sleep(0.1) woke over the last minute: how long the loop was blocked."""
    if not LAG:
        return None
    return {"p50": round(throughput.percentile(LAG, 50), 2), "p99": round(throughput.percentile(LAG, 99), 2)}


async def _stream(scope: dict, send) -> None:
    """GET /api/stream: one chunk a second, each its own http.response.body. Used by the streaming check."""
    headers = app_module.stream_headers(scope.get("query_string", b"").decode("latin-1"))
    await send({"type": "http.response.start", "status": 200,
                "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers]})
    for i in range(app_module.STREAM_CHUNKS):
        if i:
            await asyncio.sleep(1)
        await send({"type": "http.response.body", "body": app_module.stream_chunk(i), "more_body": True})
    await send({"type": "http.response.body", "body": b""})


async def _websocket(scope: dict, receive, send) -> None:
    """GET /ws/echo: echo text messages until the client closes. Used by the WebSocket check."""
    if (await receive())["type"] != "websocket.connect":
        return
    if scope["path"] != "/ws/echo":
        await send({"type": "websocket.close", "code": 1008})
        return
    await send({"type": "websocket.accept"})
    while (message := await receive())["type"] == "websocket.receive":
        await send({"type": "websocket.send", "text": message.get("text") or ""})


# Adds Cloud-Request-ID to every log line written during the request.
app = asgi_middleware(app)


def self_check() -> None:
    """Async router contract without Redis: run with `python asgi.py --self-check`."""
    import logging
    from unittest.mock import patch

    from laravel_cloud_logging import cloud_request_id

    global STARTED
    STARTED = True  # skip opening clients: no Redis here
    seen = []

    class Capture(logging.Handler):
        def emit(self, record):
            seen.append((record.getMessage(), logs.CONTEXT.get().get("request_id"), cloud_request_id.get()))

    async def child(i: int) -> None:
        await asyncio.sleep(0)
        app_module.log.info("child %s", i)

    async def snapshot() -> dict:
        await asyncio.gather(child(1), asyncio.create_task(child(2)))
        return {}

    async def call(path: str, hang_up: float = 3600) -> int | None:
        messages, sent = [{"type": "http.request", "body": b""}], []

        async def receive():
            if messages:
                return messages.pop()
            await asyncio.sleep(hang_up)
            return {"type": "http.disconnect"}

        async def send(message):
            sent.append(message)

        path, _, query = path.partition("?")
        scope = {"type": "http", "method": "GET", "path": path, "query_string": query.encode(), "client": ("::1", 1),
                 "headers": [(b"cloud-request-id", b"cr-1"), (b"x-request-id", b"r-1")]}
        await app(scope, receive, send)
        return sent[0]["status"] if sent else None

    async def main() -> None:
        handler = Capture()
        logging.getLogger().addHandler(handler)
        try:
            with patch.object(app_module.telemetry, "asnapshot", snapshot):
                assert await call("/api/stats") == 200
        finally:
            logging.getLogger().removeHandler(handler)
        children = [s for s in seen if s[0].startswith("child")]
        assert sorted(children) == [("child 1", "r-1", "cr-1"), ("child 2", "r-1", "cr-1")], seen
        started = time.monotonic()
        assert await asyncio.gather(*(call("/api/slow?seconds=1") for _ in range(100))) == [200] * 100
        assert time.monotonic() - started < 1.5, "/api/slow must wait on the loop, not a thread"
        started = time.monotonic()
        assert await call("/api/slow?seconds=5", hang_up=0.1) is None  # cancelled, nothing sent
        assert time.monotonic() - started < 1
        assert await call("/api/cpu") == 200 and await call("/nope") == 404

    asyncio.run(main())
    print("Async router checks passed")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-check"]:
        self_check()
        raise SystemExit(0)
    raise SystemExit(__doc__)
