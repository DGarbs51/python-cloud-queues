"""ASGI entrypoint for uvicorn. Streaming, uploads, and /ws/echo stay here; other routes call app.handle."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import signal
import socket
import threading
import time
import traceback
import urllib.parse

from laravel_cloud_logging import asgi_middleware

import app as app_module
from app import handle

app_module.SERVER = "uvicorn"
app_module.SERVER_NAME = "uvicorn"

# JSON posts are capped by the core contract. Uploads are streamed and capped apart from that.
MAX_JSON_BODY = 64 * 1024
MAX_UPLOAD_BODY = 512 * 1024 * 1024
MAX_STREAM_EVENTS = 600

_inflight = 0
_inflight_lock = threading.Lock()


def _force_dual_stack() -> None:
    # asyncio sets IPV6_V6ONLY on :: listeners. The stdlib server clears that flag so the
    # Cloud proxy can connect with IPv4 or IPv6; do the same for this process.
    original = socket.socket.setsockopt

    def setsockopt(self, level, optname, value, *args):
        if level == socket.IPPROTO_IPV6 and optname == socket.IPV6_V6ONLY and value:
            value = 0
        return original(self, level, optname, value, *args)

    socket.socket.setsockopt = setsockopt


def log_sigterm() -> None:
    with _inflight_lock:
        current = _inflight
    print(f"sigterm pid={os.getpid()} inflight={current}", flush=True)


def _install_sigterm_log() -> None:
    try:
        import uvicorn.server as server_mod
    except ImportError:
        return
    current = server_mod.Server.handle_exit
    if getattr(current, "_logs_sigterm", False):
        return

    def handle_exit(self, sig, frame):
        if sig == signal.SIGTERM:
            log_sigterm()
        return current(self, sig, frame)

    setattr(handle_exit, "_logs_sigterm", True)
    server_mod.Server.handle_exit = handle_exit


def _enter() -> None:
    global _inflight
    with _inflight_lock:
        _inflight += 1


def _exit() -> None:
    global _inflight
    with _inflight_lock:
        _inflight -= 1


_force_dual_stack()
_install_sigterm_log()


async def app(scope: dict, receive, send) -> None:
    kind = scope["type"]
    if kind == "lifespan":
        await _lifespan(receive, send)
        return
    _enter()
    try:
        if kind == "http":
            await _http(scope, receive, send)
        elif kind == "websocket":
            await _websocket(scope, receive, send)
    except _ClientError as exc:
        if kind == "http":
            await _send_json(send, exc.status, {"error": exc.message}, close=True)
    except Exception:
        traceback.print_exc()
        if kind == "http":
            try:
                await _send_json(send, 500, {"error": "internal error"})
            except Exception:
                pass
    finally:
        _exit()


async def _lifespan(receive, send) -> None:
    while True:
        message = await receive()
        kind = message["type"]
        if kind == "lifespan.startup":
            print(f"lifespan startup pid={os.getpid()}", flush=True)
            await send({"type": "lifespan.startup.complete"})
        elif kind == "lifespan.shutdown":
            # With --workers, uvicorn installs SIGTERM in the child before this
            # module can wrap it. Shutdown still runs here, which is the hook
            # the contract allows for this log line.
            log_sigterm()
            print(f"lifespan shutdown pid={os.getpid()}", flush=True)
            await send({"type": "lifespan.shutdown.complete"})
            return


async def _http(scope: dict, receive, send) -> None:
    method = scope.get("method", "GET")
    path = _path(scope)
    route, _, query = path.partition("?")
    headers = _headers(scope)
    if method == "GET" and route == "/api/stream":
        await _stream(send, query)
        return
    if method == "POST" and route == "/api/upload":
        await _upload(receive, send, headers)
        return
    body = await _json_body(receive, headers, method)
    # handle() is synchronous (Redis, DB, files). Run it off the event loop so
    # a slow core route cannot stall ping, SSE, or websockets.
    status, resp_headers, payload = await asyncio.to_thread(handle, method, path, headers, body)
    if not isinstance(payload, (bytes, bytearray)):
        raise TypeError("handle() body must be bytes")
    await _send(send, status, list(resp_headers), bytes(payload))


async def _stream(send, query: str) -> None:
    parsed = _stream_params(query)
    if parsed is None:
        await _send_json(send, 400, {"error": "invalid stream params"}, close=True)
        return
    seconds, interval = parsed
    await send({"type": "http.response.start", "status": 200, "headers": _encode(_sse_headers())})
    index = 0
    elapsed = 0.0
    while index < MAX_STREAM_EVENTS and elapsed + interval <= seconds + 1e-9:
        await asyncio.sleep(interval)
        elapsed += interval
        await send({"type": "http.response.body", "body": _sse(index), "more_body": True})
        index += 1
    await send({"type": "http.response.body", "body": b"", "more_body": False})


async def _upload(receive, send, headers: dict) -> None:
    length = _content_length(headers.get("content-length"))
    if length == -1:
        await _send_json(send, 400, {"error": "invalid content length"}, close=True)
        return
    if length is not None and length > MAX_UPLOAD_BODY:
        await _send_json(send, 413, {"error": "body too large"}, close=True)
        return
    hasher = hashlib.sha256()
    total = 0
    while True:
        message = await receive()
        if message["type"] != "http.request":
            break
        chunk = message.get("body") or b""
        if not isinstance(chunk, (bytes, bytearray)):
            chunk = b""
        total += len(chunk)
        if total > MAX_UPLOAD_BODY:
            await _send_json(send, 413, {"error": "body too large"}, close=True)
            return
        hasher.update(chunk)
        if not message.get("more_body", False):
            break
    await _send_json(send, 200, {"bytes": total, "sha256": hasher.hexdigest()})


async def _json_body(receive, headers: dict, method: str) -> bytes:
    raw = headers.get("content-length")
    if (raw is None or raw == "") and method not in ("POST", "PUT", "PATCH"):
        return b""
    length = _content_length(raw)
    if length == -1:
        raise _ClientError(400, "invalid content length")
    if length is not None and length > MAX_JSON_BODY:
        raise _ClientError(400, "body too large")
    if length == 0:
        return b""
    chunks = []
    total = 0
    while True:
        message = await receive()
        if message["type"] != "http.request":
            break
        chunk = message.get("body") or b""
        if not isinstance(chunk, (bytes, bytearray)):
            chunk = b""
        total += len(chunk)
        if total > MAX_JSON_BODY:
            raise _ClientError(400, "body too large")
        chunks.append(bytes(chunk))
        if not message.get("more_body", False):
            break
    return b"".join(chunks)


async def _websocket(scope: dict, receive, send) -> None:
    path = scope.get("path") or "/"
    if path != "/ws/echo":
        await send({"type": "websocket.close", "code": 1008})
        return
    await send({"type": "websocket.accept"})
    while True:
        message = await receive()
        kind = message["type"]
        if kind == "websocket.disconnect":
            return
        if kind != "websocket.receive":
            continue
        text = message.get("text")
        data = message.get("bytes")
        if text == "bye" or data == b"bye":
            await send({"type": "websocket.close", "code": 1000})
            return
        if text is not None:
            await send({"type": "websocket.send", "text": text})
        elif data is not None:
            await send({"type": "websocket.send", "bytes": data})


class _ClientError(Exception):
    def __init__(self, status: int, message: str) -> None:
        self.status = status
        self.message = message


def _path(scope: dict) -> str:
    raw = scope.get("raw_path") or b""
    if isinstance(raw, bytes) and raw:
        path = raw.decode("latin1")
    else:
        path = scope.get("path") or "/"
    query = scope.get("query_string") or b""
    if isinstance(query, bytes) and query:
        return path + "?" + query.decode("latin1")
    return path


def _headers(scope: dict) -> dict:
    headers = {}
    for item in scope.get("headers") or ():
        if len(item) != 2:
            continue
        key, value = item
        if isinstance(key, bytes):
            key = key.decode("latin1")
        if isinstance(value, bytes):
            value = value.decode("latin1")
        headers[str(key).lower()] = str(value)
    return headers


def _content_length(raw: str | None) -> int | None:
    if raw is None or raw == "":
        return None
    if len(raw) > 20 or not str(raw).isdigit():
        return -1
    return int(raw)


def _stream_params(query: str) -> tuple | None:
    params = urllib.parse.parse_qs(query, keep_blank_values=True)
    raw_seconds = (params.get("seconds") or [""])[0]
    raw_interval = (params.get("interval") or [""])[0]
    if not re.fullmatch(r"[0-9]+", raw_seconds):
        return None
    if not re.fullmatch(r"[0-9]+(\.[0-9]+)?", raw_interval):
        return None
    seconds = int(raw_seconds)
    interval = float(raw_interval)
    if not 1 <= seconds <= 60:
        return None
    if not 0.1 <= interval <= 10:
        return None
    return seconds, interval


def _sse(index: int) -> bytes:
    return f"data: {json.dumps({'i': index, 't': time.time()})}\n\n".encode()


def _sse_headers() -> list:
    # X-Accel-Buffering asks the Cloud nginx in front of the app not to hold the stream.
    return [
        (b"content-type", b"text/event-stream"),
        (b"cache-control", b"no-cache"),
        (b"x-accel-buffering", b"no"),
        (b"x-content-type-options", b"nosniff"),
    ]


def _encode(headers: list) -> list:
    encoded = []
    for name, value in headers:
        if isinstance(name, str):
            name = name.lower().encode("latin1")
        if isinstance(value, str):
            value = value.encode("latin1")
        encoded.append((name, value))
    return encoded


async def _send(send, status: int, headers: list, body: bytes) -> None:
    await send({"type": "http.response.start", "status": status, "headers": _encode(headers)})
    await send({"type": "http.response.body", "body": body, "more_body": False})


async def _send_json(send, status: int, payload: dict, close: bool = False) -> None:
    body = json.dumps(payload).encode()
    headers = [
        ("Content-Type", "application/json"),
        ("Content-Length", str(len(body))),
        ("X-Content-Type-Options", "nosniff"),
    ]
    if close:
        headers.append(("Connection", "close"))
    await _send(send, status, headers, body)


# Adds Cloud-Request-ID to every log line written during the request.
app = asgi_middleware(app)
