"""WSGI entrypoint for gunicorn. Routes that stream stay here; everything else calls app.handle."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import threading
import time
import traceback
import urllib.parse
from collections.abc import Iterator

from laravel_cloud_logging import wsgi_middleware

import app as app_module
from app import handle

app_module.SERVER = "gunicorn"
app_module.SERVER_NAME = "gunicorn"

# JSON posts are capped by the core contract. Uploads are streamed and capped apart from that.
MAX_JSON_BODY = 64 * 1024
MAX_UPLOAD_BODY = 512 * 1024 * 1024
READ_CHUNK = 64 * 1024
MAX_STREAM_EVENTS = 600

_inflight = 0
_inflight_lock = threading.Lock()


def log_sigterm() -> None:
    with _inflight_lock:
        current = _inflight
    print(f"sigterm pid={os.getpid()} inflight={current}", flush=True)


def _enter() -> None:
    global _inflight
    with _inflight_lock:
        _inflight += 1


def _exit() -> None:
    global _inflight
    with _inflight_lock:
        _inflight -= 1


def app(environ: dict, start_response):
    """WSGI callable. SSE is a generator so each event is one chunk on the wire."""
    _enter()
    handed_off = False
    try:
        status, headers, body, streaming = _dispatch(environ)
        start_response(_status(status), headers)
        if streaming:
            handed_off = True
            return _guard(body)
        return [body]
    except Exception:
        traceback.print_exc()
        payload = json.dumps({"error": "internal error"}).encode()
        try:
            start_response("500 Internal Server Error", _json_headers(payload))
        except Exception:
            return [payload]
        return [payload]
    finally:
        if not handed_off:
            _exit()


def _guard(chunks: Iterator[bytes]) -> Iterator[bytes]:
    try:
        yield from chunks
    finally:
        _exit()


def _dispatch(environ: dict) -> tuple:
    method = environ.get("REQUEST_METHOD", "GET")
    path = _path(environ)
    route, _, query = path.partition("?")
    if method == "GET" and route == "/api/stream":
        return _stream(query)
    if method == "POST" and route == "/api/upload":
        return _upload(environ)
    try:
        body = _json_body(environ, method)
    except _ClientError as exc:
        return _error(exc.status, exc.message)
    status, headers, payload = handle(method, path, _headers(environ), body)
    if not isinstance(payload, (bytes, bytearray)):
        raise TypeError("handle() body must be bytes")
    return status, list(headers), bytes(payload), False


def _stream(query: str) -> tuple:
    parsed = _stream_params(query)
    if parsed is None:
        return _error(400, "invalid stream params")
    seconds, interval = parsed
    return 200, _sse_headers(), _events(seconds, interval), True


def _events(seconds: float, interval: float) -> Iterator[bytes]:
    index = 0
    elapsed = 0.0
    while index < MAX_STREAM_EVENTS and elapsed + interval <= seconds + 1e-9:
        time.sleep(interval)
        elapsed += interval
        yield _sse(index)
        index += 1


def _upload(environ: dict) -> tuple:
    length = _content_length(environ.get("CONTENT_LENGTH"))
    if length == -1:
        return _error(400, "invalid content length")
    if length is not None and length > MAX_UPLOAD_BODY:
        return _error(413, "body too large")
    hasher = hashlib.sha256()
    total = 0
    stream = environ["wsgi.input"]
    remaining = length
    while True:
        if remaining is None:
            chunk = stream.read(READ_CHUNK)
        elif remaining <= 0:
            break
        else:
            chunk = stream.read(min(READ_CHUNK, remaining))
        if not chunk:
            break
        if remaining is not None:
            remaining -= len(chunk)
        total += len(chunk)
        if total > MAX_UPLOAD_BODY:
            return _error(413, "body too large")
        hasher.update(chunk)
    payload = json.dumps({"bytes": total, "sha256": hasher.hexdigest()}).encode()
    return 200, _json_headers(payload), payload, False


def _json_body(environ: dict, method: str) -> bytes:
    raw = environ.get("CONTENT_LENGTH") or ""
    if raw == "" and method not in ("POST", "PUT", "PATCH"):
        return b""
    length = _content_length(raw if raw != "" else None)
    if length == -1:
        raise _ClientError(400, "invalid content length")
    if length is not None and length > MAX_JSON_BODY:
        raise _ClientError(400, "body too large")
    # No declared length: stop one byte past the cap instead of reading forever.
    bounded = length is None
    remaining = MAX_JSON_BODY + 1 if bounded else length
    stream = environ["wsgi.input"]
    chunks = []
    while remaining > 0:
        chunk = stream.read(min(READ_CHUNK, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    body = b"".join(chunks)
    if bounded and len(body) > MAX_JSON_BODY:
        raise _ClientError(400, "body too large")
    return body


class _ClientError(Exception):
    def __init__(self, status: int, message: str) -> None:
        self.status = status
        self.message = message


def _path(environ: dict) -> str:
    path = environ.get("PATH_INFO") or "/"
    query = environ.get("QUERY_STRING") or ""
    if query:
        return f"{path}?{query}"
    return path


def _headers(environ: dict) -> dict:
    headers = {}
    for key, value in environ.items():
        if not isinstance(value, str):
            continue
        if key.startswith("HTTP_"):
            headers[key[5:].replace("_", "-").lower()] = value
        elif key in ("CONTENT_TYPE", "CONTENT_LENGTH"):
            headers[key.replace("_", "-").lower()] = value
    return headers


def _content_length(raw: str | None) -> int | None:
    """Return the declared length, None if absent, or -1 if it is not a safe integer."""
    if raw is None or raw == "":
        return None
    if len(raw) > 20 or not raw.isdigit():
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


def _error(status: int, message: str) -> tuple:
    payload = json.dumps({"error": message}).encode()
    headers = _json_headers(payload)
    headers.append(("Connection", "close"))
    return status, headers, payload, False


def _json_headers(payload: bytes) -> list:
    return [
        ("Content-Type", "application/json"),
        ("Content-Length", str(len(payload))),
        ("X-Content-Type-Options", "nosniff"),
    ]


def _sse_headers() -> list:
    # X-Accel-Buffering asks the Cloud nginx in front of the app not to hold the stream.
    return [
        ("Content-Type", "text/event-stream"),
        ("Cache-Control", "no-cache"),
        ("X-Accel-Buffering", "no"),
        ("X-Content-Type-Options", "nosniff"),
    ]


def _status(status: int) -> str:
    return f"{status} {http.client.responses.get(status, 'Error')}"


# Adds Cloud-Request-ID to every log line written during the request.
app = wsgi_middleware(app)
