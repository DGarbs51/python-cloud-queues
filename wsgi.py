"""WSGI entrypoint: gunicorn wsgi:app --bind [::]:$PORT (settings in gunicorn.conf.py).

Other WSGI servers: see serve.py.
"""

from __future__ import annotations

import http.client
import os
import time

from laravel_cloud_logging import wsgi_middleware

import app as app_module
import checks

app_module.started(os.environ.get("WEB_SERVER", "gunicorn"))


def app(environ: dict, start_response):
    if environ.get("REQUEST_METHOD") == "GET" and environ.get("PATH_INFO") == "/api/stream":
        start_response("200 OK", app_module.stream_headers(environ.get("QUERY_STRING", "")))
        return _stream()
    try:
        length = int(environ.get("CONTENT_LENGTH") or 0)
    except ValueError:
        length = -1
    chunked = "chunked" in environ.get("HTTP_TRANSFER_ENCODING", "").lower()
    uploaded = None
    if environ.get("REQUEST_METHOD") == "POST" and environ.get("PATH_INFO") == "/api/upload":
        uploaded, length, body = _count_upload(environ, length, chunked), 0, b""
    elif not environ.get("CONTENT_LENGTH") and (environ.get("wsgi.input_terminated") or chunked):
        # Chunked body: no length up front, so read one byte past the cap to detect oversize.
        # gunicorn sets wsgi.input_terminated; hypercorn buffers the body without saying so.
        body = environ["wsgi.input"].read(app_module.MAX_BODY + 1)
        # uWSGI can't de-chunk into wsgi.input and reads b"": reject rather than treat it as empty.
        length = len(body) if body or not chunked else -1
    else:
        body = None
    if not 0 <= length <= app_module.MAX_BODY:
        status, headers, body = app_module.json_response(400, {"error": "invalid body length (maximum 64 KiB)"})
    else:
        path = environ.get("PATH_INFO") or "/"
        if environ.get("QUERY_STRING"):
            path += "?" + environ["QUERY_STRING"]
        headers = {key[5:].replace("_", "-").title(): value for key, value in environ.items() if key.startswith("HTTP_")}
        if environ.get("CONTENT_TYPE"):
            headers["Content-Type"] = environ["CONTENT_TYPE"]
        if body is None:
            body = environ["wsgi.input"].read(length) if length else b""
        # The address nginx connected from: ::1 over IPv6, 127.0.0.1 over IPv4.
        checks.PEER.set(environ.get("REMOTE_ADDR", ""))
        status, headers, body = app_module.handle(environ.get("REQUEST_METHOD", "GET"), path, headers, body, uploaded)
    start_response(f"{status} {http.client.responses.get(status, 'Error')}", headers)
    return [body]


def _count_upload(environ: dict, length: int, chunked: bool) -> int:
    """POST /api/upload: read the body in chunks and count it without keeping it (#10). Stops past UPLOAD_MAX."""
    if length < 0 or length > app_module.UPLOAD_MAX:
        return length  # rejected by handle() without reading
    sized = bool(environ.get("CONTENT_LENGTH"))  # else chunked: read until the server signals the end
    count = 0
    while count <= app_module.UPLOAD_MAX and (not sized or count < length):
        chunk = environ["wsgi.input"].read(min(app_module.UPLOAD_CHUNK, length - count) if sized else app_module.UPLOAD_CHUNK)
        if not chunk:
            break
        count += len(chunk)
    # uWSGI can't de-chunk into wsgi.input and reads b"": reject rather than report a truncated body.
    return -1 if chunked and not sized and not count else count


def _stream():
    """GET /api/stream: one chunk a second from a generator. Used by the streaming check."""
    for i in range(app_module.STREAM_CHUNKS):
        if i:
            time.sleep(1)
        yield app_module.stream_chunk(i)


# Adds Cloud-Request-ID to every log line written during the request.
app = wsgi_middleware(app)
