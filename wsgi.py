"""WSGI entrypoint: gunicorn wsgi:app --bind [::]:$PORT (settings in gunicorn.conf.py)."""

from __future__ import annotations

import http.client

from laravel_cloud_logging import wsgi_middleware

import app as app_module

app_module.started("gunicorn")


def app(environ: dict, start_response):
    try:
        length = int(environ.get("CONTENT_LENGTH") or 0)
    except ValueError:
        length = -1
    if not 0 <= length <= app_module.MAX_BODY:
        status, headers, body = app_module.json_response(400, {"error": "invalid body length (maximum 64 KiB)"})
    else:
        path = environ.get("PATH_INFO") or "/"
        if environ.get("QUERY_STRING"):
            path += "?" + environ["QUERY_STRING"]
        headers = {key[5:].replace("_", "-").title(): value for key, value in environ.items() if key.startswith("HTTP_")}
        if environ.get("CONTENT_TYPE"):
            headers["Content-Type"] = environ["CONTENT_TYPE"]
        body = environ["wsgi.input"].read(length) if length else b""
        status, headers, body = app_module.handle(environ.get("REQUEST_METHOD", "GET"), path, headers, body)
    start_response(f"{status} {http.client.responses.get(status, 'Error')}", headers)
    return [body]


# Adds Cloud-Request-ID to every log line written during the request.
app = wsgi_middleware(app)
