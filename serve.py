"""Start the web server named in .web-server, or the one given: python serve.py [server]

Cloud start command for every environment: python serve.py
Each server branch differs from main only in .web-server (and .python-version, to test another
Python), so switching server or version is a commit, not a settings change.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Bind [::]:$PORT, dual-stack: Cloud's startup probes connect over IPv6, the pod's nginx over 127.0.0.1.
# uvicorn and gunicorn read WEB_CONCURRENCY themselves; the others get it as a flag. waitress (threads) and daphne have no process count.
COMMANDS = {
    "uvicorn": "uvicorn asgi:app --host :: --port {port}",
    "gunicorn": "gunicorn wsgi:app --bind [::]:{port}",
    # --lazy-apps: import the app in each worker, not once in the master before forking.
    "uwsgi": "uwsgi --http-socket [::]:{port} --module wsgi:app --master --processes {workers} --lazy-apps "
             "--enable-threads --die-on-term --need-app --disable-logging",
    # waitress sets IPV6_V6ONLY on [::], and the pod's nginx connects over 127.0.0.1: listen on both.
    # It also deletes X-Forwarded-* from untrusted proxies; keep them, as the other servers do.
    "waitress": "waitress-serve --listen=[::]:{port} --listen=0.0.0.0:{port} --no-clear-untrusted-proxy-headers wsgi:app",
    "granian-wsgi": "granian --interface wsgi --host :: --port {port} --workers {workers} wsgi:app",
    "granian-asgi": "granian --interface asgi --host :: --port {port} --workers {workers} asgi:app",
    "hypercorn-wsgi": "hypercorn --bind [::]:{port} --workers {workers} wsgi:app",
    "hypercorn-asgi": "hypercorn --bind [::]:{port} --workers {workers} asgi:app",
    "daphne": "daphne --bind :: --port {port} asgi:app",
}
SINGLE_PROCESS = {"waitress", "daphne"}


def command(server: str) -> list[str]:
    port = os.environ.get("PORT", "8000")
    workers = os.environ.get("WEB_CONCURRENCY", "1")
    return COMMANDS[server].format(port=port, workers=workers).split()


if __name__ == "__main__":
    server = sys.argv[1] if len(sys.argv) > 1 else Path(__file__).with_name(".web-server").read_text().strip()
    if server not in COMMANDS:
        raise SystemExit(f"unknown server {server!r}; expected one of {', '.join(COMMANDS)}")
    argv = command(server)
    os.environ["WEB_SERVER"] = server  # asgi.py / wsgi.py report it as app.SERVER
    os.execvp(argv[0], argv)
