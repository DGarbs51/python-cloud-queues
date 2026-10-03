"""gunicorn settings for the Cloud app process.

Workers come from WEB_CONCURRENCY (otherwise 1). gunicorn 26 already uses that
variable as its default; assigning it here replaces the default with the same
count and does not multiply it.
"""

from __future__ import annotations

import os
import signal
import socket

import gunicorn.sock
from laravel_cloud_logging import configure

# JSON from the master too. No accesslog: Cloud's nginx already logs each request.
logconfig_dict = configure()

# The stdlib server listens on :: with IPv4 mapped in. gunicorn leaves the
# kernel default, which is v6-only on macOS, so set the flag before bind.
_orig_set_options = gunicorn.sock.TCPSocket.set_options


def _dual_stack(self, sock, bound=False):
    if not bound and sock.family == socket.AF_INET6:
        try:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        except OSError:
            pass
    return _orig_set_options(self, sock, bound=bound)


gunicorn.sock.TCP6Socket.set_options = _dual_stack


def _worker_count(raw: str | None) -> int:
    if raw is None or raw == "":
        return 1
    if len(raw) > 20:
        print("invalid WEB_CONCURRENCY; using 1 worker", flush=True)
        return 1
    try:
        count = int(raw)
    except ValueError:
        print(f"invalid WEB_CONCURRENCY={raw!r}; using 1 worker", flush=True)
        return 1
    if count < 1:
        print(f"invalid WEB_CONCURRENCY={raw!r}; using 1 worker", flush=True)
        return 1
    return count


raw_web_concurrency = os.environ.get("WEB_CONCURRENCY")
workers = _worker_count(raw_web_concurrency)
# app.py listens on 8000 when PORT is unset, then execs gunicorn -b without
# putting PORT in the environment. The file is loaded before -b is applied.
bind = f"[::]:{os.environ.get('PORT') or '8000'}"
graceful_timeout = 30
# The default silence timeout is 30s, which would kill a 60s event stream.
timeout = 120


def on_starting(server) -> None:
    print(
        f"gunicorn workers={server.cfg.workers} WEB_CONCURRENCY={raw_web_concurrency}",
        flush=True,
    )


def post_worker_init(worker) -> None:
    import wsgi

    previous = signal.getsignal(signal.SIGTERM)

    def handle_sigterm(signum, frame) -> None:
        wsgi.log_sigterm()
        if callable(previous):
            previous(signum, frame)

    signal.signal(signal.SIGTERM, handle_sigterm)


def worker_exit(server, worker) -> None:
    # Runs in the master after the worker is gone, which on Cloud is the SIGTERM path.
    print(f"sigterm pid={worker.pid} inflight=0", flush=True)


def on_exit(server) -> None:
    print(f"sigterm pid={os.getpid()} inflight=0", flush=True)
