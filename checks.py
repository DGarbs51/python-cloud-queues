"""Platform checks behind GET /api/checks.

Each check is a small function returning (status, detail, help); CHECKS lists them in display order.
run() never raises: a check that throws becomes a fail row, every detail is scrubbed of credentials,
and the checks run in parallel threads so the whole call stays well under 15 s.
"""

from __future__ import annotations

import contextvars
import os
import re
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import unquote, urlsplit

import certifi

TIMEOUT = 5
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
CLOUD_ONLY = "Only checked on Laravel Cloud."
# asgi.py sets this to the measured seconds for 50 concurrent 0.2 s sleeps; unset means WSGI.
LOOP_SECONDS: contextvars.ContextVar[float | None] = contextvars.ContextVar("loop_seconds", default=None)
USERINFO = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^\s/@]*@")

Result = tuple[str, str, str]


def on_cloud() -> bool:
    return os.environ.get("LARAVEL_CLOUD") == "1"


def _scrub(text: str) -> str:
    text = USERINFO.sub(r"\1", text)
    for name in ("DATABASE_URL", "REDIS_URL"):
        password = urlsplit(os.environ.get(name) or "").password
        for secret in (password, unquote(password or "")):
            if secret:
                text = text.replace(secret, "***")
    return text


def _redis():
    """The telemetry Redis client with bounded timeouts, or None when no Valkey is attached."""
    import app

    try:
        client = app.telemetry.store
    except RuntimeError:
        return None
    client.connection_pool.connection_kwargs.update(socket_connect_timeout=TIMEOUT, socket_timeout=TIMEOUT)
    return client


def _database() -> tuple[str, str, int] | None:
    """(kind, host, port) from DATABASE_URL, or None when unset. Raises on an unsupported scheme."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    parts = urlsplit(url)
    if parts.scheme == "mysql":
        return "mysql", parts.hostname or "", parts.port or 3306
    if parts.scheme in ("postgres", "postgresql"):
        return "postgres", parts.hostname or "", parts.port or 5432
    raise ValueError(f"DATABASE_URL scheme {parts.scheme!r} is not mysql or postgres")


def _header(headers, name: str) -> str:
    return next((v for k, v in headers.items() if k.lower() == name), "")


# Web


def web_server(headers) -> Result:
    import app

    expected = os.environ.get("WEB_CONCURRENCY")
    if not expected:
        if on_cloud():
            return "warn", f"{app.SERVER}, WEB_CONCURRENCY is not set", (
                "Cloud sets WEB_CONCURRENCY to match the instance size. Without it the server runs a single process.")
        return "pass", f"{app.SERVER}, 1 process (WEB_CONCURRENCY not set)", ""
    wanted = int(expected)
    # Workers are the children of one parent (uvicorn or gunicorn master); a single process has no parent.
    count = _siblings(os.getppid()) if wanted > 1 else 1
    detail = f"{app.SERVER}, {count} process(es), WEB_CONCURRENCY={wanted}"
    if count == wanted:
        return "pass", detail, ""
    return "warn", detail, "The server is running a different number of processes than WEB_CONCURRENCY asks for. A worker may be restarting; reload to check again."


def _siblings(parent: int) -> int:
    """Child processes of parent, ignoring multiprocessing's resource tracker (uvicorn spawns one)."""
    if Path("/proc/self").is_dir():
        procs = []
        for pid in filter(str.isdigit, os.listdir("/proc")):
            try:
                ppid = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[1]
                procs.append((ppid, Path(f"/proc/{pid}/cmdline").read_text()))
            except OSError:
                continue
    else:
        out = subprocess.run(["ps", "-A", "-o", "pid=,ppid=,command="], capture_output=True, text=True, timeout=TIMEOUT, check=True).stdout
        procs = [(line.split()[1], line) for line in out.splitlines() if line.strip()]
    return sum(ppid == str(parent) and "resource_tracker" not in cmd for ppid, cmd in procs)


def web_port(headers) -> Result:
    port = os.environ.get("PORT")
    if port:
        return "pass", f"PORT={port}", ""
    return "warn", "PORT is not set", "Cloud tells the server which port to listen on through PORT. Use `--port $PORT` in the start command."


def _proxy_header(name: str, expected: str | None = None):
    def check(headers) -> Result:
        if not on_cloud():
            return "skip", "Not on Laravel Cloud, so no proxy sits in front of this app.", CLOUD_ONLY
        value = _header(headers, name.lower())
        if not value:
            return "fail", f"{name} header is missing", "Requests should reach the app through Cloud's proxy, which adds this header. Check the app is opened through its Cloud URL."
        if expected and value.lower() != expected:
            return "fail", f"{name}={value}, expected {expected}", "Cloud's proxy should forward HTTPS requests. Check the app is opened through its https:// Cloud URL."
        return "pass", f"{name} present" + (f" ({value})" if expected else ""), ""

    return check


def web_event_loop(headers) -> Result:
    seconds = LOOP_SECONDS.get()
    if seconds is None:
        return "skip", "Running under WSGI, so there is no event loop to test.", (
            "This app is running on gunicorn (WSGI). Switch the start command to uvicorn to test async.")
    detail = f"50 concurrent 0.2 s sleeps finished in {seconds:.2f} s"
    if seconds < 0.5:
        return "pass", detail, ""
    return "fail", detail, "Awaiting many tasks at once took far longer than one sleep, so something is blocking the async event loop. Async requests will queue behind each other."


# Logging


def log_handler(headers) -> Result:
    import logging

    from laravel_cloud_logging import CloudHandler

    handlers = [type(h).__name__ for h in logging.getLogger().handlers]
    if any(isinstance(h, CloudHandler) for h in logging.getLogger().handlers):
        return "pass", f"root logger handlers: {', '.join(handlers)}", ""
    return "fail", f"root logger handlers: {', '.join(handlers) or 'none'}", (
        "laravel-cloud-logging is not installed on the root logger, so logs will not be structured for Cloud. Call configure() at startup.")


def log_socket(headers) -> Result:
    if not on_cloud():
        return "skip", "Not on Laravel Cloud, so logs go to stdout.", CLOUD_ONLY
    address = os.environ.get("LARAVEL_CLOUD_LOG_SOCKET") or "unix:///tmp/cloud-init.sock"
    try:
        if address.startswith("unix://"):
            sock, target = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM), address[len("unix://"):]
        else:
            host, _, port = address.removeprefix("tcp://").rpartition(":")
            sock, target = socket.socket(socket.AF_INET, socket.SOCK_STREAM), (host, int(port))
        with sock:
            sock.settimeout(2)
            sock.connect(target)
    except Exception as exc:
        return "fail", f"could not connect to {address}: {type(exc).__name__}", (
            "Logs fall back to stdout and may lose their levels (everything looks like info). Check the Cloud log socket is available to this process.")
    return "pass", f"connected to {address}", ""


# Runtime


def python_version(headers) -> Result:
    running = f"{sys.version_info.major}.{sys.version_info.minor}"
    detail = f"running Python {sys.version.split()[0]}"
    pinned = Path(__file__).with_name(".python-version")
    if not pinned.is_file():
        return "skip", detail + ", no .python-version file", "Add a .python-version file so the expected Python version is written down."
    wanted = pinned.read_text().strip()
    if wanted == running or wanted.startswith(running + "."):
        return "pass", f"{detail}, .python-version is {wanted}", ""
    return "warn", f"{detail}, .python-version is {wanted}", "The Python running this app is not the one pinned in .python-version. Check the Python version set for this Cloud environment."


def outbound_https(headers) -> Result:
    with urllib.request.urlopen("https://pypi.org/simple/", timeout=TIMEOUT, context=ssl.create_default_context()) as response:
        response.read(1)
        return "pass", f"GET https://pypi.org/simple/ returned {response.status}", ""


def tmp_writable(headers) -> Result:
    with tempfile.NamedTemporaryFile(dir="/tmp") as file:
        file.write(b"ok")
        file.flush()
    return "pass", "wrote and removed a file in /tmp", ""


def _cgroup(name: str) -> str | None:
    try:
        return Path("/sys/fs/cgroup", name).read_text().strip()
    except OSError:
        return None


def cpu_limit(headers) -> Result:
    value = _cgroup("cpu.max")
    if value is None:
        return "skip", "cgroup v2 cpu.max not present", "No container CPU limit can be read here (normal on a laptop)."
    quota, period = value.split()
    if quota == "max":
        return "pass", f"no CPU limit, os.cpu_count()={os.cpu_count()}", ""
    return "pass", f"CPU limit {int(quota) / int(period):g} vCPU, os.cpu_count()={os.cpu_count()}", ""


def memory_limit(headers) -> Result:
    value = _cgroup("memory.max")
    if value is None:
        return "skip", "cgroup v2 memory.max not present", "No container memory limit can be read here (normal on a laptop)."
    if value == "max":
        return "pass", "no memory limit", ""
    return "pass", f"memory limit {int(value) / 2**20:.0f} MiB", ""


def subprocess_run(headers) -> Result:
    out = subprocess.run([sys.executable, "-c", "print(1)"], capture_output=True, text=True, timeout=TIMEOUT, check=True).stdout
    assert out.strip() == "1", out
    return "pass", "started a child Python and read its output", ""


def threads_run(headers) -> Result:
    done = []
    threads = [threading.Thread(target=done.append, args=(n,)) for n in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(TIMEOUT)
    if len(done) != 4:
        return "fail", f"{len(done)} of 4 threads finished", "Threads did not finish, which breaks anything that uses background work."
    return "pass", "started and joined 4 threads", ""


# Services


def redis_ping(headers) -> Result:
    client = _redis()
    if client is None:
        return "skip", "REDIS_URL is not set", "Attach a Valkey cache to this environment to test it and to run queue jobs."
    client.ping()
    return "pass", "PING returned PONG", ""


def database_query(headers) -> Result:
    database = _database()
    if database is None:
        return "skip", "DATABASE_URL is not set", "Attach a database to this environment to test it."
    kind, host, port = database
    parts = urlsplit(os.environ["DATABASE_URL"])
    user, password, name = unquote(parts.username or ""), unquote(parts.password or ""), parts.path.lstrip("/")
    tls = host not in LOCAL_HOSTS
    verify = os.environ.get("DB_SSL_VERIFY") != "0"
    if kind == "mysql":
        import pymysql

        context = None
        if tls:
            context = ssl.create_default_context(cafile=certifi.where())
            if not verify:  # Cloud's ProxySQL certificate is self-signed: encrypted, not authenticated
                context.check_hostname, context.verify_mode = False, ssl.CERT_NONE
        conn = pymysql.connect(host=host, port=port, user=user, password=password, database=name or None,
                               ssl=context, connect_timeout=TIMEOUT, read_timeout=TIMEOUT)
    else:
        import psycopg

        extra = {}
        if tls:
            extra = dict(sslmode="verify-full", sslrootcert=certifi.where()) if verify else dict(sslmode="require")
        conn = psycopg.connect(host=host, port=port, user=user, password=password, dbname=name or None,
                               connect_timeout=TIMEOUT, **extra)
    with conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        assert tuple(cursor.fetchone()) == (1,)
    security = ("TLS, certificate not verified" if not verify else "TLS") if tls else "no TLS (local host)"
    return "pass", f"SELECT 1 on {kind} at {host}:{port}, {security}", ""


def dns_hosts(headers) -> Result:
    client, database = _redis(), _database()
    hosts = {}
    if client is not None:
        hosts["redis"] = client.connection_pool.connection_kwargs.get("host")
    if database is not None:
        hosts[database[0]] = database[1]
    hosts = {label: host for label, host in hosts.items() if host}
    if not hosts:
        return "skip", "no Redis or database host configured", "Attach a Valkey cache or database to test its DNS name."
    found = {}
    for label, host in hosts.items():
        # getaddrinfo has no timeout of its own, so a daemon thread bounds it.
        answer: list = []
        worker = threading.Thread(target=lambda: answer.append(socket.getaddrinfo(host, None)), daemon=True)
        worker.start()
        worker.join(TIMEOUT)
        found[label] = bool(answer)
    detail = ", ".join(f"{hosts[label]} {'resolved' if ok else 'did not resolve'}" for label, ok in found.items())
    if all(found.values()):
        return "pass", detail, ""
    return "fail", detail, "The app cannot look up the hostname of an attached service, so it cannot connect to it. Check the service is attached to this environment."


FAIL_HELP = "The check could not complete. See the detail for the error, then check the network and service settings of this environment."

CHECKS = [
    ("Web", "web.server", "Server and processes", web_server),
    ("Web", "web.port", "Listening port", web_port),
    ("Web", "web.proto", "Proxy sends X-Forwarded-Proto: https", _proxy_header("X-Forwarded-Proto", "https")),
    ("Web", "web.forwarded_for", "Proxy sends X-Forwarded-For", _proxy_header("X-Forwarded-For")),
    ("Web", "web.request_id", "Proxy sends Cloud-Request-ID", _proxy_header("Cloud-Request-ID")),
    ("Web", "web.event_loop", "Async event loop not blocked", web_event_loop),
    ("Logging", "logging.handler", "Cloud logging handler installed", log_handler),
    ("Logging", "logging.socket", "Cloud log socket reachable", log_socket),
    ("Runtime", "runtime.python", "Python version matches .python-version", python_version),
    ("Runtime", "runtime.https", "Outbound HTTPS", outbound_https),
    ("Runtime", "runtime.tmp", "/tmp writable", tmp_writable),
    ("Runtime", "runtime.cpu", "CPU limit", cpu_limit),
    ("Runtime", "runtime.memory", "Memory limit", memory_limit),
    ("Runtime", "runtime.subprocess", "Subprocess", subprocess_run),
    ("Runtime", "runtime.threads", "Threads", threads_run),
    ("Services", "services.redis", "Valkey / Redis", redis_ping),
    ("Services", "services.database", "Database", database_query),
    ("Services", "services.dns", "DNS for service hosts", dns_hosts),
]


def _one(entry, headers) -> dict[str, str]:
    _, id, label, fn = entry
    try:
        status, detail, help = fn(headers)
    except Exception as exc:
        status, detail, help = "fail", f"{type(exc).__name__}: {exc}", FAIL_HELP
    return {"id": id, "label": label, "status": status, "detail": _scrub(detail)[:300], "help": help}


def run(headers) -> dict:
    with ThreadPoolExecutor(len(CHECKS)) as pool:
        # Copy the context here: worker threads must see LOOP_SECONDS set by asgi.py.
        futures = [pool.submit(contextvars.copy_context().run, _one, entry, headers) for entry in CHECKS]
        rows = [future.result() for future in futures]
    groups: dict[str, list] = {}
    for entry, row in zip(CHECKS, rows):
        groups.setdefault(entry[0], []).append(row)
    return {"groups": [{"name": name, "checks": checks} for name, checks in groups.items()]}
