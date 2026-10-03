"""Platform checks behind GET /api/checks.

Each check is a small function returning (status, detail, help); CHECKS lists them in display order.
run() never raises: a check that throws becomes a fail row, every detail is scrubbed of credentials,
and the checks run in parallel daemon threads under a 12 s deadline.
"""

from __future__ import annotations

import base64
import contextvars
import hashlib
import os
import re
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path
from urllib.parse import unquote, urlsplit

import certifi

TIMEOUT = 5
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
CLOUD_ONLY = "Only checked on Laravel Cloud."
# asgi.py sets this to the measured seconds for 50 concurrent 0.2 s sleeps; unset means WSGI.
LOOP_SECONDS: contextvars.ContextVar[float | None] = contextvars.ContextVar("loop_seconds", default=None)
# asgi.py / wsgi.py set this to the address the request came from: nginx's side of the upstream connection.
PEER: contextvars.ContextVar[str] = contextvars.ContextVar("peer", default="")
USERINFO = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^\s/@]*@")

Result = tuple[str, str, str]


def on_cloud() -> bool:
    return os.environ.get("LARAVEL_CLOUD") == "1"


def _scrub(text: str) -> str:
    text = USERINFO.sub(r"\1", text)
    for name in ("DATABASE_URL", "REDIS_URL"):
        try:
            password = urlsplit(os.environ.get(name) or "").password
        except ValueError:  # unparsable URL: its own check reports that
            continue
        for secret in (password, unquote(password or "")):
            if secret:
                text = text.replace(secret, "***")
    return text


def _redis():
    """The telemetry Redis client with bounded timeouts, or None when no Valkey is attached."""
    import app

    if not (os.environ.get("REDIS_URL") or os.environ.get("LARAVEL_CLOUD_QUEUES_REDIS_URL")):
        return None
    client = app.telemetry.store
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
    port = os.environ.get("PORT", "")
    if not port.isdigit():
        return "warn", f"PORT is {port or 'not set'}", "Cloud tells the server which port to listen on through PORT. Use `--port $PORT` in the start command."
    if not Path("/proc/net/tcp6").is_file():
        return "skip", f"PORT={port}; listening sockets can't be read here", "Only checked on Linux, which includes Laravel Cloud."
    listeners = _listeners(int(port))
    if "::" in listeners:
        return "pass", f"listening on [::]:{port}", ""
    if listeners:
        return "fail", f"listening on {', '.join(sorted(listeners))} port {port}, not [::]", (
            "Cloud reaches apps over IPv6, so the server must listen on `::`. Use `--host ::` (uvicorn) or `--bind [::]:$PORT` (gunicorn).")
    return "warn", f"nothing found listening on port {port}", "The server may be listening on a different port than PORT. Use `--port $PORT` in the start command."


def _listeners(port: int) -> set[str]:
    """Addresses in LISTEN state on port, from /proc/net/tcp6 and /proc/net/tcp ("::", "0.0.0.0", or other)."""
    found = set()
    for table, wildcard in (("tcp6", "0" * 32), ("tcp", "0" * 8)):
        for line in Path("/proc/net", table).read_text().splitlines()[1:]:
            local, state = line.split()[1], line.split()[3]
            address, hex_port = local.split(":")
            if state == "0A" and int(hex_port, 16) == port:  # 0A = LISTEN
                found.add(("::" if table == "tcp6" else "0.0.0.0") if address == wildcard else address)
    return found


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


def web_websocket(headers) -> Result:
    import app

    if not on_cloud():
        return "skip", "Not on Laravel Cloud, so there is no proxy chain to test.", CLOUD_ONLY
    if app.SERVER != "uvicorn":
        return "skip", f"Running on {app.SERVER} (WSGI), which has no WebSockets.", (
            "Switch the start command to uvicorn to test WebSockets through Cloud's proxy.")
    host = _header(headers, "host").split(":")[0]
    if not host:
        return "fail", "request has no Host header", FAIL_HELP
    # A raw RFC 6455 handshake to this app's public URL: Cloudflare, Envoy and the pod's nginx must all
    # pass Upgrade: websocket through for the app to answer 101.
    key = base64.b64encode(os.urandom(16)).decode()
    request = (f"GET /ws/echo HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
               f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n")
    context = ssl.create_default_context()
    with socket.create_connection((host, 443), timeout=TIMEOUT) as raw, context.wrap_socket(raw, server_hostname=host) as sock:
        sock.sendall(request.encode())
        response = b""
        while b"\r\n\r\n" not in response and len(response) < 16384:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
    status_line = response.split(b"\r\n", 1)[0].decode(errors="replace")
    accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
    if " 101 " in status_line + " " and accept.encode() in response:
        return "pass", f"wss://{host}/ws/echo upgraded ({status_line})", ""
    return "fail", f"wss://{host}/ws/echo answered {status_line or 'nothing'}", (
        "A proxy between the browser and the app dropped the WebSocket upgrade, so WebSockets (live updates, "
        "chat, Django Channels) can't connect. The pod's nginx must forward the Upgrade and Connection headers.")


def web_upstream_ipv6(headers) -> Result:
    if not on_cloud():
        return "skip", "Not on Laravel Cloud, so no nginx sits in front of this app.", CLOUD_ONLY
    peer = PEER.get()
    if peer == "::1":
        return "pass", "nginx reaches the app over IPv6 (::1)", ""
    if peer == "127.0.0.1":
        return "warn", "nginx reaches the app over IPv4 (127.0.0.1)", (
            "This app works because it listens on both IPv4 and IPv6, but an app that listens on IPv6 only "
            "(for example `--host ::` with IPV6_V6ONLY) would be unreachable. nginx should try [::1] first.")
    return "warn", f"request came from {peer or 'an unknown address'}, not the pod's nginx", (
        "Expected nginx on the same pod (::1 or 127.0.0.1).")


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
    limit = -(-int(quota) // int(period))  # whole CPUs, rounded up
    detail = f"CPU limit {int(quota) / int(period):g} vCPU, os.cpu_count()={os.cpu_count()}"
    if (os.cpu_count() or 0) > limit:
        return "warn", detail, (
            "Python sees the whole server's CPUs, not this instance's, so default pool sizes (multiprocessing, "
            "ProcessPoolExecutor, NumPy/OpenBLAS threads) start too many workers. Set PYTHON_CPU_COUNT (3.13+) "
            "and size pools explicitly.")
    return "pass", detail, ""


def memory_limit(headers) -> Result:
    value = _cgroup("memory.max")
    if value is None:
        return "skip", "cgroup v2 memory.max not present", "No container memory limit can be read here (normal on a laptop)."
    if value == "max":
        return "pass", "no memory limit", ""
    seen = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    detail = f"memory limit {int(value) / 2**20:.0f} MiB, sysconf reports {seen / 2**20:.0f} MiB"
    if seen > int(value):
        return "warn", detail, (
            "Python sees the whole server's memory, not this instance's limit, so anything sized from physical "
            "memory (caches, worker counts) can overshoot and get the process killed.")
    return "pass", detail, ""


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
                               connect_timeout=TIMEOUT, options=f"-c statement_timeout={TIMEOUT * 1000}", **extra)
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
        # Bind per iteration: a lookup that outlives its timeout must not land in the next host's answer.
        worker = threading.Thread(target=lambda out=answer, name=host: out.append(socket.getaddrinfo(name, None)), daemon=True)
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
    ("Web", "web.websocket", "WebSocket upgrade through Cloud's proxy", web_websocket),
    ("Web", "web.upstream_ipv6", "nginx reaches the app over IPv6", web_upstream_ipv6),
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


DEADLINE = 12  # seconds for the whole list; a check still running then is reported, not awaited
# Checks still running (possibly hung) from any request. A hung check keeps its one thread; later
# requests report it instead of starting another, so stuck threads can't pile up.
_running: set[str] = set()
_running_lock = threading.Lock()


def _record(entry, headers, results: dict) -> None:
    try:
        results[entry[1]] = _one(entry, headers)
    finally:
        with _running_lock:
            _running.discard(entry[1])


def run(headers) -> dict:
    results: dict[str, dict] = {}
    threads, busy = [], set()
    for entry in CHECKS:
        with _running_lock:
            if entry[1] in _running:
                busy.add(entry[1])
                continue
            _running.add(entry[1])
        # Copy the context per check: threads must see LOOP_SECONDS set by asgi.py. Daemon threads are
        # never joined at exit, so a hung probe can't block shutdown.
        thread = threading.Thread(target=contextvars.copy_context().run, args=(_record, entry, headers, results), daemon=True)
        thread.start()
        threads.append(thread)
    deadline = time.monotonic() + DEADLINE
    for thread in threads:
        thread.join(max(0, deadline - time.monotonic()))
    rows = []
    for _, id, label, _fn in CHECKS:
        if id in results:
            rows.append(results[id])
        elif id in busy:
            rows.append({"id": id, "label": label, "status": "warn", "detail": "an earlier run of this check has not finished yet",
                         "help": "This check was still running from a previous request. If it stays like this, the service it talks to has stopped responding."})
        else:
            rows.append({"id": id, "label": label, "status": "fail", "detail": f"no answer within {DEADLINE} s",
                         "help": "This check hung. The service it talks to accepted the connection but stopped responding."})
    groups: dict[str, list] = {}
    for entry, row in zip(CHECKS, rows):
        groups.setdefault(entry[0], []).append(row)
    return {"groups": [{"name": name, "checks": checks} for name, checks in groups.items()]}
