"""Start gunicorn and uvicorn with a stand-in handle and check the adapter routes.

The real handle() is owned by the core router. This file installs a tiny fake in the
child process only, via sitecustomize, so the adapters can be checked before that lands.
Run: uv run --with gunicorn --with 'uvicorn[standard]' python test_servers.py
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
UPLOAD_BYTES = 5 * 1024 * 1024
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# Child processes import this before wsgi/asgi. It does not live in the repo.
STANDIN = """
import os
import sys
import time

root = os.environ["L5_ROOT"]
if root not in sys.path:
    sys.path.insert(0, root)

import app

def handle(method, path, headers, body):
    route = path.split("?", 1)[0]
    if method == "GET" and route == "/api/ping":
        payload = b'{"ok": true}'
    elif method == "GET" and route == "/api/slow":
        time.sleep(0.6)
        payload = b'{"slow": true}'
    elif method == "GET" and route == "/api/server-name":
        server = getattr(app, "SERVER", "")
        payload = ('{"server":"%s"}' % server).encode()
    elif method == "POST" and route == "/api/body":
        ctype = headers.get("content-type", "")
        payload = ('{"n":%d,"content_type":"%s"}' % (len(body), ctype)).encode()
    else:
        payload = b'{"error": "not found"}'
        return 404, _headers(payload), payload
    return 200, _headers(payload), payload

def _headers(payload):
    return [
        ("Content-Type", "application/json"),
        ("Content-Length", str(len(payload))),
    ]

app.handle = handle
"""


def main() -> None:
    check_config()
    with tempfile.TemporaryDirectory(prefix="l5-standin-") as directory:
        standin = Path(directory)
        (standin / "sitecustomize.py").write_text(STANDIN)
        check_gunicorn(standin)
        check_gunicorn_without_port(standin)
        check_uvicorn(standin)
        check_uvicorn_workers(standin)
    print("test_servers ok")


def check_config() -> None:
    script = """
import importlib.util
spec = importlib.util.spec_from_file_location("gconf", "gunicorn.conf.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
print("workers", mod.workers)
print("graceful", mod.graceful_timeout)
print("timeout", mod.timeout)
print("bind", mod.bind)
"""

    def run(extra: dict, drop: tuple) -> str:
        env = os.environ.copy()
        env["PORT"] = "1234"
        for key in drop:
            env.pop(key, None)
        env.update(extra)
        proc = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise AssertionError(proc.stderr or proc.stdout)
        return proc.stdout

    unset = run({}, ("WEB_CONCURRENCY",))
    expect(unset, "workers 1", "graceful 30", "timeout 120", "bind [::]:1234")
    no_port = run({}, ("PORT", "WEB_CONCURRENCY"))
    expect(no_port, "workers 1", "bind [::]:8000")
    expect(run({"WEB_CONCURRENCY": "4"}, ()), "workers 4")
    bad = run({"WEB_CONCURRENCY": "nope"}, ())
    expect(bad, "workers 1", "invalid WEB_CONCURRENCY")
    print("config ok")


def check_gunicorn(standin: Path) -> None:
    port = free_port()
    log_path = standin / "gunicorn.log"
    env = child_env(standin)
    env["PORT"] = str(port)
    env["WEB_CONCURRENCY"] = "1"
    proc = spawn(
        [sys.executable, "-m", "gunicorn", "wsgi:app", "--config", str(ROOT / "gunicorn.conf.py")],
        env,
        log_path,
    )
    try:
        wait_ready(proc, port, log_path)
        check_http(port, "gunicorn")
        text = stop_and_read(proc, log_path)
    finally:
        reap(proc)
    expect(text, "gunicorn workers=1 WEB_CONCURRENCY=1", "sigterm pid=")
    print("gunicorn ok")


def check_gunicorn_without_port(standin: Path) -> None:
    # L1 execs `gunicorn wsgi:app -b [::]:8000` and does not set PORT.
    # Use a free port so this check does not depend on 8000 being open.
    port = free_port()
    log_path = standin / "gunicorn-noport.log"
    env = child_env(standin)
    env.pop("PORT", None)
    env["WEB_CONCURRENCY"] = "1"
    proc = spawn(
        [sys.executable, "-m", "gunicorn", "wsgi:app", "-b", f"[::]:{port}"],
        env,
        log_path,
    )
    try:
        wait_ready(proc, port, log_path)
        status, body, _headers = request(port, "GET", "/api/ping")
        assert status == 200 and body == b'{"ok": true}', (status, body)
        stop_and_read(proc, log_path)
    finally:
        reap(proc)
    print("gunicorn without PORT ok")


def check_uvicorn(standin: Path) -> None:
    port = free_port()
    log_path = standin / "uvicorn.log"
    env = child_env(standin)
    env["PORT"] = str(port)
    env.pop("WEB_CONCURRENCY", None)
    proc = spawn(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "asgi:app",
            "--host",
            "::",
            "--port",
            str(port),
            "--workers",
            "1",
            "--no-access-log",
            "--log-level",
            "info",
        ],
        env,
        log_path,
    )
    try:
        wait_ready(proc, port, log_path)
        check_http(port, "uvicorn")
        check_websocket(port)
        check_event_loop_not_stalled(port)
        text = stop_and_read(proc, log_path)
    finally:
        reap(proc)
    expect(
        text,
        "lifespan startup pid=",
        "lifespan shutdown pid=",
        "sigterm pid=",
    )
    print("uvicorn ok")


def check_uvicorn_workers(standin: Path) -> None:
    port = free_port()
    log_path = standin / "uvicorn-workers.log"
    env = child_env(standin)
    env["PORT"] = str(port)
    env["WEB_CONCURRENCY"] = "2"
    proc = spawn(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "asgi:app",
            "--host",
            "::",
            "--port",
            str(port),
            "--no-access-log",
            "--log-level",
            "info",
        ],
        env,
        log_path,
    )
    try:
        # The parent binds the socket before asgi is imported, so this host is ::1.
        wait_ready(proc, port, log_path, host="::1")
        wait_for_log(proc, log_path, "lifespan startup pid=", 2)
        text = stop_and_read(proc, log_path)
    finally:
        reap(proc)
    if text.count("lifespan shutdown pid=") < 2:
        raise AssertionError(f"expected two lifespan shutdown lines\n{text}")
    if text.count("sigterm pid=") < 2:
        raise AssertionError(f"expected two sigterm lines\n{text}")
    print("uvicorn WEB_CONCURRENCY=2 ok")


def check_event_loop_not_stalled(port: int) -> None:
    errors = []
    results = {}

    def run(name, func):
        try:
            results[name] = func()
        except Exception as exc:
            errors.append(exc)

    def slow():
        started = time.monotonic()
        status, body, _headers = request(port, "GET", "/api/slow")
        return time.monotonic() - started, status, body

    def ping():
        started = time.monotonic()
        status, body, _headers = request(port, "GET", "/api/ping")
        return time.monotonic() - started, status, body

    def sse():
        times, events = read_stream(port, "/api/stream?seconds=1&interval=0.2")
        return times, events

    slow_thread = threading.Thread(target=run, args=("slow", slow))
    slow_thread.start()
    time.sleep(0.1)
    ping_thread = threading.Thread(target=run, args=("ping", ping))
    sse_thread = threading.Thread(target=run, args=("sse", sse))
    ping_thread.start()
    sse_thread.start()
    slow_thread.join(timeout=3)
    ping_thread.join(timeout=3)
    sse_thread.join(timeout=3)
    if errors or slow_thread.is_alive() or ping_thread.is_alive() or sse_thread.is_alive():
        raise AssertionError(errors or "timed out waiting for concurrent requests")
    slow_s, slow_status, slow_body = results["slow"]
    ping_s, ping_status, ping_body = results["ping"]
    times, events = results["sse"]
    assert slow_status == 200 and slow_body == b'{"slow": true}', slow_body
    assert slow_s >= 0.5, slow_s
    assert ping_status == 200 and ping_body == b'{"ok": true}', ping_body
    assert ping_s < 0.25, ping_s
    assert events and times[0] < 0.45, times


def check_http(port: int, server: str) -> None:
    status, body, headers = request(port, "GET", "/api/ping")
    assert status == 200 and body == b'{"ok": true}', (status, body)
    assert headers.get("access-control-allow-origin") is None

    status, body, _headers = request(port, "GET", "/api/ping", host="::1")
    assert status == 200 and body == b'{"ok": true}', (status, body)

    status, body, _headers = request(port, "GET", "/api/server-name")
    assert json.loads(body) == {"server": server}, body

    status, body, _headers = request(
        port,
        "POST",
        "/api/body",
        b"hello-body",
        {"Content-Type": "Application/JSON"},
    )
    assert json.loads(body) == {"n": 10, "content_type": "Application/JSON"}, body

    times, events = read_stream(port)
    assert [event["i"] for event in events] == [0, 1, 2, 3], events
    assert all(event["t"] > 0 for event in events)
    # Buffered streams arrive together at the end (~2s) or in one burst.
    assert times[0] < 1.2, times
    assert times[-1] > 1.4, times
    assert times[-1] - times[0] >= 1.0, times

    status, body, headers = request(port, "GET", "/api/stream?seconds=0&interval=0.5")
    assert status == 400, status
    assert b"Traceback" not in body
    assert headers.get("x-content-type-options") == "nosniff"
    assert headers.get("access-control-allow-origin") is None
    status, _body, _headers = request(port, "GET", "/api/stream?seconds=2&interval=0.01")
    assert status == 400, status

    payload = os.urandom(UPLOAD_BYTES)
    status, body, _headers = request(port, "POST", "/api/upload", payload)
    uploaded = json.loads(body)
    assert status == 200, body
    assert uploaded["bytes"] == UPLOAD_BYTES
    assert uploaded["sha256"] == hashlib.sha256(payload).hexdigest()

    too_big = 512 * 1024 * 1024 + 1
    line = raw_status(
        port,
        (
            "POST /api/upload HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{port}\r\n"
            f"Content-Length: {too_big}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode(),
    )
    assert b" 413 " in line, line
    line = raw_status(
        port,
        (
            "POST /api/body HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{port}\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {64 * 1024 + 1}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode(),
    )
    assert b" 400 " in line, line


def check_websocket(port: int) -> None:
    sock = ws_connect("127.0.0.1", port, "/ws/echo")
    try:
        ws_send(sock, 1, b"hello")
        opcode, data = ws_recv(sock)
        assert opcode == 1 and data == b"hello", (opcode, data)
        ws_send(sock, 2, b"\x00\x01")
        opcode, data = ws_recv(sock)
        assert opcode == 2 and data == b"\x00\x01", (opcode, data)
        ws_send(sock, 1, b"bye")
        opcode, _data = ws_recv(sock)
        assert opcode == 8, opcode
    finally:
        sock.close()


def read_stream(port: int, path: str = "/api/stream?seconds=2&interval=0.5") -> tuple:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    start = time.monotonic()
    conn.request("GET", path)
    resp = conn.getresponse()
    try:
        assert resp.status == 200, resp.status
        assert "text/event-stream" in (resp.getheader("Content-Type") or "")
        assert resp.getheader("X-Content-Type-Options") == "nosniff"
        assert resp.getheader("Access-Control-Allow-Origin") is None
        times = []
        events = []
        while True:
            line = resp.readline()
            if not line:
                break
            if line.startswith(b"data:"):
                times.append(time.monotonic() - start)
                events.append(json.loads(line[len(b"data:") :].strip()))
        return times, events
    finally:
        conn.close()


def request(
    port: int,
    method: str,
    path: str,
    body: bytes | None = None,
    headers: dict | None = None,
    host: str = "127.0.0.1",
) -> tuple:
    conn = http.client.HTTPConnection(host, port, timeout=30)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        payload = resp.read()
        found = {key.lower(): value for key, value in resp.getheaders()}
        return resp.status, payload, found
    finally:
        conn.close()


def raw_status(port: int, payload: bytes) -> bytes:
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    try:
        sock.sendall(payload)
        sock.settimeout(5)
        data = b""
        while b"\r\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
        if not data:
            raise AssertionError("no response")
        return data.split(b"\r\n", 1)[0]
    finally:
        sock.close()


def ws_connect(host: str, port: int, path: str) -> socket.socket:
    sock = socket.create_connection((host, port), timeout=5)
    key = base64.b64encode(os.urandom(16)).decode()
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    sock.sendall(request.encode())
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise AssertionError("websocket handshake closed")
        data += chunk
    head = data.split(b"\r\n\r\n", 1)[0].decode("latin1")
    lines = head.split("\r\n")
    if " 101 " not in lines[0]:
        raise AssertionError(lines[0])
    expected = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
    accept = ""
    for line in lines[1:]:
        if line.lower().startswith("sec-websocket-accept:"):
            accept = line.split(":", 1)[1].strip()
    if accept != expected:
        raise AssertionError(accept)
    sock.settimeout(5)
    return sock


def ws_send(sock: socket.socket, opcode: int, payload: bytes) -> None:
    mask = os.urandom(4)
    header = bytearray([0x80 | opcode])
    length = len(payload)
    if length < 126:
        header.append(0x80 | length)
    else:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", length))
    masked = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
    sock.sendall(bytes(header) + mask + masked)


def ws_recv(sock: socket.socket) -> tuple:
    first, second = _exact(sock, 2)
    opcode = first & 0x0F
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", _exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _exact(sock, 8))[0]
    if length > 1024 * 1024:
        raise AssertionError("frame too large")
    if second & 0x80:
        mask = _exact(sock, 4)
        raw = _exact(sock, length)
        payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(raw))
    else:
        payload = _exact(sock, length)
    return opcode, payload


def _exact(sock: socket.socket, count: int) -> bytes:
    buf = b""
    while len(buf) < count:
        chunk = sock.recv(count - len(buf))
        if not chunk:
            raise AssertionError("socket closed")
        buf += chunk
    return buf


def child_env(standin: Path) -> dict:
    env = os.environ.copy()
    env["L5_ROOT"] = str(ROOT)
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = str(standin) + os.pathsep + env.get("PYTHONPATH", "")
    return env


def spawn(args: list, env: dict, log_path: Path) -> subprocess.Popen:
    log_file = open(log_path, "wb")
    proc = subprocess.Popen(
        args,
        cwd=ROOT,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    proc._log_file = log_file  # closed after the process exits
    return proc


def wait_ready(proc: subprocess.Popen, port: int, log_path: Path, host: str = "127.0.0.1") -> None:
    deadline = time.monotonic() + 20
    last = ""
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError(f"server exited {proc.returncode}\n{log_path.read_text(errors='replace')}")
        try:
            status, body, _headers = request(port, "GET", "/api/ping", host=host)
        except OSError as exc:
            last = str(exc)
            time.sleep(0.05)
            continue
        if status == 200 and body == b'{"ok": true}':
            return
        last = f"{status} {body!r}"
        time.sleep(0.05)
    raise AssertionError(f"server did not become ready ({last})\n{log_path.read_text(errors='replace')}")


def wait_for_log(proc: subprocess.Popen, log_path: Path, needle: str, count: int) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError(f"server exited {proc.returncode}\n{log_path.read_text(errors='replace')}")
        if log_path.read_text(errors="replace").count(needle) >= count:
            return
        time.sleep(0.05)
    text = log_path.read_text(errors="replace")
    raise AssertionError(f"expected {count} {needle!r} lines\n{text}")


def stop_and_read(proc: subprocess.Popen, log_path: Path) -> str:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            code = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            reap(proc)
            raise AssertionError(f"did not exit on SIGTERM\n{log_path.read_text(errors='replace')}")
    else:
        code = proc.returncode
    close_log(proc)
    if code not in (0, -signal.SIGTERM):
        raise AssertionError(f"exit {code}\n{log_path.read_text(errors='replace')}")
    return log_path.read_text(errors="replace")


def reap(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    close_log(proc)


def close_log(proc: subprocess.Popen) -> None:
    log_file = getattr(proc, "_log_file", None)
    if log_file is not None and not log_file.closed:
        log_file.close()


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def expect(text: str, *needles: str) -> None:
    missing = [needle for needle in needles if needle not in text]
    if missing:
        raise AssertionError(f"missing {missing} in:\n{text}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback

        traceback.print_exc()
        sys.exit(1)
