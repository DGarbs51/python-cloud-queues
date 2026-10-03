"""Boot the real app under uvicorn and gunicorn with Cloud-style start commands.

No Redis needed: only routes that don't touch the queue are called.
Run: uv run python test_servers.py
"""

from __future__ import annotations

import http.client
import json
import os
import re
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
COMMANDS = {
    # Exactly what the README tells people to paste into Cloud, with $PORT expanded.
    "uvicorn": "uvicorn asgi:app --host :: --port {port}",
    "gunicorn": "gunicorn wsgi:app --bind [::]:{port}",
}
# uvicorn's multi-worker parent never imports the app, so configure() can't reach it
# (laravel-cloud-logging README "Limits"). Its lifecycle lines stay plain text.
UVICORN_PARENT = re.compile(r"INFO: +(Uvicorn running on|Started parent process|Received SIGTERM|Waiting for child process|Stopping parent process)")


def free_port() -> int:
    with socket.socket(socket.AF_INET6) as sock:
        sock.bind(("::1", 0))
        return sock.getsockname()[1]


def request(port, method, path, body=b"", headers=None):
    conn = http.client.HTTPConnection("::1", port, timeout=10)
    try:
        conn.request(method, path, body=body, headers=headers or {},
                     encode_chunked=(headers or {}).get("Transfer-Encoding") == "chunked")
        response = conn.getresponse()
        return response.status, {k.lower(): v for k, v in response.getheaders()}, response.read()
    finally:
        conn.close()


def check(server: str) -> None:
    port = free_port()
    env = {**os.environ, "PORT": str(port), "WEB_CONCURRENCY": "2", "PYTHONUNBUFFERED": "1"}
    env.pop("LARAVEL_CLOUD", None)  # logs to stdout, not the Cloud socket
    with tempfile.TemporaryFile() as out:
        proc = subprocess.Popen(COMMANDS[server].format(port=port).split(), cwd=ROOT, env=env,
                                stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + 20
            while True:
                assert proc.poll() is None, f"{server} exited {proc.returncode}"
                try:
                    if request(port, "GET", "/api/ping")[0] == 200:
                        break
                except OSError:
                    pass
                assert time.monotonic() < deadline, f"{server} not ready on [::]:{port}"
                time.sleep(0.1)
            status, headers, body = request(port, "GET", "/api/ping", headers={"Cloud-Request-ID": "cr-test"})
            assert (status, json.loads(body)) == (200, {"ok": True}) and headers.get("x-request-id")
            assert request(port, "GET", "/")[0] == 200
            assert request(port, "GET", "/nope")[0] == 404
            assert request(port, "POST", "/api/check", b"{}", {"Content-Type": "text/plain"})[0] == 415
            # Chunked bodies (no Content-Length) get the same JSON checks as sized ones.
            chunked = request(port, "POST", "/api/check", iter([b"[", b"]"]),
                              {"Content-Type": "application/json", "Transfer-Encoding": "chunked"})
            assert chunked[0] == 400, f"{server} chunked body: {chunked}"
            # uvicorn serves WebSockets at /ws/echo (the Cloud check upgrades through nginx to here).
            if server == "uvicorn":
                with socket.create_connection(("::1", port), timeout=10) as ws:
                    ws.sendall(b"GET /ws/echo HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                               b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
                    reply = ws.recv(4096)
                assert reply.startswith(b"HTTP/1.1 101") and b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" in reply, reply
            big = b"x" * (64 * 1024 + 1)
            assert request(port, "POST", "/api/check", big, {"Content-Type": "application/json"})[0] == 400
            os.killpg(proc.pid, signal.SIGTERM)
            assert proc.wait(timeout=40) == 0, f"{server} exit code {proc.returncode}"
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()
        out.seek(0)
        lines = [line for line in out.read().decode(errors="replace").splitlines() if line.strip()]
    plain = [line for line in lines if not line.startswith("{") and not (server == "uvicorn" and UVICORN_PARENT.match(line))]
    assert not plain, f"{server} wrote plain-text log lines:\n" + "\n".join(plain)
    records = [json.loads(line) for line in lines if line.startswith("{")]
    access = [r for r in records if r["message"] == "access" and r["context"].get("cloud_request_id") == "cr-test"]
    assert len(access) == 1 and access[0]["context"]["path"] == "/api/ping", records
    startups = {r["context"].get("pid") for r in records if r["message"] == "startup"}
    assert len(startups) == 2, f"{server}: expected 2 workers from WEB_CONCURRENCY, saw {startups}"
    print(f"{server} ok")


if __name__ == "__main__":
    for name in COMMANDS:
        check(name)
    print("test_servers ok")
