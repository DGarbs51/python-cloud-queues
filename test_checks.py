"""Contract tests for checks.py (no network, no Redis, no database).

Run: uv run --env-file .env python test_checks.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from unittest.mock import patch

import checks

SECRET = "hunter2-s3cret"
STATUSES = {"pass", "warn", "fail", "skip"}
KEYS = {"id", "label", "status", "detail", "help"}


def check_shape(result: dict) -> list[dict]:
    assert list(result) == ["groups"]
    rows = [row for group in result["groups"] for row in group["checks"]]
    assert [g["name"] for g in result["groups"]] == ["Web", "Logging", "Runtime", "Services"]
    assert all(set(row) == KEYS and row["status"] in STATUSES for row in rows), rows
    assert all(isinstance(v, str) for row in rows for v in row.values())
    assert len({row["id"] for row in rows}) == len(rows) == len(checks.CHECKS)
    json.dumps(result)
    return rows


def offline_run(headers: dict, **env: str) -> list[dict]:
    """Run every check with the network-facing ones stubbed out."""
    with patch.dict(os.environ, env), patch("urllib.request.urlopen", side_effect=OSError("offline")), \
            patch("socket.getaddrinfo", return_value=[("addr",)]):
        return check_shape(checks.run(headers))


def main() -> None:
    rows = {row["id"]: row for row in offline_run({})}
    # Never raises: the stubbed network call becomes a fail row carrying the exception class.
    assert rows["runtime.https"]["status"] == "fail" and rows["runtime.https"]["detail"].startswith("OSError")
    # WSGI path (LOOP_SECONDS unset) reports the async check as skipped with the fixed help text.
    assert rows["web.event_loop"]["status"] == "skip" and "Switch the start command to uvicorn" in rows["web.event_loop"]["help"]
    assert rows["runtime.tmp"]["status"] == rows["runtime.subprocess"]["status"] == rows["runtime.threads"]["status"] == "pass"
    assert rows["web.proto"]["status"] == "skip"  # off Cloud

    # ASGI path: a measured value passes under 0.5 s and fails over it.
    for seconds, status in ((0.21, "pass"), (3.0, "fail")):
        checks.LOOP_SECONDS.set(seconds)
        assert {r["id"]: r for r in offline_run({})}["web.event_loop"]["status"] == status
    checks.LOOP_SECONDS.set(None)

    # On Cloud the proxy headers are required, matched case-insensitively.
    cloud = {"LARAVEL_CLOUD": "1", "LARAVEL_CLOUD_LOG_SOCKET": "unix:///nonexistent.sock", "WEB_CONCURRENCY": "1"}
    bad = {r["id"]: r["status"] for r in offline_run({"x-forwarded-proto": "http"}, **cloud)}
    assert bad["web.proto"] == bad["web.forwarded_for"] == bad["web.request_id"] == "fail"
    assert bad["logging.socket"] == "fail"
    good = {r["id"]: r["status"] for r in offline_run(
        {"X-Forwarded-Proto": "https", "x-forwarded-for": "1.2.3.4", "Cloud-Request-Id": "r"}, **cloud)}
    assert good["web.proto"] == good["web.forwarded_for"] == good["web.request_id"] == "pass"

    # One check blowing up never takes the endpoint down.
    with patch("tempfile.NamedTemporaryFile", side_effect=PermissionError("read-only")):
        broken = {r["id"]: r for r in offline_run({})}
    assert broken["runtime.tmp"]["status"] == "fail" and broken["runtime.tmp"]["detail"] == "PermissionError: read-only"
    assert broken["runtime.threads"]["status"] == "pass"

    # Credentials never reach detail: neither from the URL nor from an exception message that echoes them.
    url = f"mysql://app:{SECRET}@127.0.0.1:1/db"
    with patch("pymysql.connect", side_effect=RuntimeError(f"cannot connect with {url} ({SECRET})")):
        secret_rows = offline_run({}, DATABASE_URL=url)
    database = next(r for r in secret_rows if r["id"] == "services.database")
    assert database["status"] == "fail" and database["detail"].startswith("RuntimeError")
    assert SECRET not in json.dumps(secret_rows), database

    # Unsupported scheme fails cleanly; no DATABASE_URL skips.
    assert next(r for r in offline_run({}, DATABASE_URL="sqlite:///x") if r["id"] == "services.database")["status"] == "fail"
    with patch.dict(os.environ):
        os.environ.pop("DATABASE_URL", None)
        assert next(r for r in offline_run({}) if r["id"] == "services.database")["status"] == "skip"

    # A malformed service URL fails its own rows, never the whole list.
    bad_url = {r["id"]: r["status"] for r in offline_run({}, DATABASE_URL="mysql://[broken")}
    assert bad_url["services.database"] == "fail" and bad_url["runtime.threads"] == "pass"

    # No Redis URL at all is a skip, not a fail.
    with patch.dict(os.environ):
        for name in ("REDIS_URL", "LARAVEL_CLOUD_QUEUES_REDIS_URL"):
            os.environ.pop(name, None)
        assert next(r for r in offline_run({}) if r["id"] == "services.redis")["status"] == "skip"

    # A hung check is reported at the deadline instead of hanging the endpoint.
    import threading
    release = threading.Event()
    with patch.object(checks, "DEADLINE", 0.5), patch("pymysql.connect", side_effect=lambda **_: release.wait(10)):
        started = time.monotonic()
        hung = {r["id"]: r for r in offline_run({}, DATABASE_URL="mysql://u@127.0.0.1:1/db")}
        assert time.monotonic() - started < 3
        # While it is still stuck, the next request reports it instead of starting another probe.
        before = threading.active_count()
        again = {r["id"]: r for r in offline_run({}, DATABASE_URL="mysql://u@127.0.0.1:1/db")}
        assert again["services.database"]["status"] == "warn", again["services.database"]
        assert threading.active_count() <= before
    release.set()
    assert hung["services.database"]["status"] == "fail" and "no answer" in hung["services.database"]["detail"]
    deadline = time.monotonic() + 5
    while "services.database" in checks._running and time.monotonic() < deadline:
        time.sleep(0.05)
    assert "services.database" not in checks._running

    # Listening port: read from /proc on Linux, judged on [::] vs IPv4-only.
    with patch.dict(os.environ, PORT="8000"), patch.object(checks.Path, "is_file", return_value=True):
        with patch.object(checks, "_listeners", return_value={"::"}):
            assert checks.web_port({})[0] == "pass"
        with patch.object(checks, "_listeners", return_value={"0.0.0.0"}):
            assert checks.web_port({})[0] == "fail"
        with patch.object(checks, "_listeners", return_value=set()):
            assert checks.web_port({})[0] == "warn"
    # /proc/net parsing: port 8000 = 0x1F40, state 0A = LISTEN, 01 = ESTABLISHED.
    head = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
    tables = {
        "tcp6": head + "   0: 00000000000000000000000000000000:1F40 00000000000000000000000000000000:0000 0A 0:0 00:0 0 1000 0 1\n",
        "tcp": head + "   0: 0100007F:1F40 00000000:0000 0A 0:0 00:0 0 1000 0 2\n"
                      "   1: 00000000:1F41 00000000:0000 0A 0:0 00:0 0 1000 0 3\n"
                      "   2: 00000000:1F40 0100007F:9999 01 0:0 00:0 0 1000 0 4\n",
    }
    with patch.object(checks.Path, "read_text", lambda self: tables[self.name]):
        assert checks._listeners(8000) == {"::", "0100007F"}
        assert checks._listeners(8001) == {"0.0.0.0"}
    with patch.dict(os.environ, PORT="nope"):
        assert checks.web_port({})[0] == "warn"

    # The endpoint finishes fast even with every network check stubbed.
    started = time.monotonic()
    offline_run({})
    assert time.monotonic() - started < 15

    # Through the router (handle) under the sync/WSGI path.
    import app
    with patch("urllib.request.urlopen", side_effect=OSError("offline")):
        status, _, body = app.handle("GET", "/api/checks", {}, b"")
    assert status == 200
    check_shape(json.loads(body))
    print("checks contract passed")


if __name__ == "__main__":
    main()
