"""App logging setup (laravel-cloud-logging) plus the logtest probe formats, redaction and context."""
from __future__ import annotations

import atexit
import contextvars
import hashlib
import io
import json
import logging
import os
import platform
import re
import socket
import subprocess
import sys
import threading
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from urllib.parse import quote, unquote

from laravel_cloud_logging import configure

CONTEXT = contextvars.ContextVar("log_context", default={})
LOGGER = logging.getLogger("cloud_demo")
LOCK = threading.RLock()
FORMATS = ("json", "text", "logfmt")
CASES = ("levels", "stderr_vs_stdout", "traceback", "exception_group", "unicode", "ansi",
         "long_lines", "embedded_newline", "json_nested", "print_unflushed", "burst",
         "secret_sentinel", "large_extra", "crash_line")
UNICODE = "Hello 👋 中文 العربية e\u0301"
TAG = re.compile(r"\[logtest ([0-9a-f-]+) (json|text|logfmt) (web|worker) (\w+) (\d+)\] ")


def timestamp(at=None):
    return datetime.fromtimestamp(time.time() if at is None else at, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _settings():
    defaults = dict(format="json" if "LARAVEL_CLOUD" in os.environ else "text", level="INFO", stream="stdout")
    choices = dict(format=FORMATS, level=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), stream=("stdout", "stderr"))
    config, invalid = {}, []
    for key, default in defaults.items():
        name = "LOG_" + key.upper()
        value = os.environ.get(name, default)
        if key == "level":
            value = value.upper()
        if value not in choices[key]:
            invalid.append(name)
            value = default
        config[key] = value
    return config, invalid


def settings():
    return _settings()[0]


def secret_values():
    secrets = set()
    for name, secret in os.environ.items():
        if len(secret) >= 6 and (name in ("DATABASE_URL", "REDIS_URL") or name.endswith(("_PASSWORD", "_SECRET", "_TOKEN"))):
            secrets.update(value for value in (secret, quote(secret, safe=""), unquote(secret)) if len(value) >= 6)
    return sorted(secrets, key=len, reverse=True)


def redact(value, secrets=None):
    # Scan the environment once per record, including nested fields and object strings.
    if secrets is None:
        secrets = secret_values()
    if isinstance(value, dict):
        return {redact(str(k), secrets): redact(v, secrets) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [redact(v, secrets) for v in value]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    value = str(value)
    for secret in secrets:
        value = value.replace(secret, "[REDACTED]")
    # Tolerate unencoded / in userinfo too; the last @ separates password from host.
    return re.sub(r"(?<![a-zA-Z0-9+.-])([a-zA-Z][a-zA-Z0-9+.-]*://[^\s/@:]*:)[^\s]*(@)", r"\1[REDACTED]\2", value)


@contextmanager
def context(**values):
    token = CONTEXT.set({**CONTEXT.get(), **values})
    try:
        yield
    finally:
        CONTEXT.reset(token)


class RedactionFilter(logging.Filter):
    def __init__(self, role):
        super().__init__()
        self.role = role
        self.static = dict(release=os.environ.get("LARAVEL_CLOUD_COMMIT_SHA", "")[:7],
                           env=os.environ.get("LARAVEL_CLOUD_ENV_NAME", ""),
                           host=socket.gethostname(), python=platform.python_version())

    def filter(self, record):
        # Filters run outside StreamHandler.emit's error guard. Never let bad log data
        # interrupt a request/job, or echo the offending args in a diagnostic traceback.
        try:
            data = dict(ts=timestamp(record.created), level=record.levelname, logger=record.name,
                        role=self.role, pid=os.getpid(), **self.static)
            data.update(CONTEXT.get())
            fields = getattr(record, "fields", {})
            data["extra"] = fields if isinstance(fields, dict) else {"fields": fields}
            data["msg"] = record.getMessage()
            if record.exc_info:
                data["exc"] = "".join(traceback.format_exception(*record.exc_info))
            record.safe_data = redact(data)
        except Exception as exc:
            record.safe_data = dict(ts=timestamp(), level="ERROR", logger="cloud_demo",
                                    role=self.role, pid=os.getpid(), msg="log record formatting failed",
                                    extra={"error_type": type(exc).__name__})
        return True


def pairs(data):
    def value_text(value):
        if isinstance(value, (dict, list, tuple)):
            value = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
        return json.dumps(value, ensure_ascii=False, default=str)
    return " ".join(f"{key}={value_text(value)}" for key, value in data.items())


class Formatter(logging.Formatter):
    def __init__(self, fmt):
        super().__init__()
        self.fmt = fmt

    def format(self, record):
        data = record.safe_data
        if self.fmt == "json":
            rendered = json.dumps(data, ensure_ascii=False, default=str, separators=(",", ":"))
        elif self.fmt == "logfmt":
            rendered = pairs({**{k: v for k, v in data.items() if k != "extra"}, **data["extra"]})
        else:
            rendered = (f"{data['ts']} {data['level']} {data['logger']} [{data['role']} {data['pid']}] "
                        + pairs({k: v for k, v in data.items() if k not in ("msg", "exc")})
                        + " msg=" + data["msg"])
            if "exc" in data:
                rendered += "\n" + data["exc"].rstrip("\n")
        # Text continuations need identity too: count physical entries per logical record.
        fields = data["extra"]
        if self.fmt == "text" and "marker" in fields:
            tag = f"[logtest {fields['marker']} text {data['role']} {fields['case']} {fields['seq']}] "
            rendered = "\n".join(tag + line for line in rendered.split("\n"))
        return rendered


def handler(role, fmt, stream=None):
    result = logging.StreamHandler(stream if stream is not None else getattr(sys, settings()["stream"]))
    result.addFilter(RedactionFilter(role))
    result.setFormatter(Formatter(fmt))
    return result


class ContextFilter(logging.Filter):
    """Copy role and logs.context() values into the record; laravel-cloud-logging puts them in context."""

    def __init__(self, role):
        super().__init__()
        self.role = role

    def filter(self, record):
        # extra= wins over request/job context, so a field named like a context key never raises.
        for key, value in {"role": self.role, **CONTEXT.get()}.items():
            record.__dict__.setdefault(key, value)
        return True


def setup(role: str):
    """App logging goes through laravel-cloud-logging; the format/stream settings only drive the probes."""
    if role not in ("web", "worker"):
        raise ValueError("role must be web or worker")
    with LOCK:
        if not getattr(setup, "registered", False):
            setup.registered = True
            atexit.register(lambda: LOGGER.info("shutdown"))
        configure()  # replaces the root handlers, so every call adds the filter to fresh ones
        for owned in logging.getLogger().handlers:
            owned.addFilter(ContextFilter(role))
    return LOGGER


def probe_tag(marker, fmt, role, case, seq):
    return f"[logtest {marker} {fmt} {role} {case} {seq}] "


def emit_tests(marker, fmt, burst, role):
    """Dedicated DEBUG logger per call: no global level/handler changes or logger leaks."""
    for selected in FORMATS if fmt == "all" else (fmt,):
        probe = logging.Logger("cloud_demo.logtest", logging.DEBUG)
        output = handler(role, selected)
        probe.addHandler(output)
        seq = 0

        def emit(case, msg="probe", level=logging.INFO, exc_info=None, **fields):
            nonlocal seq
            seq += 1
            fields = dict(marker=marker, format=selected, case=case, seq=seq, **fields)
            probe.log(level, msg, extra={"fields": fields}, exc_info=exc_info)

        def raw(case, destination, msg, **fields):
            nonlocal seq
            seq += 1
            data = dict(ts=timestamp(), marker=marker, format=selected, role=role,
                        case=case, seq=seq, msg=msg, **fields)
            # Only fake sentinel credentials bypass the filter, never application inputs.
            print(json.dumps(data, ensure_ascii=False) + "\n", end="", file=destination, flush=case != "print_unflushed")

        try:
            emit("manifest", burst_expected=burst, cases=CASES, formats=list(FORMATS) if fmt == "all" else [fmt], role=role)
            for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL):
                emit("levels", logging.getLevelName(level), level=level)
            raw("stderr_vs_stdout", sys.stdout, "raw stdout", stream="stdout")
            raw("stderr_vs_stdout", sys.stderr, "raw stderr", stream="stderr")
            def nested():
                raise RuntimeError("nested logtest traceback")
            try:
                nested()
            except RuntimeError:
                emit("traceback", "nested exception", level=logging.ERROR, exc_info=True)
            try:
                if sys.version_info >= (3, 11):
                    raise ExceptionGroup("logtest group", [ValueError("first"), RuntimeError("second")])
                try:
                    nested()
                except RuntimeError as exc:
                    raise ValueError("chained logtest exception") from exc
            except Exception:
                emit("exception_group", "group or chain", level=logging.ERROR, exc_info=True)
            emit("unicode", UNICODE)
            emit("ansi", "\x1b[31mred\x1b[0m")
            for size in (4096, 16384, 65536, 262144):
                payload = "x" * size
                emit("long_lines", payload_bytes=size, sha256=hashlib.sha256(payload.encode()).hexdigest(), payload=payload)
            emit("embedded_newline", "first\nsecond\nthird")
            emit("json_nested", nested={"array": [1, True, None, {"hello": "world"}]})
            raw("print_unflushed", sys.stdout, "unflushed print", pythonunbuffered=os.environ.get("PYTHONUNBUFFERED"))
            time.sleep(1)
            emit("print_unflushed", "one second after print")
            started = time.monotonic()
            for index in range(burst):
                emit("burst", index=index, burst_expected=burst)
            emit("burst_summary", burst_expected=burst, duration_ms=round((time.monotonic() - started) * 1000, 3))
            sentinel = f"mysql://probe:SENTINEL_PW_{marker}@example.invalid/db"
            emit("secret_sentinel", sentinel, source="filtered")
            raw("secret_sentinel", sys.stdout, sentinel, source="raw_control")
            emit("large_extra", fields={f"field_{n}": f"value_{n}" for n in range(200)})
            seq += 1
            child_record = dict(ts=timestamp(), marker=marker, format=selected, role=role,
                                case="crash_line", seq=seq, msg="immediately before os._exit(1)")
            # Inherit the actual stdout pipe/buffering environment; do not capture/reprint it.
            child = subprocess.run([sys.executable, "-c",
                                    "import os,sys; print(sys.argv[1] + chr(10), end=''); os._exit(1)",
                                    json.dumps(child_record)], timeout=10, check=False)
            emit("crash_result", returncode=child.returncode, pythonunbuffered=os.environ.get("PYTHONUNBUFFERED"))
        finally:
            output.close()


def self_check():
    from unittest.mock import patch
    record = logging.LogRecord("cloud_demo", logging.ERROR, __file__, 1,
                               "mysql://user:SENTINEL@example.invalid/db %s", ("token-value",), None)
    try:
        raise ValueError("password-value\nnext")
    except ValueError:
        record.exc_info = sys.exc_info()
    record.fields = {"nested": ["password-value", "token-value", "redis://user:other@host/0"]}
    with patch.dict(os.environ, {"DEMO_PASSWORD": "password-value", "DEMO_TOKEN": "token-value",
                                "DATABASE_URL": "mysql://hidden@host/db"}):
        for fmt in FORMATS:
            stream = io.StringIO()
            out = handler("web", fmt, stream)
            with context(request_id="request-1"):
                out.handle(record)
            rendered = stream.getvalue()
            assert all(secret not in rendered for secret in ("SENTINEL", "password-value", "token-value", ":other@"))
            assert (len(rendered.splitlines()) > 1) == (fmt == "text")
            if fmt == "json":
                data = json.loads(rendered)
                assert data["request_id"] == "request-1" and "ValueError" in data["exc"]
            if fmt == "logfmt":
                assert r'\n' in rendered and 'exc="' in rendered
        assert redact("mysql://hidden@host/db") == "[REDACTED]"
        assert "password" not in redact("redis://:password@localhost/0")
        assert "p%40ss" not in redact("mysql://user:p%40ss@host/db")
    class DSN:
        def __str__(self):
            return "redis://u:p/w@h"

    with patch.dict(os.environ, {"SHORT_TOKEN": "ab", "DEMO_TOKEN": "sensitive-token"}):
        assert redact("abacus label") == "abacus label"
        assert redact("redis://u:p/w@h") == "redis://u:[REDACTED]@h"
        for fmt in FORMATS:
            stream = io.StringIO()
            out = handler("web", fmt, stream)
            for fields in (None, [DSN()], DSN(), {"nested": [DSN(), "sensitive-token"]}):
                record = logging.LogRecord("test", logging.INFO, __file__, 1, "safe", (), None)
                record.fields = fields
                with patch(__name__ + ".secret_values", wraps=secret_values) as scanned:
                    out.handle(record)
                    scanned.assert_called_once()
            rendered = stream.getvalue()
            assert "p/w" not in rendered and "sensitive-token" not in rendered
            assert "log record formatting failed" not in rendered
            stream.seek(0)
            stream.truncate(0)
            out.handle(logging.LogRecord("test", logging.INFO, __file__, 1, "%d", ("bad-argument",), None))
            assert "log record formatting failed" in stream.getvalue()
            assert "bad-argument" not in stream.getvalue()
    from laravel_cloud_logging import CloudHandler
    setup("web")
    setup("worker")
    root = logging.getLogger().handlers
    assert len(root) == 1 and isinstance(root[0], CloudHandler) and LOGGER.propagate
    assert [f.role for f in root[0].filters if isinstance(f, ContextFilter)] == ["worker"]
    record = LOGGER.makeRecord(LOGGER.name, logging.INFO, __file__, 1, "x", (), None, extra={"run": "extra"})
    with context(run="context", job="demo"):
        root[0].filters[0].filter(record)
    assert (record.role, record.run, record.job) == ("worker", "extra", "demo")
    assert CONTEXT.get() == {}
    setup("web")
    print("Logging formatter, redaction, context and idempotency checks passed")
