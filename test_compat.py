"""Self-check for compat.py on the current interpreter: uv run python test_compat.py"""

from __future__ import annotations

import json
import os
import sys
import time

import compat

EXPECTED = {
    "interpreter", "tomllib", "except*", "PEP 695 type parameters", "t-strings (PEP 750)",
    "concurrent.interpreters", "compression.zstd", "ssl", "zoneinfo", "multiprocessing spawn",
    "DNS pypi.org", "HTTPS default ssl context", "SIGTERM handler", "DB TLS connect", "import sqlalchemy",
}

started = time.monotonic()
results = compat.run_probes()
elapsed = time.monotonic() - started

names = [r["name"] for r in results]
assert len(names) == len(set(names)), "duplicate probe names"
assert EXPECTED <= set(names), f"missing probes: {EXPECTED - set(names)}"
assert elapsed <= 30, f"run_probes took {elapsed:.1f} s"
for r in results:
    assert set(r) == {"name", "group", "min_python", "status", "detail"}, r
    assert r["status"] in {"pass", "fail", "skip", "info"}, r
    assert all(isinstance(v, str) for v in r.values()), r
    major, minor = map(int, r["min_python"].split("."))
    if sys.version_info < (major, minor):
        assert r["status"] == "skip", r
    # Language-feature probes are deterministic for a given interpreter, so a failure there is a probe bug.
    if r["group"] in {"version", "syntax"}:
        assert r["status"] != "fail", r

# Credentials must never reach a detail string, even when a URL is malformed and the parser's error echoes it.
# U+FF0F (fullwidth solidus) makes urlsplit() raise a ValueError that quotes the whole netloc.
os.environ.update(
    DATABASE_URL="mysql://dbuser:SYNTHETIC_DB%40PW@db\uff0f.invalid/app",
    REDIS_URL="redis://:SYNTHETIC_REDIS_PW@cache\uff0f.invalid:6379/0",
    DB_PASSWORD="SYNTHETIC_RAW/PW",
)
leaked = json.dumps(compat.run_probes())
for secret in ("SYNTHETIC", "dbuser", "DB%40PW", "DB@PW", "RAW/PW", "RAW%2FPW"):
    assert secret not in leaked, f"{secret!r} leaked into probe details"
assert compat._sanitize("see https://u:p@host/x and redis://:pw@h") == "see https://***@host/x and redis://***@h"
for name in ("DATABASE_URL", "REDIS_URL", "DB_PASSWORD"):
    del os.environ[name]

for r in results:
    if r["status"] == "fail":
        print(f"fail  {r['name']}: {r['detail']}")
print(f"ok: {len(results)} probes in {elapsed:.1f} s on Python {sys.version.split()[0]}")
