"""Self-check for compat.py on the current interpreter: uv run python test_compat.py"""

from __future__ import annotations

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

for r in results:
    if r["status"] == "fail":
        print(f"fail  {r['name']}: {r['detail']}")
print(f"ok: {len(results)} probes in {elapsed:.1f} s on Python {sys.version.split()[0]}")
