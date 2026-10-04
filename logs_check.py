"""Check that every environment's logs render correctly in Cloud's log viewer (#17).

Run: uv run python logs_check.py [env ...] [--since ISO]
With no envs, checks all of them in parallel. Per environment it calls /api/log-test, /api/boom and
/api/boom-thread with a fresh marker, reads the window back with `env:logs`, and scores levels,
exceptions, request IDs, encoding and plain-text noise. --since widens the noise window back to
a deploy, so boot and shutdown lines are counted too. Writes results/logs-<time>.json.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app import LOG_TEST_LEVELS, LOG_TEST_UNICODE

APP = "app-a2daff20-3072-4065-a7fb-08b4e05a5333"
# Cloudflare's browser integrity check rejects Python's default user agent with 403.
HEADERS = {"User-Agent": "Mozilla/5.0"}
# Cloud's level names for the cases /api/log-test logs. DEBUG is below LOG_LEVEL=INFO and must not arrive.
EXPECTED_LEVELS = {name: name for name in LOG_TEST_LEVELS if name != "debug"}
MULTILINE = "log-test multi-line\nsecond line\nthird line"
SETTLE_SECONDS = 20  # Cloud's log API lags a few seconds behind


def cloud(*args: str):
    out = subprocess.run(["cpx", "laravel/cloud-cli", *args, "--json", "-n"], capture_output=True, text=True, timeout=120)
    if out.returncode:
        raise RuntimeError(f"{args[0]} failed: {out.stdout[-500:]} {out.stderr[-500:]}")
    return json.loads(out.stdout)


def logs(env: str, start: datetime, end: datetime) -> list[dict]:
    """Every entry in [start, end]. The API returns at most 100 per query, so query in slices and split full ones."""
    entries: list[dict] = []
    while start < end:
        stop = min(start + timedelta(seconds=10), end)
        entries += _slice(env, start, stop)
        start = stop
    return entries


def _slice(env: str, start: datetime, end: datetime) -> list[dict]:
    batch = cloud("env:logs", APP, env, f"--from={start.isoformat()}", f"--to={end.isoformat()}")
    if isinstance(batch, dict):
        batch = batch.get("logs", [])  # an empty window comes back as {"logs": []}
    if len(batch) >= 100:
        if (end - start).total_seconds() <= 1:
            raise RuntimeError(f"{env}: over 100 log entries in one second at {start}, can't read them all")
        middle = start + (end - start) / 2
        return _slice(env, start, middle) + _slice(env, middle, end)
    return batch


def get(url: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=30) as response:
            return response.status, {k.lower(): v for k, v in response.getheaders()}
    except urllib.error.HTTPError as exc:
        return exc.code, {k.lower(): v for k, v in exc.headers.items()}


def context(entry: dict) -> dict:
    return (entry.get("data") or {}).get("context") or {}


def check(env: str, url: str, since: datetime | None) -> dict:
    marker = f"lc-{uuid.uuid4().hex[:10]}"
    started = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=2)
    status, headers = get(f"{url}/api/log-test?marker={marker}")
    request_id = headers.get("x-request-id", "")
    boom_status, _ = get(f"{url}/api/boom?marker={marker}-boom")
    thread_status, _ = get(f"{url}/api/boom-thread?marker={marker}-thread")
    time.sleep(SETTLE_SECONDS)
    ended = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=1)
    window = logs(env, started, ended)
    noise_window = logs(env, since, started) + window if since else window

    ours = [e for e in window if context(e).get("marker") == marker]
    by_case = {context(e).get("case"): e for e in ours}
    results: dict[str, tuple[bool, str]] = {}

    wrong = {case: (by_case.get(case) or {}).get("level") for case, level in EXPECTED_LEVELS.items()
             if (by_case.get(case) or {}).get("level") != level}
    if "debug" in by_case:
        wrong["debug"] = "arrived, but is below LOG_LEVEL=INFO"
    results["levels"] = (status == 200 and not wrong, f"log-test {status}; wrong: {wrong}" if wrong else f"{len(EXPECTED_LEVELS)} levels match")

    extra = context(by_case.get("extra", {})).get("order")
    results["context"] = (extra == {"id": 42, "items": ["a", "b"]}, f"order={extra!r}")
    results["multiline"] = (by_case.get("multiline", {}).get("message") == MULTILINE,
                            repr(by_case.get("multiline", {}).get("message")))
    unicode = by_case.get("unicode", {}).get("message", "")
    results["encoding"] = (unicode == f"log-test unicode {LOG_TEST_UNICODE}", repr(unicode))

    exception = by_case.get("exception", {})
    detail = context(exception).get("exception") or {}
    # One entry, at error, with the class and message; traceback lines must not arrive as entries of their own.
    stray = [e for e in window if "inner cause" in json.dumps(e) and e is not exception]
    results["traceback"] = (exception.get("level") == "error" and "ValueError" in str(detail.get("class")) and not stray,
                            f"level={exception.get('level')} class={detail.get('class')} message={detail.get('message')!r} stray={len(stray)}")

    for name, tag, code in (("uncaught", f"{marker}-boom", boom_status), ("thread", f"{marker}-thread", thread_status)):
        hits = [e for e in window if f"uncaught boom {tag}" in json.dumps(e)]
        good = [e for e in hits if e.get("type") == "application" and e.get("level") in ("error", "critical", "alert", "emergency")]
        # The request must still be answered: a 500 from the server, not a dropped connection (nginx 502).
        status_ok = code == 500 if name == "uncaught" else code == 200
        results[name] = (len(hits) == 1 and len(good) == 1 and status_ok,
                         f"HTTP {code}; {len(hits)} entries: " + ", ".join(f"{e.get('type')}/{e.get('level')}" for e in hits))

    missing = [context(e).get("case") for e in ours if context(e).get("cloud_request_id") != request_id]
    results["request_id"] = (bool(request_id) and bool(ours) and not missing,
                             f"X-Request-ID={request_id!r}; mismatched: {missing}" if missing else f"{len(ours)} entries carry it")

    plain = [e["message"] for e in noise_window if e.get("type") == "system"]
    results["noise"] = (not plain, f"{len(plain)} plain line(s)")
    return {"env": env, "marker": marker, "results": results, "plain": plain}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("envs", nargs="*")
    parser.add_argument("--since", help="also count plain lines from this time (ISO), e.g. the deploy start")
    args = parser.parse_args()
    since = datetime.fromisoformat(args.since.replace("Z", "+00:00")) if args.since else None
    urls = {e["name"]: e["url"] for e in cloud("env:list", APP)}
    envs = args.envs or sorted(urls)
    with ThreadPoolExecutor(len(envs)) as pool:
        reports = list(pool.map(lambda env: check(env, urls[env], since), envs))

    names = list(reports[0]["results"])
    print(f"{'environment':<22}" + "".join(f"{n:<11}" for n in names))
    for report in reports:
        print(f"{report['env']:<22}" + "".join(f"{'pass' if report['results'][n][0] else 'FAIL':<11}" for n in names))
    for report in reports:
        failed = {n: d for n, (ok, d) in report["results"].items() if not ok}
        if failed or report["plain"]:
            print(f"\n{report['env']} ({report['marker']})")
            for name, detail in failed.items():
                print(f"  {name}: {detail}")
            for line in report["plain"][:15]:
                print(f"  plain: {line[:160]}")
    out = Path("results") / f"logs-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(reports, indent=2, ensure_ascii=False))
    print(f"\nwrote {out}")
    sys.exit(0 if all(ok for r in reports for ok, _ in r["results"].values()) else 1)


if __name__ == "__main__":
    main()
