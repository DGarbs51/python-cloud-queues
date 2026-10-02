#!/usr/bin/env python3
"""Inspect captured log lines or Cloud's capped log API. Findings never fail the command."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import logs


def instant(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def capture(path):
    raw = Path(path).read_text(encoding="utf-8", errors="replace")
    if raw.lstrip().startswith("[") and not raw.lstrip().startswith("[logtest "):
        try:
            entries = json.loads(raw)
        except ValueError:
            entries = None
        if isinstance(entries, list):
            return entries
    entries = []
    for line in raw.split("\n"):
        if not line:
            continue
        try:
            obj = json.loads(line)
        except ValueError:
            obj = None
        entries.append(obj if isinstance(obj, dict) and "message" in obj and "loggedAt" in obj
                       else dict(message=line, level=None, type="local", loggedAt=None, data={}))
    return entries


def cloud(args):
    start = instant(args.since).replace(microsecond=0)
    end = (instant(args.until) if getattr(args, "until", None)
           else datetime.now(timezone.utc).replace(microsecond=0) + timedelta(seconds=args.slack))
    if start > end:
        raise ValueError("--since is in the future")
    entries, capped, api_orders = [], [], []
    seen = set()

    def window(low, high):
        # Wait through the final slack window; a query with a future --to cannot see future logs.
        while datetime.now(timezone.utc) < high:
            time.sleep(min(1, (high - datetime.now(timezone.utc)).total_seconds()))
        command = ["cpx", "cloud", "environment:logs", args.app, args.env,
                   "--from=" + low.isoformat(), "--to=" + high.isoformat(), "--json"]
        for attempt in range(3):
            try:
                result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=60)
                break
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)
        batch = json.loads(result.stdout)
        if isinstance(batch, dict) and isinstance(batch.get("logs"), list):
            batch = batch["logs"]  # an empty window comes back as {"logs": []} instead of []
        if not isinstance(batch, list) or any(not isinstance(e, dict) or "message" not in e or "loggedAt" not in e for e in batch):
            raise ValueError("unexpected cpx log response (expected an array of log entries)")
        seconds = int((high - low).total_seconds())
        if len(batch) >= 100 and seconds > 1:
            middle = low + timedelta(seconds=max(1, seconds // 2))
            window(low, middle)
            window(middle, high)
            return
        if len(batch) >= 100:
            capped.append(dict(start=low.isoformat(), end=high.isoformat(), entries=len(batch)))
        api_orders.append(dict(start=low.isoformat(), end=high.isoformat(), order=entry_order(batch)))
        # Keep inclusive overlap: displayed seconds may be rounded rather than truncated.
        # Dedupe exact matches across windows, preserving duplicates within one response.
        fingerprints = [(e["loggedAt"], e.get("type"), e["message"]) for e in batch]
        entries.extend(e for e, fingerprint in zip(batch, fingerprints) if fingerprint not in seen)
        seen.update(fingerprints)

    while start < end:
        stop = min(start + timedelta(seconds=5), end)
        window(start, stop)
        start = stop
    entries.sort(key=lambda e: instant(e["loggedAt"]))
    return entries, capped, api_orders


def entry_order(entries):
    if not entries or any(not e.get("loggedAt") for e in entries):
        return "unavailable"
    times = [instant(e["loggedAt"]) for e in entries]
    if len(set(times)) == 1:
        return "tied"
    if times == sorted(times):
        return "ascending"
    if times == sorted(times, reverse=True):
        return "descending"
    return "mixed"


def parse_pairs(text):
    data, position = {}, 0
    decoder = json.JSONDecoder()
    while position < len(text):
        match = re.match(r"\s*(\w+)=", text[position:])
        if not match:
            break
        position += match.end()
        try:
            value, used = decoder.raw_decode(text[position:])
        except ValueError:
            if match[1] == "msg":
                data["msg"] = text[position:]
            break
        data[match[1]] = value
        position += used
    return data


def parse(message):
    tag = logs.TAG.match(message)
    identity = {}
    if tag:
        marker, fmt, role, case, seq = tag.groups()
        identity = dict(marker=marker, format=fmt, role=role, case=case, seq=int(seq))
        message = message[tag.end():]
    try:
        data = json.loads(message)
        if not isinstance(data, dict):
            data = {}
    except ValueError:
        start = message.find('ts=')
        data = parse_pairs(message[start:]) if start >= 0 else {}
    extra = data.pop("extra", {})
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except ValueError:
            extra = {}
    if isinstance(extra, dict):
        data.update(extra)
    if "marker" not in data:
        # Identity precedes the large payload so even truncated JSON/logfmt is attributable.
        for key in ("marker", "format", "role", "case", "seq", "ts", "level", "payload_bytes", "sha256"):
            match = re.search(r'"' + key + r'"\s*:\s*("(?:\\.|[^"\\])*"|\d+)', message)
            if match:
                data[key] = json.loads(match[1])
    return {**data, **identity}


def expected(case, burst):
    return {"levels": 5, "stderr_vs_stdout": 2, "long_lines": 4, "print_unflushed": 2,
            "secret_sentinel": 2, "burst": burst}.get(case, 1)


def analyze(entries, marker, fmt, where, burst=None):
    input_order = entry_order(entries)
    if input_order != "unavailable":
        entries = sorted(entries, key=lambda e: instant(e["loggedAt"]))
    groups = defaultdict(list)
    observed = []
    unattributed = defaultdict(list)
    for entry in entries:
        message = str(entry.get("message", ""))
        data = entry.get("data")
        if isinstance(data, dict) and isinstance(data.get("extra"), dict):
            data = dict(data["extra"], **{k: v for k, v in data.items() if k != "extra"})
        if isinstance(data, dict) and data.get("marker") == marker:
            # Cloud parses JSON lines: msg/message becomes the entry message, level becomes the
            # entry level, and every other field moves into data.
            record = dict(data, msg=message, platform_parsed=True)
        elif marker not in message:
            if entry.get("loggedAt"):
                second = instant(entry["loggedAt"]).replace(microsecond=0)
                unattributed[(second, entry.get("type"))].append(len(message.encode()))
            continue
        else:
            record = parse(message)
        if record.get("marker") != marker or record.get("format") != fmt or "seq" not in record:
            continue
        role = record.get("role", "unknown")
        groups[(role, record["seq"])].append((entry, record))
        observed.append((role, record["seq"]))
    roles = ("web", "worker") if where == "both" else (where,)
    rows = []
    for role in roles:
        records = [(seq, parts) for (r, seq), parts in groups.items() if r == role]
        manifest = next((parts[0][1] for _, parts in records if parts[0][1].get("case") == "manifest"), {})
        wanted_burst = burst if burst is not None else manifest.get("burst_expected")
        for case in logs.CASES:
            found = [(seq, parts) for seq, parts in records if parts[0][1].get("case") == case]
            items = [parts[0][1] for _, parts in found]
            physical = [entry for _, parts in found for entry, _ in parts]
            lengths = [len(str(e.get("message", "")).encode()) for e in physical]
            sequence = [seq for r, seq in observed if r == role and any(seq == n for n, _ in found)]
            # A multiline record repeats its seq. Return-order evidence is distinct from timestamp order.
            compact = [n for index, n in enumerate(sequence) if index == 0 or n != sequence[index - 1]]
            latencies = []
            for _, parts in found:
                entry, item = parts[0]
                if entry.get("loggedAt") and item.get("ts"):
                    latencies.append(round((instant(entry["loggedAt"]) - instant(item["ts"])).total_seconds(), 3))
            row = dict(role=role, case=case, found=bool(found), records=len(found), expected=expected(case, wanted_burst),
                       entries=len(physical), entries_per_record=dict(Counter(str(len(parts)) for _, parts in found)),
                       level_mapping=sorted({f"{item.get('level', 'raw')} -> {entry.get('level') or 'unknown'}"
                                             for _, parts in found for entry, _ in parts for item in [parts[0][1]]}),
                       types=sorted({e.get("type") or "unknown" for e in physical}),
                       order_preserved=compact == sorted(compact) if compact else None,
                       entry_bytes=dict(min=min(lengths), max=max(lengths)) if lengths else None,
                       latency_s=dict(min=min(latencies), max=max(latencies)) if latencies else None)
            if case == "long_lines":
                row["payloads"] = [dict(expected_bytes=item.get("payload_bytes"),
                                        observed_bytes=len(item["payload"].encode()) if "payload" in item else None,
                                        sha256_match=hashlib.sha256(item["payload"].encode()).hexdigest() == item.get("sha256") if "payload" in item else False)
                                   for item in items]
                seconds = {(instant(e["loggedAt"]).replace(microsecond=0), e.get("type"))
                           for e in physical if e.get("loggedAt")}
                candidates = [size for key in seconds for size in unattributed[key]]
                row["unattributed_same_second"] = dict(entries=len(candidates), bytes=sum(candidates)) if seconds else None
                row["incomplete_payload"] = any(not item["sha256_match"] for item in row["payloads"])
                if row["incomplete_payload"]:
                    row["split_vs_truncation"] = "unknown; same-second unmarked entries may include continuations or unrelated logs"
            elif case == "unicode":
                row["unicode_intact"] = all(item.get("msg") == logs.UNICODE for item in items) if items else None
            elif case == "ansi":
                row["ansi"] = "raw" if any("\x1b[31m" in str(item.get("msg", "")) for item in items) else "stripped_or_missing"
            elif case == "secret_sentinel":
                row["secret_visible"] = {item.get("source", "unknown"): f"SENTINEL_PW_{marker}" in item.get("msg", "") for item in items}
            elif case == "burst":
                summary = next((parts[0][1] for _, parts in records if parts[0][1].get("case") == "burst_summary"), {})
                delivered_at = [instant(e["loggedAt"]) for e in physical if e.get("loggedAt")]
                row.update(delivered=len(found), duration_ms=summary.get("duration_ms"),
                           delivery_span_s=(max(delivered_at) - min(delivered_at)).total_seconds() if delivered_at else None)
            elif case in ("print_unflushed", "crash_line"):
                row["pythonunbuffered"] = next((item.get("pythonunbuffered") for item in items if "pythonunbuffered" in item), None)
                if case == "crash_line":
                    result = next((parts[0][1] for _, parts in records if parts[0][1].get("case") == "crash_result"), {})
                    row.update(returncode=result.get("returncode"), pythonunbuffered=result.get("pythonunbuffered"))
                else:
                    received = [instant(e["loggedAt"]) for e in physical if e.get("loggedAt")]
                    row["delivery_gap_s"] = (max(received) - min(received)).total_seconds() if len(received) >= 2 else None
            elif case == "stderr_vs_stdout":
                row["stream_metadata"] = {item.get("stream", "unknown"): dict(type=entry.get("type"), level=entry.get("level"), data=entry.get("data"))
                                          for _, parts in found for entry, item in parts}
                values = list(row["stream_metadata"].values())
                row["streams_distinguishable"] = len(values) == 2 and values[0] != values[1]
            rows.append(row)
    return dict(format=fmt, marker=marker, rows=rows, input_order=input_order,
                order_basis="loggedAt (stable for ties)" if input_order != "unavailable" else "file order",
                order_by_role={role: [n for r, n in observed if r == role] == sorted(n for r, n in observed if r == role)
                               for role in roles if any(r == role for r, _ in observed)},
                notes=["Order uses stable loggedAt sorting when available; API order is reported separately. Same-second ties cannot prove delivery order.",
                       "Cloud loggedAt has 1-second resolution; subsecond/negative latency is not conclusive.",
                       "Text continuations repeat seq; unmarked entries in long-line seconds are counted separately, not assigned without proof.",
                       "Missing crash_line can demonstrate buffered print loss. Set PYTHONUNBUFFERED=1.",
                       "Raw sentinel is an intentionally fake control. File mode has no platform level/time/stream metadata."])


def table(report):
    print(f"\n{report['format']}  marker={report['marker']}")
    print(f"{'role / case':30} {'found/expected':15} {'entries/record':17} {'order':7} levels / type / findings")
    for row in report["rows"]:
        findings = {key: row[key] for key in ("payloads", "unicode_intact", "ansi", "secret_visible", "delivered", "duration_ms", "delivery_span_s", "latency_s", "streams_distinguishable", "pythonunbuffered", "returncode", "delivery_gap_s", "unattributed_same_second", "incomplete_payload", "split_vs_truncation") if key in row and row[key] is not None}
        print(f"{row['role'] + '/' + row['case']:30} {str(row['records']) + '/' + str(row['expected']):15} "
              f"{str(row['entries_per_record']):17} {str(row['order_preserved']):7} "
              + ', '.join(row["level_mapping"]) + ' / ' + ', '.join(row["types"]) + ' / ' + json.dumps(findings, ensure_ascii=False))


def compare(paths):
    reports = [json.loads(Path(path).read_text()) for path in paths]
    print("role/case                      " + " | ".join(f"{r['format']:24}" for r in reports))
    keys = dict.fromkeys((row["role"], row["case"]) for report in reports for row in report["rows"])
    for role, case in keys:
        cells = []
        for report in reports:
            row = next((r for r in report["rows"] if r["role"] == role and r["case"] == case), None)
            cells.append(f"{row['records']}/{row['expected']} records, {row['entries']} entries" if row else "missing")
        print(f"{role + '/' + case:30} " + " | ".join(f"{cell:24}" for cell in cells))


def self_check():
    marker = "00000000-0000-0000-0000-000000000000"
    payload = "x" * 4096
    records = [dict(ts="2026-10-01T00:00:00.100Z", role="web", level="ERROR", msg=logs.UNICODE,
                    extra=dict(marker=marker, format="json", case="unicode", seq=1)),
               dict(role="web", level="INFO", msg="probe", extra=dict(marker=marker, format="json", case="long_lines", seq=2,
                    payload_bytes=4096, sha256=hashlib.sha256(payload.encode()).hexdigest(), payload=payload))]
    entries = [dict(message=json.dumps(record), level="info", type="system", loggedAt="2026-10-01T00:00:01Z") for record in records]
    report = analyze(entries, marker, "json", "web", 0)
    assert next(r for r in report["rows"] if r["case"] == "unicode")["unicode_intact"]
    assert next(r for r in report["rows"] if r["case"] == "long_lines")["payloads"][0]["sha256_match"]
    entries[-1]["message"] = entries[-1]["message"][:500]
    report = analyze(entries, marker, "json", "web", 0)
    assert not next(r for r in report["rows"] if r["case"] == "long_lines")["payloads"][0]["sha256_match"]
    assert parse(logs.pairs(records[0]))["marker"] == marker
    tag = logs.probe_tag(marker, "text", "web", "traceback", 1)
    split = [dict(message=tag + text) for text in ("traceback", "continuation")]
    row = next(r for r in analyze(split, marker, "text", "web")["rows"] if r["case"] == "traceback")
    assert row["records"] == 1 and row["entries"] == 2
    # Unmarked chunks must not silently disappear from the split/truncation findings.
    logfmt = logs.pairs(dict(ts="2026-10-01T00:00:01Z", role="web", **{**records[1]["extra"], "format": "logfmt"}))
    for fmt, whole in (("json", json.dumps(records[1])), ("logfmt", logfmt)):
        chunks = [dict(message=text, type="system", loggedAt="2026-10-01T00:00:01") for text in (whole[:500], whole[500:])]
        row = next(r for r in analyze(chunks, marker, fmt, "web")["rows"] if r["case"] == "long_lines")
        assert row["incomplete_payload"] and row["unattributed_same_second"] == dict(entries=1, bytes=len(whole[500:].encode()))
        assert "unknown" in row["split_vs_truncation"]
    assert instant("2026-10-01T00:00:01") == instant("2026-10-01T00:00:01Z")
    descending = [dict(message=json.dumps(record), type="system", loggedAt=f"2026-10-01T00:00:0{n}Z")
                  for n, record in enumerate(records)]
    report = analyze(list(reversed(descending)), marker, "json", "web")
    assert report["input_order"] == "descending" and report["order_by_role"]["web"]

    from types import SimpleNamespace
    from unittest.mock import patch
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 1, 0, 0, 10, tzinfo=timezone.utc)
    args = SimpleNamespace(since="2026-10-01T00:00:00Z", slack=0, app="test", env="test")
    calls = []
    def fake_run(command, **kwargs):
        low = instant(next(v.split("=", 1)[1] for v in command if v.startswith("--from=")))
        high = instant(next(v.split("=", 1)[1] for v in command if v.startswith("--to=")))
        calls.append((low, high))
        # Include shared boundaries newest-first, plus a genuine same-response duplicate.
        batch = [dict(message="boundary", type="system", loggedAt=high.isoformat()),
                 dict(message="boundary", type="access", loggedAt=high.isoformat()),
                 dict(message="different", type="system", loggedAt=high.isoformat()),
                 dict(message="boundary", type="system", loggedAt=low.isoformat()),
                 dict(message="boundary", type="system", loggedAt=low.isoformat())]
        return SimpleNamespace(stdout=json.dumps(batch))
    with patch(__name__ + ".datetime", Clock), patch.object(subprocess, "run", side_effect=fake_run):
        collected, capped, orders = cloud(args)
    assert not capped and len(calls) == 2
    assert len(collected) == 8  # Same-second differences in type/message must survive.
    assert sum(instant(e["loggedAt"]).second == 5 for e in collected) == 3
    assert all(window["order"] == "descending" for window in orders)
    assert entry_order(collected) == "ascending"

    calls.clear()
    def capped_run(command, **kwargs):
        result = fake_run(command, **kwargs)
        batch = json.loads(result.stdout)
        batch.extend(dict(message=f"probe-{n}", type="system", loggedAt=batch[-1]["loggedAt"]) for n in range(95))
        return SimpleNamespace(stdout=json.dumps(batch))
    with patch(__name__ + ".datetime", Clock), patch.object(subprocess, "run", side_effect=capped_run):
        collected, capped, orders = cloud(args)
    assert len(capped) == 10 and len(calls) > len(capped)
    assert all((instant(w["end"]) - instant(w["start"])).total_seconds() == 1 for w in capped)

    success = SimpleNamespace(stdout="[]")
    transient = [subprocess.TimeoutExpired("cpx", 60), subprocess.CalledProcessError(1, "cpx"), success, success]
    with patch(__name__ + ".datetime", Clock), patch.object(subprocess, "run", side_effect=transient) as run, \
            patch.object(time, "sleep") as sleep:
        assert cloud(args)[0] == []
        assert run.call_count == 4 and [c.args[0] for c in sleep.call_args_list] == [1, 2]
    with patch(__name__ + ".datetime", Clock), patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired("cpx", 60)) as run, \
            patch.object(time, "sleep"):
        try:
            cloud(args)
            raise AssertionError("exhausted retries must fail")
        except subprocess.TimeoutExpired:
            assert run.call_count == 3
    print("logcheck parsing, truncation/split evidence, overlap, ordering, retries and capped-window checks passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default="local")
    parser.add_argument("--marker")
    parser.add_argument("--since")
    parser.add_argument("--app", default="python-cloud-queues")
    parser.add_argument("--file")
    parser.add_argument("--format", choices=(*logs.FORMATS, "all"), default="all")
    parser.add_argument("--where", choices=("web", "worker", "both"), default="both")
    parser.add_argument("--burst", type=int)
    parser.add_argument("--slack", type=int, default=10)
    parser.add_argument("--until", help="ISO end of the collection window (default: now + slack)")
    parser.add_argument("--compare", nargs="*", metavar="RESULT_JSON")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.env):
        parser.error("invalid environment name")
    if args.compare is not None:
        paths = args.compare or [f"results/logcheck-{args.env}-{fmt}.json" for fmt in logs.FORMATS]
        compare(paths)
        return
    if not args.marker or not re.fullmatch(r"[0-9a-f-]{32,36}", args.marker) or (not args.file and not args.since):
        parser.error("--marker and either --file or --since are required")
    if not 0 <= args.slack <= 300 or (args.burst is not None and not 0 <= args.burst <= 5000):
        parser.error("slack must be 0..300 and burst 0..5000")
    entries, capped, api_orders = (capture(args.file), [], []) if args.file else cloud(args)
    for fmt in logs.FORMATS if args.format == "all" else (args.format,):
        report = analyze(entries, args.marker, fmt, args.where, args.burst)
        report.update(env=args.env, source=args.file or "cloud", capped_windows=capped, api_window_orders=api_orders,
                      collection_complete=not capped, collected_at=logs.timestamp())
        table(report)
        if api_orders:
            print("API window order:", dict(Counter(window["order"] for window in api_orders)))
        if capped:
            print(f"WARNING: {len(capped)} one-second windows hit the 100-entry cap; absence is inconclusive.")
        destination = Path("results") / f"logcheck-{args.env}-{fmt}.json"
        destination.parent.mkdir(exist_ok=True)
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(destination)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"logcheck error: {exc}", file=sys.stderr)
        sys.exit(1)
