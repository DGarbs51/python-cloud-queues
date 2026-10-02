"""Times each event of /api/stream as it arrives, which k6 cannot do (it buffers bodies).

    BASE_URL=https://python-cloud-queues-3-14.laravel-demo.cloud python3 k6/sse.py
    STREAM_S=30 INTERVAL=1 python3 k6/sse.py

Fails (exit 1) when the stream is not 200 text/event-stream, ends early (the 20 s proxy cut),
delivers its first event late or has a gap longer than three intervals (proxy buffering).
Standard library only, so it runs on any Python 3.10+.
"""

from __future__ import annotations

import os
import sys
import time
import urllib.error
import urllib.request

base = os.environ.get("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
seconds = int(os.environ.get("STREAM_S", "30"))
interval = float(os.environ.get("INTERVAL", "1"))
expected = int(seconds / interval)
url = f"{base}/api/stream?seconds={seconds}&interval={interval:g}"

status, content_type = 0, ""
arrivals = []
start = time.monotonic()
try:
    with urllib.request.urlopen(url, timeout=seconds + 60) as response:
        status, content_type = response.status, response.headers.get("Content-Type", "")
        for line in response:
            if line.startswith(b"data:"):
                arrivals.append(time.monotonic() - start)
                print(f"{arrivals[-1]:7.3f} s  {line.decode().strip()}", flush=True)
except urllib.error.HTTPError as exc:
    status, content_type = exc.code, exc.headers.get("Content-Type", "")
except OSError as exc:  # connection reset or timeout mid-stream: judge what arrived
    print(f"stream ended with {exc!r}")

gaps = [b - a for a, b in zip(arrivals, arrivals[1:])]
first = arrivals[0] if arrivals else None
checks = {
    "status 200": status == 200,
    "content-type text/event-stream": content_type.startswith("text/event-stream"),
    f"all {expected} events arrived (not cut at the proxy timeout)": len(arrivals) >= expected,
    "first event within 3 intervals (not buffered)": first is not None and first <= 3 * interval,
    "no gap over 3 intervals (not buffered)": bool(gaps) and max(gaps) <= 3 * interval,
}
print(f"\n{url}\nstatus {status}, {len(arrivals)}/{expected} events, first event "
      f"{first if first is None else round(first, 3)} s, max gap {round(max(gaps), 3) if gaps else None} s")
for name, ok in checks.items():
    print(f"  {'ok  ' if ok else 'FAIL'} {name}")
sys.exit(0 if all(checks.values()) else 1)
