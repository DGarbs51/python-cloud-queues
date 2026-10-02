# Laravel Cloud Python runtime: QA report

Run: 2026-10-01 to 2026-10-02 (UTC). Org `Laravel GTM`, app `python-cloud-queues`, region us-east-2.
Environments: `python-3-10` … `python-3-14` (one per Python minor). Each has an App cluster and a Worker cluster (1 vCPU / 2 GiB, autoscale 1–6 on CPU 60% / memory 70%, 4 queue worker processes), its own Valkey cache, and its own schema on one Laravel MySQL cluster.
Detailed evidence for every item: [FINDINGS.md](FINDINGS.md). How to rerun the tests: [TESTING.md](TESTING.md). Porting guide for FastAPI, Django and Flask: [PORTING.md](PORTING.md).

## Summary

Python on Laravel Cloud works end to end on 3.10–3.14. All five environments deploy from `.python-version`, serve HTTP, run queue workers on Valkey, reach MySQL, autoscale on CPU and memory, and pass the deployment check (sync, async, delayed, retry, failure, timeout with worker restart, 20-job burst).

The defects that matter most are in the platform's lifecycle and the edges around it, not the Python interpreter:

1. **Scaling and shutdown.**
   - Scaling changes only apply on the next deploy (R6).
   - An idle Worker cluster scales up with the App's HTTP traffic (R8).
   - The graceful shutdown timeout, 30 s by default and settable only in the UI, also bounds autoscaler scale-in. Long jobs are therefore killed when replicas are removed (R7).
2. **Networking.**
   - WebSocket upgrades never reach Python apps (N4).
   - The edge returns 403 to Python's standard `urllib` user agent (N2).
3. **Data services.**
   - Laravel Valkey denies `WATCH`/`UNWATCH` (W1).
   - Laravel MySQL presents a self-signed certificate that can't be verified (W2).
   - Dashboard-created caches default to `allkeys-lru`, which can evict queued jobs (W4).
4. **Logging.**
   - Levels come only from a JSON `level` key, so plain Python logs are all `info` and tracebacks split into one entry per line (O1, O3, O5).
   - OOM kills leave no trace in the logs (O6).
   - The logs API caps responses at 100 entries (O2).
5. **CLI/API consistency.**
   - `--scale-to-zero=false` turns scale-to-zero on (C1).
   - Instance and database size names returned by the API can't be written back (R1, R5).
   - Several read endpoints return null or empty fields (C3, C4, R4).

## Results matrix

| Area | Result |
|---|---|
| Deploy 3.10–3.14 from `.python-version` | Pass. A failed deploy command keeps the old release serving. |
| Deployment check (8 cases) on all 5 | Pass. |
| Runtime compatibility (62 probes, web + worker, 5 versions) | Pass, except: MySQL TLS verification fails everywhere (W2); the 3.10 Worker has no timezone data (P3). |
| Queue throughput, 1 replica | 36 jobs/s for 100 ms jobs (ceiling 40). Async = sync, because the worker runs one job per process. |
| CPU-bound jobs | 4.9 jobs/s × 200 ms = exactly 1 vCPU, on every Python version. |
| CPU autoscaling | 1→6 replicas in ~135 s; linear throughput (~30 jobs/s at 6). Scale-in ~15 min. |
| Memory autoscaling | One replica at 95% added a second within ~33 s; the full replica is not relieved. |
| OOM | Whole Worker container restarts; every in-flight job on it is lost; no log signal (O6). |
| Graceful shutdown | 30 s: a 7-minute job was killed by scale-in. 600 s: 24 × 8-minute jobs all completed during scale-in. |
| End to end under load (3.11, k6 e2e.js) | Pass: 10,217/10,217 checks. Dashboard readers (10,201 requests), a 500-job queue run (done, 0 failed) and a Run check (all 8 cases) concurrently. |
| Redeploy during jobs | 8/8 processed, 0 duplicates. Old workers finish while the new release takes queued jobs. |
| DB (sync vs async, pooled) | Both 35–70 jobs/s. Queue overhead dominates, not DB I/O. |
| HTTP ingress (stdlib server, from a laptop) | Light routes: 600 req/s, 0 errors. A Valkey-heavy route collapsed (504 at the 20 s HTTP timeout). App scaled late. |
| Servers | gunicorn and uvicorn honor `WEB_CONCURRENCY=3` without double counting. SSE streams past the 20 s timeout. 100 MB upload OK. WebSockets fail (N4). |
| Cold start (flex + scale-to-zero) | 5.6–7.5 s to first byte vs 0.12 s warm. |
| Logging | JSON lines with `level` and `msg` work well: levels, single-entry tracebacks, 256 KiB lines, Unicode. Everything else is `info`. |

## Recommendations: running Python well on Laravel Cloud today

1. **Log JSON.** Use [`cloud_logging.py`](cloud_logging.py):
   - It needs only the standard library: call `configure()` at startup, or pass its return value to gunicorn as `logconfig_dict`.
   - It routes app, gunicorn, uvicorn, Django, Celery and warnings logs through one JSON handler with Cloud's level names.
   - It was verified on Cloud with uvicorn, gunicorn and queue workers.
2. **Size workers for 1 vCPU.**
   - CPU work scales only with replicas.
   - `WEB_CONCURRENCY=3` suits I/O-bound apps.
   - Each open SSE stream holds a sync gunicorn worker.
3. **Keep jobs shorter than the graceful shutdown timeout.**
   - Raise the timeout (up to 600 s, UI only) for long jobs.
   - Split or checkpoint anything longer.
   - It applies to scale-in too.
4. **Redeploy after changing scaling or timeout settings.** They do not apply until then.
5. **Use a `noeviction` cache for queues**, or a separate cache, when Valkey backs a queue.
6. **Avoid `WATCH`-based redis-py transactions** on Laravel Valkey; use Lua scripts or `MULTI`/`EXEC`.
7. **Read `DATABASE_URL`** (`mysql://`, no TLS parameters).
   - Set the driver scheme yourself and enable TLS.
   - Verification against Laravel MySQL currently fails (W2).
8. **Run uvicorn or gunicorn with forwarded headers** (`--proxy-headers --forwarded-allow-ips='*'`) so logs show real client IPs.
9. **Set a custom `User-Agent`** for health checks and service-to-service calls (N2).
10. **For real-time updates use SSE or polling**, or a Cloud WebSocket cluster, until N4 is fixed.

## Suggested platform fixes (ranked)
1. **WebSockets for Python (N4):** forward the `Upgrade` and `Connection` headers in the Python nginx template.
2. **Autoscaling (R6, R8):**
   - Apply scaling settings on save, or mark them as pending.
   - Scale each cluster on its own metrics.
3. **Graceful shutdown (R7):**
   - Document that the timeout governs scale-in.
   - Expose both timeouts in the CLI/API.
   - Consider a longer maximum, or waiting for the worker to drain.
4. **Logging levels (O5):**
   - Recognize `warn`, `fatal`, `notice`, numeric levels and the `severity`/`levelname` keys.
   - Consider treating stderr as warning.
   - Group tracebacks.
5. **Valkey ACL (W1):** allow `WATCH`/`UNWATCH`, or document the ACL.
6. **Laravel MySQL TLS (W2):** publish a verifiable CA, or use a publicly trusted certificate.
7. **OOM visibility (O6):** log OOM kills at error level and expose a metric.
8. **CLI fixes:**
   - Boolean flags (C1).
   - Size discovery and round-trip (R1, R5, C2).
   - Logs pagination (O2) and a consistent empty-window shape (O4).
9. **SDK (`laravel-cloud-queues`):**
   - Add `level` to the JSON job events.
   - Offer JSON logging.
   - Document one job per process.

## Delivered
- **Test app** (`main`, all five `python-3.x` branches deployed):
  - load jobs: sync, async, CPU, memory, DB sync/async
  - admission and caps
  - stdlib, gunicorn and uvicorn server modes
  - structured logging
  - log pipeline probes: `/api/logtest`, `/api/logtest/raw`
  - runtime compatibility probes
  - dashboard panels for all of the above
- **k6 suite** (`k6/`): http, queue, e2e, coldstart, memory, compat, stream, logs, plus `run-fleet.sh`.
- **Scripts:** `scripts/logcheck.py`, which pages the capped logs API and verifies log delivery and format.
- **Docs PR** [laravel/cloud-docs#360](https://github.com/laravel/cloud-docs/pull/360) (open, not merged):
  - Python best practices and Python logging guides, including the `cloud_logging.py` sample.
  - Unverified statements are marked `TODO verify`.

## Current state of the fleet
- **Python 3.10, 3.11, 3.13:** stdlib server, `logs.py` JSON logging.
- **Python 3.12:** gunicorn + `LOG_CONFIG=sample` (live demo of the docs sample).
- **Python 3.14:** graceful shutdown 600 s (set in the UI).
- **All environments:**
  - `DB_SSL_VERIFY=0` (W2).
  - Autoscale 1–6 on both clusters. Idle replicas return to 1 on their own.
- **Overnight decisions** made without the user are logged in Solo scratchpad `decisions-overnight` (id 91).

## Not covered
- Framework fixture apps (Django and Flask fixtures planned; the FastAPI sibling repo exists).
- Packaging matrix: `requirements.txt`, Poetry, native sdists.
- Managed queue backend.
- Rollback with stale dependencies.
- Dashboard UI walkthrough.
- A job enqueued by another producer while the environment sleeps.
- Long-lived WebSocket and idle-timeout behavior beyond what N4 blocks.
