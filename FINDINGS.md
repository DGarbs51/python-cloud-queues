# Laravel Cloud Python runtime: findings

QA of Laravel Cloud's Python runtime using this repository as the test application. Every entry was reproduced against the `Laravel GTM` organization, application `python-cloud-queues` (us-east-2), with `cpx cloud` (Laravel Cloud CLI) unless noted otherwise.

Severity: **High** blocks or silently breaks a supported workflow; **Medium** wrong behavior with a workaround; **Low** cosmetic or misleading.
Classification: platform, CLI, SDK (`laravel-cloud-queues`), app (this repository), or docs.

## Resources

### R1. Instance size reported by the API cannot be set through the API (Medium, platform)
- Versions: all. Envs: python-3-10, python-3-14 (created in the UI).
- Repro: `cpx cloud instance:list python-3-10` shows `"size":"dedicated.c-1vcpu-2gb"`. `cpx cloud instance:update <new App instance> --size=dedicated.c-1vcpu-2gb` returns `422 {"size":["The size value is invalid."]}`. `instance:sizes` lists no `dedicated.*` sizes; the closest listed size `pro.g-1vcpu-2gb` is accepted.
- Expected: a size the API returns can be written back (round-trips), and `instance:sizes` lists it.
- Actual: read and write use different vocabularies. Scripts that copy an existing environment's configuration fail.

### R2. Existing UI environments combine a dedicated size with hibernation, which the API forbids (Medium, platform)
- Repro: python-3-10/3-14 report `usesHibernation: true` with `dedicated.c-1vcpu-2gb`. Setting `pro.g-1vcpu-2gb` on a new instance with hibernation on returns `422 "The selected size cannot be used in combination with hibernation"`.
- Expected: one rule for UI and API; either the existing state is invalid and should be surfaced, or the API should accept it.
- Impact: cold-start (scale-to-zero) testing is only possible on flex sizes.

### R6. Autoscaling changes report success immediately but take effect only on the next deployment (High, platform/API)
- Repro (python-3-14, Worker cluster): `cpx cloud instance:update <worker> --max-replicas=1 --force` -> API and `instance:list` show `maxReplicas: 1`. With no redeploy, a CPU-saturating run (`POST /api/load {"kind":"cpu","count":2000,"ms":200}`) scaled the Worker cluster to 2 replicas at 02:05:38Z (distinct worker hostnames in run records), and it scaled back in at 02:13:17Z (log: `[Deploy: 7] Worker Cluster cluster shut down...`). After `cpx cloud deploy`, the identical run stayed at 1 replica for the whole 2000 jobs (02:17-02:19Z).
- Expected: scaling settings apply when saved, or the API/CLI/UI marks them as pending until the next deploy.
- Impact: operators lowering max replicas for cost or safety are silently not protected; test baselines are poisoned unless every scaling change is followed by a redeploy. Reported by the user and confirmed.
- Note: scale-in log line is labelled `[Deploy: 7] ... shut down`, which reads like a deployment event.

### R7. Graceful shutdown timeout (30 s default, max 600 s, UI-only) also governs autoscaler scale-in; with the default, long jobs are lost (High, platform/docs)
- UI settings: "HTTP timeout" 20 s (allowed 5-60 s) and "Graceful shutdown timeout" 30 s (allowed 1-600 s). The help text says the shutdown timeout applies to "the previous deployment's compute instances"; neither setting is exposed by `environment:get`/`environment:update` in the CLI/API.
- Repro (python-3-14): 4 jobs `load.mem` holding 450 MiB for 420 s on 6 warm replicas. At ~02:50Z the autoscaler removed replica `1blzzs` while it was ~4 min into its job. The other 3 jobs finished at ~02:53Z; the 4th was recorded `failed` (lost, never redelivered as processed) by 03:04Z. Worker log on scale-in: `Received SIGTERM; stopping after the current job.`; the pod is then killed after the grace period.
- Expected: scale-in waits for in-flight background jobs (or documents that the shutdown timeout bounds job duration and applies to scale-in too).
- Control with graceful shutdown = 600 s (set in the UI on python-3-14, then redeployed): 6 replicas, 24 jobs held 480 s (03:26:05-03:34:05Z). Scale-in began removing Worker replicas at 03:31:13, 03:32:13 and 03:33:13 (`[Deploy: 13] Worker Cluster cluster shut down...`) while the jobs ran; the pods stayed until the jobs finished, and all 24 were processed (0 failed, 0 duplicates). So the setting does govern autoscaler scale-in, contrary to its help text, which mentions only the previous deployment.
- Redeploy control (python-3-11, default 30 s): 8 jobs of 120 s dispatched at 04:25:14Z, deploy started 04:25:43Z. The 4 in-flight jobs kept running on the old release (host 1x9jk8) while the new release came up; the new release (host 159s6f) started taking the 4 queued jobs at ~04:26:59Z; the old jobs finished at ~04:27:15-29Z, about 25 s after the switch, inside the 30 s window. Result 8/8 processed, 0 duplicates. This confirms the grace period counts from when the new deployment becomes active (old instances are not stopped at deploy start). It does not show a kill at 30 s; the scale-in case above does.
- Oddity: the workers' `Received SIGTERM; stopping after the current job.` lines are stamped 03:34:05, when the jobs finished, not when the shutdown started (to investigate: delayed signal delivery vs. log timestamping).
- Impact: any Python job longer than the graceful shutdown timeout (max 600 s) can be killed by scale-in, not just by deploys. Jobs over 10 minutes cannot be protected at all. Scale-in from 6 to 1 replica took ~15 min after load stopped (02:45 -> ~03:00), removing one replica at a time.

### R8. Worker cluster scales out with App HTTP traffic although it has no load (High, platform; cost)
- Repro (python-3-10, App + Worker cluster both custom autoscale 1-6, CPU 60%, memory 70%): HTTP-only k6 load against the App (ramping to 600 req/s, 03:46-03:57Z), no jobs dispatched. `cpx cloud environment:metrics python-3-10 --json` replicaCount: App 1 -> 6 at 03:57, and the Worker cluster 1 -> 6 in the same minute while Worker CPU was 0.3% and memory 5%. At 04:04Z a burst of 60 jobs was processed by 6 distinct worker hosts, confirming 6 live Worker replicas.
- Expected: each cluster scales on its own CPU/memory thresholds; an idle Worker cluster stays at its minimum.
- Impact: HTTP spikes multiply worker cost (up to 6x here) and the extra replicas take ~15 min to scale in.
- Also: the replicaCount series reports 7 replicas for both clusters (04:00:41-04:01:15Z) while max is 6 (surge during replacement, or a metrics error).
- Also: the App scaled only at the very end of the overload (03:57) with App CPU at most 42% (< 60% threshold), so the trigger was not the configured CPU threshold (likely the documented HTTP-request signal); by then /api/stats had already returned 906 x 504.

### R3. New environments default to `flex-512mb` with hibernation and no worker cluster (Low, platform)
- Repro: `cpx cloud environment:create python-cloud-queues --name=python-3-11 --branch=python-3.11` creates one App instance `flex-512mb`, scaling `none`, scale-to-zero on.
- Note: expected for PHP defaults; for Python there is no prompt about workers or start command.

### R4. Attached cache reports no environments (Low, API)
- Repro: `cpx cloud environment:get python-3-10` -> `cacheId: cache-a2db0176-...`; `cpx cloud cache:get cache-a2db0176-...` -> `"environmentIds": []`.
- Expected: the cache lists the environment it is attached to.

### R5. Two naming schemes for MySQL sizes (Low, API)
- Repro: `database-cluster:list` shows sizes `db-flex.m-1vcpu-512mb`, `db-pro.m-2vcpu-8gb` and `mysql-flex-512mb` in the same organization. Updating to `db-pro.m-2vcpu-8gb` is stored as `mysql-pro-8gb`. There is no command to list valid database sizes; invalid sizes return only "The database size is invalid."

## CLI

### C1. `--scale-to-zero=false` enables scale-to-zero (High, CLI)
- Repro: `cpx cloud instance:update <id> --scale-to-zero=false --force` followed by a size change still fails with the hibernation error; `--scale-to-zero=0` disables it.
- Expected: `false` disables; the string "false" is currently truthy.

### C2. `database-cluster:create` has no `--size`, and no size discovery command exists (Medium, CLI)
- Repro: `cpx cloud database-cluster:create --help` lists name/type/engine-version/region only. The cluster is created at `mysql-flex-512mb`; resizing needs `database-cluster:update --size=<guess>`.

### C3. `environment:create` response shows `branch: null` (Low, CLI/API)
- Repro: `environment:create ... --branch=python-3.12 --json` returns `"branch":null`; `environment:list` afterwards shows `"branch":"python-3.12"`.

### C4. `application:get` embeds environments with null branch/instances and empty deployments (Low, API)
- Repro: `cpx cloud application:get python-cloud-queues --json` -> environments with `branch:null`, `instances:null`, `deploymentIds:[]`; `environment:list` returns the real values.

### C5. Commands require an org default per repository when multiple tokens exist (Low, CLI)
- Repro: with two tokens, every command fails until `repo:config --organization=...` writes `.cloud/` into the working tree. The file is not gitignored by the CLI.

## Pods / runtime

### P1. Python environments report PHP and Node versions (Low, platform)
- Repro: every Python environment and deployment record shows `phpMajorVersion: "8.5"`, `nodeVersion: "24"`, `usesOctane: false`.

### P3. Python 3.10 worker cluster has an empty `zoneinfo.TZPATH`; the web role on the same release does not (Medium, platform)
- Env: python-3-10, release ebb44c7. Evidence: `/api/compat` web probe `zoneinfo pass ... TZPATH=['/usr/share/zoneinfo', ...]`; worker probe `zoneinfo fail 'No time zone found with key America/New_York'; source=none TZPATH=[]`. Worker executable is `/usr/local/bin/python3.10` (console script), web is `/usr/local/bin/python`.
- Other versions (3.11-3.14) pass on both roles.
- Expected: identical runtime on every role of a release. Impact: `ZoneInfo(...)` raises only inside queue jobs on 3.10.
- Follow-up: `command:run` cannot target the worker cluster, so the worker environment (e.g. `PYTHONTZPATH`) cannot be inspected directly (see D14).

### P4. CPU and PID limits visible to Python vary by node (Low, platform)
- `os.cpu_count()` reports 4 on some pods and 16 on others (8 earlier), always with cgroup `cpu.max 100000 100000` (1 vCPU). `pids.max` is 9241 on some pods and 151738 on others. Python 3.13+ `os.process_cpu_count()` follows the host count too.
- Impact: pool sizes derived from CPU count differ between deploys of the same code.

### P2. `WEB_CONCURRENCY=3` injected on 1 vCPU (Low, platform/docs)
- Repro: `command:run` -> `WEB_CONCURRENCY=3`; cgroup `cpu.max 100000 100000` (1 vCPU). The host reports `nproc=8`.
- Verified on python-3-12: `SERVER=gunicorn` starts 1 master + 3 sync workers (pids 41-43 answering requests), uvicorn with WEB_CONCURRENCY=3 starts 3 workers; no double counting. But 3 processes on 1 vCPU is only right for I/O-bound apps, and each open SSE stream holds one gunicorn sync worker for its whole duration.

## Build & deploy

### B3. Scale-to-zero cold start for Python (measured) and the Pro-worker restriction (Low, platform/docs)
- python-3-13 with App `flex.g-1vcpu-512mb` + scale-to-zero (timeout 1) and Worker cluster `flex.m-1vcpu-1gb`: after ~6.5 min idle, the first `GET /api/ping` took 5.6 s and 7.5 s (two rounds) to first byte vs 0.12 s warm; a job dispatched right after was processed in 2.5 s.
- Enabling scale-to-zero is refused while any Worker cluster uses a Pro size: `422 "Hibernation cannot be enabled with a Pro CPU configured on your worker clusters."` (yet UI-created dedicated environments report hibernation on, see R2).
- Not tested: a job enqueued by another producer while the environment sleeps (the cache is not publicly reachable).

### B2. Failed deploy command leaves the previous release serving (working as intended, recorded)
- Repro: deployment `depl-a2e1bead...` failed in the deploy command; the site kept serving release a59527d (`/api/stats` 200). Good behavior; noted for the rollback tests.

### B1. New environments report `status: running` before any deployment (Low, platform)
- Repro: `environment:list` immediately after `environment:create` shows `"status":"running"` with no deployment.

## Networking / ingress

### N2. Edge returns 403 to the Python standard-library HTTP client (Medium, platform)
- Repro: `curl -A "Python-urllib/3.14" https://python-cloud-queues-3-14.laravel-demo.cloud/api/ping` -> 403; `-A "python-requests/2.32.3"` and `-A "python-httpx/0.28.1"` -> 200. A plain `urllib.request.urlopen()` from a laptop got `HTTP Error 403: Forbidden`.
- Environment firewall settings: `bot_categories: []`, `browser_integrity_check: true`.
- Expected: documented behavior; Python services calling each other or health checkers written with urllib are blocked by default.

### N4. WebSocket upgrades never reach Python apps (High, platform)
- Repro (python-3-12, uvicorn ASGI app with a working `/ws/echo` route, verified locally): `curl --http1.1 -H "Connection: Upgrade" -H "Upgrade: websocket" -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: ..." https://python-cloud-queues-python-3-12-e8ecok.laravel-demo.cloud/ws/echo` -> `HTTP/1.1 404 Not Found` from the app's plain HTTP route; k6 reports `websocket: bad handshake`.
- Cause (from the pod): `/etc/nginx/conf.d/default.conf` sets `proxy_set_header Connection "";` and `proxy_http_version 1.1;` with no `Upgrade $http_upgrade` / `Connection $connection_upgrade` mapping, so the upgrade headers are stripped before uvicorn.
- Expected: ASGI apps (FastAPI, Starlette, Django Channels) can serve WebSockets, or the docs say to use Laravel Cloud's WebSocket (Reverb) clusters instead.
- Works: 100 MiB streaming upload (5.3 s, sha256 match); Server-Sent Events with an event every 1 s for 40 s (beyond the 20 s HTTP timeout, first byte ~1.1 s, events not buffered to the end) — the timeout is an idle-read timeout, not a total-request limit.

### N3. HTTP ingress capacity (measured)
- python-3-10, stdlib ThreadingHTTPServer, App 1 replica at start, from a laptop (~95 ms RTT): /api/ping and the static dashboard ramped to 600 req/s with 0 errors, p95 ~110 ms. /api/stats (several Valkey reads per request) failed above ~400-500 req/s: 906 x 504 (the 20 s HTTP timeout), 5810 client-side timeouts/resets, 929 dropped iterations, 1 x 502. The 504 status confirms the "HTTP timeout" setting returns 504 at 20 s.

### N1. nginx proxies Python with a PHP document root; HTTP timeout behavior (Low, platform/docs)
- Evidence: `/etc/nginx/conf.d` `root /var/www/html/public; proxy_read_timeout 20; send_timeout 20;` with upstream `PORT=3000`; `/healthz-*` is answered by nginx without reaching the app.
- The 20 s value is the environment's "HTTP timeout" setting (5-60 s, UI only). It is an idle-read timeout: SSE streams with regular events ran 40 s fine; a silent upstream gets 504 at 20 s (N3). A repository `public/` directory is served directly to Python apps (documented only for Django static files).

## Workers / queues

### Measured behavior (not defects)
- Baselines, 1 Worker replica (1 vCPU, 2 GiB), 4 worker processes, all Python 3.10-3.14 within noise: sync and async 1000x100 ms sleep jobs ~36 jobs/s (ceiling 40); async equals sync because the worker runs one job per process. CPU-time jobs 200x200 ms: 4.9 jobs/s on every version, which is exactly one vCPU; four processes add nothing.
- Database (Laravel MySQL, 200 jobs x 10 primary-key lookups, 1 replica, 4 processes, pooled engines): db_sync 35-70 jobs/s, db_async (10 queries gathered per job, 5-connection async pool reused per worker event loop) 51-70 jobs/s; drain ~4.2 s for both on every version, so queue dispatch/round-trips dominate, not DB I/O. An earlier run showing db_async 3-4x slower was an app artifact (a new async engine and TLS handshakes per job) and was fixed in c5ce572.
- Memory autoscaling (python-3-14, threshold 70%, 1 warm replica): 4 jobs holding 450 MiB each for 420 s pushed the replica's cgroup memory to 1956 MiB (~95% of 2 GiB). A second Worker replica was processing jobs ~33 s later (03:39:53 -> 03:40:26Z) and scaling stopped at 2, because existing allocations cannot move and the new idle replica brings the average to ~50%. All 4 jobs completed; no OOM kill. Memory scale-out protects new work only; it does not relieve the replica under pressure.
- CPU autoscaling (python-3-14, max 6, CPU 60%, applied via redeploy): 3000x200 ms CPU jobs scaled the Worker cluster 1->2 replicas at ~45 s, 4 at ~90 s, 5 at ~120 s, 6 at ~135 s; at 6 replicas throughput reached ~30 jobs/s (6 x 5), i.e. linear. Scale-out lag ~45 s per step.

### W1. Laravel Valkey ACL denies `WATCH`/`UNWATCH`, breaking optimistic locking in redis-py (High, platform)
- Versions: all (observed on python-3-14, release 0a62ffd).
- Repro: on any environment, `cpx cloud command:run <env> --cmd="python -c 'import os,redis;redis.Redis.from_url(os.environ[\"REDIS_URL\"]).execute_command(\"WATCH\",\"k\")'"` -> `redis.exceptions.NoPermissionError: User application has no permissions to run the 'watch' command`.
- Probe results on the pod: allowed MULTI/EXEC, EVAL, SCRIPT LOAD/EVALSHA, SET NX PX, BLPOP, INFO, SCAN, KEYS; denied WATCH, UNWATCH, FUNCTION, CONFIG GET, ACL WHOAMI, CLIENT LIST.
- Expected: either standard transactional commands work or the restriction is documented (valkey.mdx mentions only rate/size limits).
- Impact: `redis-py` `pipeline.watch()`, Celery/RQ/Dramatiq features and libraries using check-and-set fail at runtime with a 503-class error, never at deploy. Our `/api/load` admission returned 503 until rewritten as a Lua script.

### W4. Default cache eviction policy can silently drop queued jobs (Medium, platform/docs)
- The cache created in the UI for python-3-10 reports `maxmemory_policy allkeys-lru` (250 MiB). laravel-cloud-queues stores queued, delayed and reserved jobs in that Valkey; when memory fills, LRU eviction can delete job keys with no error. The CLI cache created with `--eviction-policy=noeviction` (python-3-11) reports `noeviction`.
- Expected: queue-backing caches default to (or the UI warns to use) `noeviction`, or a separate queue cache is recommended; valkey.mdx discusses eviction policies but Python worker docs do not connect it to queues.

### W3. Valkey limits (measured)
- Valkey 9.0.0 (reports redis_version 7.2.4), valkey-flex-250mb: maxmemory 250 MiB, maxclients 10000; 14 connections with 1 App + 1 Worker replica (4 processes). Cache created by CLI with `--eviction-policy=noeviction` reports `noeviction` (python-3-11).

### W2. Laravel MySQL TLS certificate is self-signed (ProxySQL auto-generated), so verified TLS fails (High, platform)
- Versions: all. Evidence: `openssl s_client -starttls mysql` from a pod shows `CN = ProxySQL_Auto_Generated_Server_Certificate` issued by `CN = ProxySQL_Auto_Generated_CA_Certificate`; verification fails with both `/etc/ssl/certs/ca-certificates.crt` and certifi (`Verify return code: 19`).
- Repro: deploy command `python -m db init` using SQLAlchemy+PyMySQL with a default verifying SSL context -> `CERTIFICATE_VERIFY_FAILED: self-signed certificate in certificate chain`; deployment `depl-a2e1bead...` failed.
- Expected: the documented path (`laravel-mysql.mdx` tells Laravel apps to set `MYSQL_ATTR_SSL_CA=/etc/ssl/certs/ca-certificates.crt`) yields a verifiable connection, or Cloud publishes the CA to trust.
- Actual: only encrypted-but-unverified TLS works (we set `DB_SSL_VERIFY=0`). The documented PHP CA setting cannot verify this chain either, so it presumably works only because PDO does not verify by default. The compat probe "DB TLS connect" fails on every role as the standing signal.

## Observability

### O3. Cloud parses JSON log lines; plain-text levels are ignored (Info + Medium, platform)
- Evidence (python-3-14, release bd9762c, logtest marker 197e6647-...): a stdout line that is a JSON object becomes one entry whose `message` is the JSON `msg`/`message` field, whose `level` comes from the JSON `level` field (`debug`/`info`/`warning`/`error` observed), and whose remaining fields are moved into `data` (`role`, `request_id`, `release`, `host`, ...). Text and logfmt lines are always `level: info`, even for ERROR/CRITICAL; a text traceback becomes 8-10 separate entries, while logfmt/JSON keep it in one entry.
- JSON run (logcheck, both roles): levels debug/info/warning/error mapped correctly, tracebacks and exception groups arrive as ONE entry at level error with the trace in data, web and worker both delivered, stdout and stderr both captured, unflushed print delivered (PYTHONUNBUFFERED=1), last line before os._exit(1) delivered. CRITICAL has no platform level (to verify: mapped to error or dropped from filters).
- Good: 4/16/64/256 KiB single lines arrive intact (sha256 match); unicode intact; ANSI escape codes kept raw (not rendered or stripped).
- Problems: (a) undocumented, so Python users do not know JSON is the only way to get levels and fields; (b) the platform does not redact secrets: a fake `mysql://probe:SENTINEL_PW_...@...` line is stored verbatim; (c) stdout and stderr are indistinguishable (no stream field); (d) the `message` field for a JSON record that lacks `msg` is unclear (to verify).
- Best practice for Python on Cloud: log one JSON object per line with `level` and `msg` keys, and put the traceback inside a field.

### O5. Log level comes only from a JSON `level` key with a narrow value set; everything else is `info` (Medium, platform + SDK)
- Repro (python-3-13, release 79c93b6, marker lvl1790912011): `POST /api/logtest/raw` printed 25 lines verbatim from the web process; all 25 were collected. Cloud levels:
  - error: JSON `level` = `error`, `ERROR`, `critical`, `CRITICAL`, `emergency`; JSON with `message` instead of `msg` also works.
  - warning: `WARNING` (and `warning`).
  - info (not recognized): JSON `level` = `warn`, `fatal`, `notice`, `40` (string), `40` (integer).
  - info (key ignored, kept in data): `severity`, `levelname`, `lvl`, `log.level`, `loglevel`, `status`.
  - info: any non-JSON line, including Python's default logging format (`... ERROR root: ...`), stderr lines, `ERROR:`-prefixed stderr, logfmt `level=error`, and the Laravel/Monolog text format `production.ERROR:`.
- Why the dashboard shows INFO for "everything": stderr is not treated as error, Python's stdlib/basicConfig text format is not parsed, and `laravel-cloud-queues` emits text logs plus JSON job events without a `level` key (a failed job event `{"laravel_cloud_queues":"job","status":"failed",...}` is stored as info).
- Expected: documented level contract; recognize common Python values (`warn`, `fatal`, `notice`, numeric levels) and keys (`severity`, `levelname`); treat stderr as at least warning, or document that it is not.
- SDK follow-up: `laravel-cloud-queues` should add `"level"` to its JSON events and offer JSON output for its logger.
- Also: output from `cpx cloud command:run` is not shipped to environment logs (probe marker `lvl-bc7eb6fb` printed via command:run at 03:24:04Z never appeared).

### O4. Empty log windows return a different JSON shape (Low, CLI/API)
- `cpx cloud environment:logs ... --json` returns a top-level array when entries exist, but `{"logs": []}` for an empty window. Scripts that expect an array break (our logcheck.py did).

### O6. Out-of-memory kills are invisible in logs and take down every process on the replica (Medium, platform)
- Repro (python-3-12, Worker cluster 1 replica, 2 GiB, 4 worker processes): `POST /api/load {"kind":"mem","count":4,"mb":600,"ms":120000}` at 04:11:56Z (4 x 600 MiB > 2 GiB). The whole Worker container restarted: the only log line is `[Deploy: 8] Worker Cluster cluster starting...` at 04:11:58, followed by `Worker started` x4. No OOM, exit code 137 or memory message appears. At 04:12:57 the SDK logged 4 `failed_job` events (lease expired, tries=1), each at level info.
- The cgroup memory.events oom_kill counter cannot be reported by the killed process, so the app's oom_events stayed 0.
- Expected: a clear, error-level log/event (e.g. "Worker Cluster replica OOMKilled, limit 2 GiB") and a metric; the restart should not be labelled like a deployment step.
- Impact: one oversized job kills all concurrent jobs on that replica; operators see only unexplained failed jobs.

### O2. `environment:logs` returns at most 100 entries per call, with no pagination (Medium, CLI/API)
- Repro: `cpx cloud environment:logs python-cloud-queues python-3-14 --minutes=60 --json` returns exactly 100 entries (81 access, 19 system) although the window holds far more; there is no cursor/next-page field. A 10-minute `--from/--to` window also returns exactly 100.
- Impact: access logs from dashboard polling crowd out application logs; finding a specific error requires scanning many 5-second windows. The web startup line `Dashboard on :: port 3000` was only found by querying a 5 s window around the deploy.

### O1. Python tracebacks are split into one log entry per line, all at level `info` (Medium, platform)
- Repro: trigger any unhandled exception; `cpx cloud environment:logs python-cloud-queues python-3-14 --minutes=5 --json` returns each traceback line as a separate entry with `"level":"info","type":"system"`, including the final `...Error:` line.
- Expected: multiline tracebacks grouped into one entry at error level (or documented guidance, e.g. JSON logging).
- Impact: errors are not filterable by level; the exception line is separated from its stack.

## Dashboard / UI
- Not tested in depth (overnight run was CLI/API/k6 driven). Settings found only in the UI: HTTP timeout and graceful shutdown timeout (R7). The replicaCount metric reported 7 replicas with max 6 (R8).

## Docs
Docs PR with a Python best-practices guide: https://github.com/laravel/cloud-docs/pull/360 (open, not merged).
- D1. `environments.mdx` says the Python version is not a dashboard setting; `runtimes.mdx` says it can be selected in environment settings (default 3.12).
- D2. No HTTP request timeout is documented; nginx `proxy_read_timeout`/`send_timeout` are 20 s.
- D3. "Graceful shutdown timeout" is referenced in the Python/Go/Node deploy guides but never defined (location, default).
- D4. `WEB_CONCURRENCY` is documented for Django/Flask/FastAPI only; generic Python apps also receive it (3 on 1 vCPU). No warning that `os.cpu_count()` reports the host's 8 CPUs.
- D5. Database/cache pages list only `DB_*`/`REDIS_HOST`; Python environments receive only `DATABASE_URL` (`mysql://`, no TLS params) and `REDIS_URL` (`rediss://`). Verified on python-3-11.
- D6. Postgres pooler guidance only covers `DB_HOST`.
- D7. `compute.mdx`: only Flex sizes can scale to zero, yet UI-created dedicated environments show hibernation on (see R2).
- D8. Scale-to-zero wake for queues/scheduler documented for Laravel only; Python background process behavior while sleeping is undocumented.
- D9. Scheduled tasks are Laravel/Symfony only; no Python scheduler guidance.
- D10. `workers.mdx` Python example omits SIGTERM, restart loops and CPU-sized concurrency defaults.
- D11. `/healthz-*` answered by nginx without reaching the app is undocumented.
- D12. nginx root `/var/www/html/public` serves a repository `public/` directory for Python apps; documented only for Django static files.
- D13. Filesystem docs say 512 MB disk per 1 GB RAM; observed root overlay is 300 GB (enforcement unclear).
- D14. `commands.mdx` is an empty stub; `command:run` runs on the App instance with no worker-cluster selector.
- D15. No Python logging guidance (multiline tracebacks); `logs.mdx` does not state stdout/stderr capture.
- D16. Python build environment (aarch64, Debian 12/glibc 2.36, compiler availability, uv/pip versions) undocumented.
- D17. Runtimes page does not state builds are GIL-enabled (no free-threaded 3.13t/3.14t).
- D18. Managed queues are Laravel/Symfony only; `queues.mdx` does not point Python users to worker clusters with their own broker.

## App-side issues found during review (not platform)
- A1. The legacy Run check `timeout` case passes on two `started` events without proving exit 124 or a restart; `flaky` and `quick` accept duplicate completions (telemetry.py, shared with fastapi-cloud-queues).
- A2. The `workers` case requires exact Python patch equality between web and workers, which fails during rolling deploys.
