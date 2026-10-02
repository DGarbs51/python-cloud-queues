# Testing Laravel Cloud's Python runtime

This repository is a QA vehicle for the Python runtime on Laravel Cloud. The app is plain
Python (a standard-library HTTP server and a `laravel-cloud-queues` worker on Valkey), so
what breaks is the platform, not a framework. Each test below drives the app with
[k6](https://k6.io) or the dashboard, captures platform metrics around the run, and states
what a pass and a platform defect look like. Defects go into `FINDINGS.md`.

The suite runs against five environments of one application, one per Python version:

| Environment | Branch | URL |
|---|---|---|
| `python-3-10` | `python-3.10` | https://python-cloud-queues-3-10.laravel-demo.cloud |
| `python-3-11` | `python-3.11` | https://python-cloud-queues-python-3-11-iutzvn.laravel-demo.cloud |
| `python-3-12` | `python-3.12` | https://python-cloud-queues-python-3-12-e8ecok.laravel-demo.cloud |
| `python-3-13` | `python-3.13` | https://python-cloud-queues-python-3-13-ehqncb.laravel-demo.cloud |
| `python-3-14` | `python-3.14` | https://python-cloud-queues-3-14.laravel-demo.cloud |

Each environment has an App cluster and a Worker cluster with custom autoscaling 1 to 6
replicas at 60% CPU or 70% memory, the worker running `laravel-cloud-queues work app:registry`
as 4 processes, a 250 MB Valkey cache and a schema on the shared MySQL cluster (Cloud injects
`DATABASE_URL` and `REDIS_URL`). Instance sizes differ: 3.10 and 3.14 predate the others and
run the legacy `dedicated.c-1vcpu-2gb` size with hibernation on; 3.11 to 3.13 run
`pro.g-1vcpu-2gb` (the API rejects the dedicated size for new instances) with scale-to-zero
off. Both sizes are 1 vCPU and 2 GiB; compare `/api/env` cgroup limits across them before
reading version differences into results. nginx sits in front of the app with a 20 s
`proxy_read_timeout`.

## Running k6

Install k6 (`brew install k6`) and run scripts from the repository root; each writes its
summary to `results/<env>-<script>.json` (create `results/` first, or use the fleet runner,
which does):

```sh
mkdir -p results
BASE_URL=https://python-cloud-queues-3-10.laravel-demo.cloud k6 run k6/queue.js
k6/run-fleet.sh queue -e KIND=async -e COUNT=1000   # every environment, one at a time
VERSIONS="12 14" k6/run-fleet.sh compat              # a subset
```

Script settings are environment variables (`-e NAME=value` or exported). `ENV_NAME` overrides
the name used in result files; it defaults to the version in `BASE_URL` (`3-12`), else `local`.

| Script | Test | Settings (defaults) |
|---|---|---|
| `k6/http.js` | ingress knee per endpoint (`/api/ping`, `/`, `/api/stats`), run one after another, 40 s apart | `RATE` (50 req/s), `DURATION` (120 s per endpoint), `EXECUTOR` (`ramping` from 1 to `RATE`, or `constant`), `ENDPOINTS` (`ping,static,stats`), `P95_MS` (1000), `MAX_VUS` |
| `k6/queue.js` | one load run from `POST /api/load` to a terminal state | `KIND` (`sync`), `COUNT` (100), `MS`, `MB`, `ROWS`, `KEY`, `DRAIN_S` (900) |
| `k6/e2e.js` | dashboard readers during a load run, then one Run check | `READ_RATE` (10), `KIND`, `COUNT` (500), `MS` (50), `DRAIN_S` (300), `DURATION` (readers; defaults to the driver's worst case, `DRAIN_S` + 720 s) |
| `k6/coldstart.js` | first request and first job after hibernation | `WAKE_S` (300), `DRAIN_S` |
| `k6/memory.js` | memory autoscaling and OOM | `SCENARIO` (`worker_scale`, `app_scale`, `oom`), `COUNT`, `MB`, `MS`, `HOLD_S`, `DEADLINE_S` (1800), `EXPECT_SCALE` |
| `k6/compat.js` | compatibility probes on web and worker | `MIN_PROBES` (1), `ALLOWED_SKIPS`, `WORKER_S` (180) |
| `k6/stream.js` | SSE event count, 100 MB upload and websocket (gunicorn/uvicorn modes) | `TESTS` (`sse,upload,ws`), `STREAM_S` (30), `INTERVAL` (1), `UPLOAD_MB` (100), `WS_IDLE_S` (25) |
| `k6/sse.py` | SSE per-event arrival times and buffering (k6 cannot see them): `python3 k6/sse.py` | `BASE_URL`, `STREAM_S` (30), `INTERVAL` (1) |

Every script except `http.js` fails (non-zero exit) when any of its checks fail, when a
request it depends on is rejected (for example a 409 because another run is active) and when
it does not reach its end (a script error or a timeout). `http.js`
fails on its latency and error-rate thresholds, which is expected past the knee; read the
per-endpoint `dropped_iterations` and `status_429`/`status_502`/`status_504` counts.

Only one load run and one memory hold can be active per environment (the app answers 409);
do not run two load scripts against the same environment at once. A load run that misses
its deadline is cancelled by the script. Stopping k6 with Ctrl-C does not stop accepted
work: cancel it with `POST /api/load/<run>/cancel` or on the dashboard.

## Capturing platform evidence

For every test, capture the platform's view before, during and after the run, and keep the
UTC start and end times. Run `cpx cloud <command> --help` for the exact arguments; the
environment and instance ids come from `cpx cloud environment:list` and
`cpx cloud instance:list`.

```sh
# before: replica counts and sizes, and the deployed release
cpx cloud instance:list <environment>
curl -s $BASE_URL/api/env          # python, release SHA, cgroup limits, server mode

# during (every few minutes on long runs) and after
cpx cloud instance:list <environment>             # replicas: did the cluster scale?
cpx cloud environment:metrics <environment>       # CPU, memory, HTTP in 2-minute points
cpx cloud cache:metrics <cache>                   # Valkey memory, connections, commands
cpx cloud database-cluster:metrics <cluster>      # MySQL connections, CPU, latency
cpx cloud environment:logs <environment>          # restarts, OOM kills, tracebacks, SIGTERM
```

Save the outputs next to the k6 summary (for example `results/3-12-queue-metrics-after.txt`).
The standard set is `instance:list`, `environment:metrics` and `environment:logs`; the tests
below name any extra captures.

## Measurement protocol

- **Baselines run at one replica.** Throughput, GIL and version-matrix runs pin both
  clusters to one replica so results compare across versions, then restore autoscaling:

  ```sh
  cpx cloud instance:update <app-instance> --max-replicas=1
  cpx cloud instance:update <worker-instance> --max-replicas=1
  # ... run the baseline ...
  cpx cloud instance:update <app-instance> --max-replicas=6
  cpx cloud instance:update <worker-instance> --max-replicas=6
  ```

  Check with `instance:list` that the clusters are back at one replica before a baseline
  starts; scale-in after an earlier test takes a few minutes.
- **Push freeze.** Do not push to `main` or any `python-3.x` branch during a measurement
  window: push-to-deploy would redeploy the environment mid-run. Record the release SHA from
  `/api/env` before and after; a change invalidates the run.
- **One environment at a time** for anything that touches MySQL: the five schemas share one
  cluster, so parallel runs measure each other.
- **Close the dashboard** during measurements: it polls `/api/stats` every second, which is
  load, and it keeps a hibernating environment awake.
- **Never press Reset during a run**; load runs keep their own records (`lcq-load:*`, 6 h TTL)
  and the Run check (`lcq-demo:*`) is only run serially, by `e2e.js` or by hand.

## Test matrix

Each test lists the steps, the extra evidence to capture, the expected result, the failure
signal and the `FINDINGS.md` category a failure belongs to.

### 1. HTTP ingress knee, 502/504 at 20 s

- **Steps:** at one replica, `k6 run -e RATE=100 -e DURATION=180 k6/http.js` per environment.
  Repeat with `EXECUTOR=constant` at the rate just below the knee for a stable number.
- **Expected:** latency flat until the single-process server saturates, then rising; errors
  only past the knee, as 502/504 from nginx. `/api/ping` and `/` knee higher than
  `/api/stats` (which reads Valkey).
- **Failure signal:** errors well below the knee, 429s from the edge without a documented
  rate limit, 504s before 20 s, connection resets, or latency that never recovers after the
  test (stuck worker threads).
- **Category:** Networking/ingress.

### 2. Queue throughput vs processes and replicas

- **Steps:** at one replica, `k6/run-fleet.sh queue -e KIND=sync -e COUNT=2000 -e MS=100`.
  Then restore autoscaling and repeat with `COUNT=10000`.
- **Extra capture:** `cache:metrics`.
- **Expected:** at one replica about 4 processes x 10 jobs/s = ~40 jobs/s, minus dispatch
  and Valkey overhead; `workers` shows 4 `host:pid` entries per worker replica; with
  autoscaling, throughput rises as replicas join. `processed == count`, `failed == 0`.
- **Failure signal:** fewer than 4 workers per replica, lost jobs (`processed + failed <
  count`), `duplicates > 0` without a worker restart, or replicas that join without taking
  jobs.
- **Category:** Workers/queues (Resources if scaling misbehaves).

### 3. Async vs sync jobs (sleep and real database IO)

- **Steps:** at one replica, on each environment, compare `KIND=sync` with `KIND=async`
  (`COUNT=1000 MS=100`), then `KIND=db_sync` with `KIND=db_async` (`COUNT=500 ROWS=50`).
- **Extra capture:** `database-cluster:metrics` for the database kinds.
- **Expected:** `sync` and `async` sleep jobs have equal throughput, because the worker runs
  one job at a time per process; `db_async` beats `db_sync` per job because its queries run
  concurrently, with equal query counts.
- **Failure signal:** `aiomysql` errors on a Python version (driver or TLS incompatibility),
  event-loop errors in async jobs, or async jobs much slower than sync ones.
- **Category:** Workers/queues; Pods/runtime for version-specific errors.

### 4. CPU-bound work and the GIL on 1 vCPU

- **Steps:** at one replica, `k6/run-fleet.sh queue -e KIND=cpu -e COUNT=200 -e MS=200`.
- **Expected:** about 5 jobs/s per worker replica however many processes run (1 vCPU,
  cgroup `cpu.max` 100000/100000); `run_ms` p95 grows to about 4 x 200 ms as the 4 processes
  share the CPU. `/api/env` reports `gil_enabled: true` and `process_cpu_count` 1.
- **Failure signal:** throughput above one CPU (cgroup limit not enforced), far below it
  (CPU throttling or noisy neighbours: check `environment:metrics`), or `os.cpu_count()`-sized
  defaults (8 host CPUs) leaking into the runtime.
- **Category:** Resources; Pods/runtime.

### 5. Autoscaling on CPU and memory, App and Worker

- **Steps:** with autoscaling 1 to 6 on both clusters:
  - Worker CPU: `k6 run -e KIND=cpu -e COUNT=3000 -e MS=500 k6/queue.js`.
  - App CPU: `k6 run -e EXECUTOR=constant -e RATE=<knee> -e DURATION=600 -e ENDPOINTS=stats k6/http.js`.
  - Worker memory: `k6 run -e SCENARIO=worker_scale k6/memory.js`.
  - App memory: `k6 run -e SCENARIO=app_scale k6/memory.js`.
- **Extra capture:** `instance:list` every minute (actual vs maximum replicas).
- **Expected:** only the loaded cluster scales; replicas join within a few minutes of
  crossing 60% CPU or 70% memory and leave after the load stops. The scripts log replicas
  seen over time and check that the cluster scaled out.
- **Failure signal:** the wrong cluster scales, no scale-out after several minutes over the
  threshold, oscillation, the dashboard disagreeing with `instance:list`, or scale-in killing
  busy workers (jobs lost or redelivered).
- **Category:** Resources; Dashboard/UI for display disagreements.

### 6. Hibernation cold start

- **Steps:** on `python-3-12`, which has scale-to-zero off as created:
  1. Record its current hibernation, scale-to-zero and minimum-replica settings
     (`instance:list` and the dashboard).
  2. Turn hibernation (scale-to-zero) on for its App and Worker clusters.
  3. Close every dashboard tab and wait until both clusters show zero running replicas.
     Without that confirmation the run measures a warm environment: stop and investigate.
  4. `k6 run k6/coldstart.js`.
  5. Restore the settings recorded in step 1.

  Record the settings and the time asleep alongside the result.
- **Expected:** the first `/api/ping` (a Python route; nginx answers `/healthz-*` without
  waking the app) answers 200 after the wake, with no 502/504; the first job is processed
  once the worker cluster is awake.
- **Failure signal:** 502/504 during the wake (`first request answered 200` fails), a wake
  longer than the 20 s proxy timeout, or a queued job that waits until someone opens the
  dashboard (the worker does not wake on queue).
- **Category:** Pods/runtime; Workers/queues for the worker wake.

### 7. Resilience: timeout, OOM and redeploy during a drain

- **Steps:**
  - OOM, alone and at one replica: `k6 run -e SCENARIO=oom k6/memory.js`.
  - Redeploy mid-drain: start `k6 run -e KIND=sync -e COUNT=3000 -e MS=500 k6/queue.js`,
    and once `processed` passes a few hundred, redeploy the environment from the dashboard
    (this is the one deliberate push-freeze exception).
  - Timeout: the Run check's `timeout` case (`e2e.js`, test 10).
- **Extra capture:** `environment:logs` around the kill and the redeploy (SIGTERM, exit
  codes, OOM kill messages).
- **Expected:** the OOM kill shows in `oom_events` and the logs, the worker restarts with a
  new pid, and every job ends processed or failed (`processed + failed == count`). On
  redeploy, workers get SIGTERM and finish or release their job; nothing accepted is lost;
  duplicates are possible (at-least-once) and reported.
- **Failure signal:** jobs that never reach a terminal state, workers killed without SIGTERM
  or with no grace period, no OOM evidence anywhere on the platform, or a worker that does
  not come back.
- **Category:** Workers/queues; Pods/runtime; Observability for missing evidence.

### 8. Valkey limits and connections

- **Steps:** at six worker replicas (24 processes), run `queue.js` with `COUNT=10000 MS=1`
  then `MS=1000`, so the queue holds thousands of jobs.
- **Extra capture:** `cache:metrics` before, during and after; the eviction policy and
  `maxclients` from the cache's settings.
- **Expected:** memory stays under 250 MB, connections stay under `maxclients`, and no keys
  are evicted (a queue must not use an evicting policy).
- **Failure signal:** `OOM command not allowed`, `max number of clients reached`, evicted
  queue keys (jobs vanish), or metrics that disagree with what `INFO` would report.
- **Category:** Resources.

### 9. MySQL connections and latency

- **Steps:** one environment at a time, at six worker replicas: `KIND=db_write COUNT=2000
  ROWS=100`, then `db_read`, then `db_async ROWS=200`.
- **Extra capture:** `database-cluster:metrics`; the cluster's `max_connections`.
- **Expected:** at most 6 replicas x 4 processes x 2 engines x 1 connection = 48 worker
  connections plus the App replicas, well under `max_connections`; no errors; latency
  stable.
- **Failure signal:** `Too many connections`, TLS handshake failures, connections dropped by
  a proxy (pool pre-ping reconnect storms), or schema access across environments.
- **Category:** Resources.

### 10. Version matrix 3.10 to 3.14

- **Steps:** at one replica, after the canary environment (`python-3-14`) is verified, run
  `k6/run-fleet.sh` for `queue` (sync, async, cpu), `http` and `e2e`.
- **Expected:** every environment reports its own Python in `/api/env` and in
  `python_versions`, the Run check passes everywhere, and throughput differences are small
  and explainable by interpreter speed.
- **Failure signal:** an environment running a different Python than its branch's
  `.python-version`, web and worker on different versions (the check's `workers` case), or a
  version that fails a case the others pass.
- **Category:** Build & deploy; Pods/runtime.

### 11. Compatibility probes, web vs worker

- **Steps:** `k6/run-fleet.sh compat -e MIN_PROBES=<count>`; see
  [Compatibility probes](#compatibility-probes).
- **Expected:** no probe fails; skips only where the probe needs a newer Python; web and
  worker agree.
- **Failure signal:** any `fail`, an unexpected skip, or `web_worker_mismatches > 0`.
- **Category:** Pods/runtime; Build & deploy.

### 12. Platform checks

Manual, once per environment unless noted. Record what the platform does, not what it should.

- `.python-version` is honoured (compare `/api/env` with the branch), including a patch pin
  and a conflict with `requires-python`.
- The scheduler runs Python commands, with the right runtime and working directory.
- The deploy command (`python -m db init`) runs once per deployment, and a failing deploy
  command fails the deployment.
- stdout and stderr from web and worker both reach `environment:logs`, with multiline
  tracebacks kept together.
- `command:run` runs on the App cluster; check whether it can target the Worker cluster.
- Redeploy sends SIGTERM to web and worker processes (look for the `sigterm` log lines).
- Python environments show PHP-only fields (PHP version, Node version, artisan/composer
  defaults) in the dashboard or `application:get`: log these as Low unless they change
  behaviour.

### Additions from plan review (REV 9)

- **Server modes.** On one environment, keep the start command `python app.py` and set the
  `SERVER` environment variable to `gunicorn`, then `uvicorn`; rerun `http.js`, `stream.js`
  and `/api/env` (which reports `server`) under each. Expected: `WEB_CONCURRENCY` worker
  processes, not zero or double; SSE events arrive spread over the stream (not buffered) and
  survive past 20 s or are cut cleanly; a 100 MB upload is hashed correctly; the websocket
  upgrades and survives 25 s idle (uvicorn only); SIGTERM drains in-flight requests. k6 sees
  only the whole SSE response, so run `BASE_URL=... python3 k6/sse.py` as well: it prints each
  event's arrival time and fails when the first event is late or a gap exceeds three intervals
  (proxy buffering), or when the stream is cut.

  Category: Networking/ingress.
- **Health vs readiness.** Deploy a release that fails at import, binds the wrong port or
  exits at once. Expected: the deployment fails and the previous release keeps serving.
  Failure: nginx `/healthz-*` reports healthy while Python is down. Category: Build & deploy.
- **Worker minimum 0.** With the Worker cluster able to scale to zero, dispatch with no web
  traffic beyond the dispatch itself; the worker should wake on queue. Category:
  Workers/queues.
- **Background-process crash loop.** Point the worker command at a module that raises at
  import; expect bounded restarts with backoff and visible errors, not a hot loop.
  Category: Workers/queues; Observability.
- **Rollback and stale dependencies.** Deploy a release that adds a dependency, then roll
  back; the package must not stay importable from `/var/www/.local`. Category: Build & deploy.
- **Lockfile from a newer uv.** Commit a `uv.lock` written by a newer uv than the image's;
  expect a clear build error or a successful install, not a silent fallback. Category:
  Build & deploy.
- **SIGTERM mid-job.** Covered by test 7's redeploy; also stop the background process from
  the dashboard during a long job. Category: Workers/queues.
- **Worker logs and command target.** Worker output appears in `environment:logs` with its
  replica; `command:run` targets are documented. Category: Observability; CLI.
- **Environment variable edge values.** Set dummy variables with empty, Unicode, multiline,
  `$`-containing and URL-reserved values; read them back from web, worker, scheduler and
  deploy command. Category: API; Build & deploy.
- **Managed queue backend.** Switch `LARAVEL_CLOUD_QUEUES_BACKEND` to the managed backend on
  one environment and run the Run check. Category: Workers/queues.

## Compatibility probes

`compat.py` checks what the Python runtime on each Laravel Cloud role can actually do. `run_probes()` returns one record per probe:

```json
{"name": "zoneinfo", "group": "build", "min_python": "3.10", "status": "pass", "detail": "DST transition correct via system tzdata; ..."}
```

| Status | Meaning |
| --- | --- |
| `pass` | Capability present and working. |
| `fail` | Capability missing or broken where it should work. Investigate; most are platform findings. |
| `skip` | Not applicable: the interpreter is older than `min_python`, or the input (DB vars, certifi, `/dev/shm`) is absent. |
| `info` | Build- or environment-dependent fact (JIT, free-threading, cgroup limits, UID). Record it; it is not a pass/fail verdict. |

Groups:

- **version**: 3.11 `tomllib`, `ExceptionGroup`, `asyncio.TaskGroup`, `asyncio.timeout`; 3.12 `sys.monitoring`; 3.13 removed PEP 594 modules absent, `os.process_cpu_count`, JIT and free-threaded build (info); 3.14 `concurrent.interpreters` + `InterpreterPoolExecutor`, `annotationlib`, the `multiprocessing` default start method (`forkserver` on Linux).
- **syntax**: `except*`, PEP 695 type parameters, PEP 701 f-strings, PEP 750 t-strings. Compiled from source strings, so `compat.py` itself imports on 3.10.
- **build**: `ssl` + OpenSSL version and CA paths, `sqlite3`, `zlib`/`lzma`/`bz2`/`compression.zstd`, `ctypes`, `_decimal`, `readline`, `_uuid`, `dbm`, `hashlib` algorithms, `zoneinfo` with a DST transition check (fails if the build has no tzdata).
- **container**: locale and default `open()` encoding, CPU count vs cgroup `cpu.max`, `memory.max`/`memory.current`/OOM kills, `pids.max`, `/dev/shm` size and write, `multiprocessing` fork/spawn/forkserver `Pool`, subprocess, threads, writable `/tmp`/cwd/`HOME`, user site dir on `sys.path` (Cloud installs deps to `PYTHONUSERBASE`), UID/GID, IPv4 + IPv6 DNS for `pypi.org` and the Redis/DB hosts, outbound HTTPS with the default SSL context and with certifi, SIGTERM handler, stdout/stderr buffering, C compiler, `uv`, `LARAVEL_CLOUD_*` release vars (names only), DB TLS connect via PyMySQL.
- **deps**: import status and version of SQLAlchemy, PyMySQL, aiomysql, redis, laravel-cloud-queues, certifi. Informational; none are required by `compat.py`.

Probes that can block or crash (DNS, HTTPS, multiprocessing, signals, subinterpreters, DB) run in a child interpreter in its own session and are killed with their process group after 10 s. All probes run concurrently; `run_probes()` returns within 25 s, never raises and never signals the calling process.

### Run locally

```bash
uv run --python 3.10 python test_compat.py
uv run --python 3.14 python test_compat.py
uv run python -c "import compat, json; print(json.dumps(compat.run_probes(), indent=1))"
```

Local notes: `DB TLS connect` fails against Herd MySQL with TLS because of its self-signed certificate; set `DB_SSL=0` to get an `info` result. uv's standalone CPython 3.10 build has an empty `zoneinfo.TZPATH`, so `zoneinfo` fails there unless the `tzdata` package is installed.

### Run on Laravel Cloud

1. Open the dashboard, Runtime compatibility panel, or `GET /api/compat` for the web role.
2. `POST /api/compat/worker` (JSON body `{}`) dispatches `demo.compat`; the worker result appears in `GET /api/compat` under `worker` with `worker_at`.
3. Compare web vs worker on each environment (3.10 to 3.14). Expected: zero `fail`; `skip` only for `min_python` above the environment's version or missing inputs; identical `deps` and `build` details on both roles.
4. For a manual run on the App instance: `cpx cloud command:run` with `python -c "import compat, json; print(json.dumps(compat.run_probes(), indent=1))"`.

Record every `fail`, and every web/worker difference, in FINDINGS.md with environment, release SHA and the probe `detail`.

## Recording findings

Each `FINDINGS.md` entry has a category (Resources, CLI, API, Pods/runtime, Build & deploy,
Networking/ingress, Workers/queues, Observability, Dashboard/UI, Docs), a severity, the
affected versions, a classification (platform, SDK, app or docs), the contract it breaks
(documentation, CLI help or this repository's contract), the release SHA, the UTC window,
repro steps, expected vs actual, the evidence (k6 summary, `cpx cloud` captures, logs) and a
control result (the same test passing elsewhere, or locally). Known app-side issues, such as
check verdicts that overclaim on the `timeout` and `flaky` cases, are logged as app
findings, not platform ones.

## Teardown

After the measurement windows:

1. Cancel any active load run (`POST /api/load/<run>/cancel` or the dashboard) and wait for
   the queues to drain (`/api/stats` depth 0).
2. Scale both clusters of every environment back to one replica and restore each
   environment's hibernation setting as recorded before the tests (on for 3.10 and 3.14 as
   created, off for 3.11 to 3.13) with `cpx cloud instance:update` (see `--help` for the flags).
3. Delete the load rows from each schema (`DELETE FROM load_rows`) and resize the MySQL
   cluster down.
4. Leave the `lcq-load:*` keys to their 6 h TTL; press Reset on the dashboard only when no
   check is running.
5. Lift the push freeze.
