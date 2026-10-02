# python-cloud-queues

A plain-Python (no web framework) demo of [`laravel-cloud-queues`](https://pypi.org/project/laravel-cloud-queues/),
installed from PyPI. It proves the queue driver works on a Laravel Cloud deployment: a
dashboard shows jobs as they are queued and processed, and **Run check** dispatches a fixed
set of jobs and gives a pass/fail verdict for each, so the same check can be rerun after
changing Python version, cluster size, instance type or cache.

The FastAPI version with the same dashboard and check is
[`fastapi-cloud-queues`](https://github.com/DGarbs51/fastapi-cloud-queues).

## Branches: one per Python version

| Branch | Python |
|---|---|
| `python-3.10` | 3.10 |
| `python-3.11` | 3.11 |
| `python-3.12` | 3.12 |
| `python-3.13` | 3.13 |
| `python-3.14` | 3.14 |

The branches differ from `main` only in `.python-version`, which Laravel Cloud reads to
pick the runtime. Deploy each branch as its own application, with its own Valkey cache.
Make changes on `main` and merge them into every branch:

```sh
for v in 3.10 3.11 3.12 3.13 3.14; do
  git switch python-$v && git merge --no-edit main && git push
done
git switch main
```

## Deploy to Laravel Cloud

1. Create an application from this repository and pick a `python-3.x` branch.
2. Attach a Laravel Valkey cache. Cloud injects `REDIS_URL`.
3. Set `LARAVEL_CLOUD_QUEUES_BACKEND=redis`.
4. Leave the build command empty; Cloud installs dependencies with uv from `uv.lock`.
5. Set the start command to `python app.py`. It listens on `$PORT` over IPv6 and IPv4 whenever `PORT` is set (Cloud's cluster network is IPv6). Cloud auto-detects FastAPI apps; this plain-Python one has not been deployed yet, so check that Cloud accepts the start command.
6. Create a worker cluster that runs the queue worker as **4 processes**, each with:

   ```sh
   laravel-cloud-queues work app:registry
   ```

   The App cluster serves only the dashboard; it runs no workers. Keep the worker
   cluster's instance count fixed so check runs are comparable across hardware changes.
   Do not add `--stop-when-empty`: Cloud restarts any worker that exits.

   With 4 processes the timeout case, which exits the worker that runs it, never stalls the
   other cases. Each process shows on the dashboard as `hostname:pid`; a restarted worker
   appears with a new pid. `WORKER_LABEL` overrides the hostname if you add more clusters.

Protect the app at the network level; the dashboard and dispatch endpoints have no login.

## Run check

The check dispatches one job per case and polls what the workers recorded:

| Case | Passes when |
|---|---|
| `quick` | a sync job is processed on attempt 1 |
| `async` | an async job is processed on attempt 1 |
| `delayed` | a job dispatched with a 5 s delay starts at least 5 s later |
| `flaky` | a job that fails once is released, then processed on attempt 2 |
| `failing` | a job that always fails is released, then failed on attempt 2 |
| `timeout` | a job over its 3 s timeout exits the worker (exit 124), the platform restarts it, and the job is redelivered after the 60 s Redis lease |
| `burst` | 20 jobs are each processed exactly once |
| `workers` | at least one worker ran, and every worker runs the same Python version as the web process |

A full run takes one to two minutes because of the timeout case. A case still waiting after
240 s fails.

## What the dashboard shows

- **Queue depth**: ready, delayed (including retry backoff) and reserved (in-flight) jobs,
  read from the package's Redis keys. Shown in `redis` mode only.
- **Totals and recent events**: `queued`, `started`, `processed`, `released` and `failed`.
  Outside managed mode the package emits no lifecycle events, so each job records its own
  telemetry into Valkey (`lcq-demo:*`) using `current_job()`. The store falls back to
  `REDIS_URL` in other modes, so the dashboard keeps working after a switch to managed queues.
- **Workers**: worker label, process and Python version for the last minute.

## Load and compatibility tests

The repository is also a QA vehicle for Laravel Cloud's Python runtime. On top of the
dashboard it serves:

| Endpoint | Purpose |
|---|---|
| `GET /api/ping` | cheap route for ingress tests (no Valkey or database) |
| `GET /api/env` | Python build, cgroup limits, memory, server mode, release |
| `POST /api/load`, `GET /api/load`, `GET /api/load/<run>`, `POST /api/load/<run>/cancel` | queue load runs: sync, async, CPU, memory and database jobs, with throughput and wait percentiles |
| `POST /api/hold` | hold memory in the web process, for App memory autoscaling |
| `GET /api/compat`, `POST /api/compat/worker` | runtime compatibility probes on the web process and a worker |

[TESTING.md](TESTING.md) has the test matrix, the [k6](https://k6.io) scripts in `k6/` and
how to capture platform metrics. [PORTING.md](PORTING.md) is a guide to porting the app and
tests to FastAPI, Django and Flask.

## Run locally

You need Redis or Valkey (Laravel Herd bundles Valkey on `127.0.0.1:6379`) and
[uv](https://docs.astral.sh/uv/).

```sh
uv sync
export LARAVEL_CLOUD_QUEUES_BACKEND=redis
export LARAVEL_CLOUD_QUEUES_REDIS_URL=redis://127.0.0.1:6379/0

uv run python app.py                           # dashboard on http://127.0.0.1:8000
uv run laravel-cloud-queues work app:registry  # in a second terminal; run more to scale
```

Locally nothing restarts a worker that exits, so the `timeout` case only passes with a
restart loop: `while true; do uv run laravel-cloud-queues work app:registry; done`.

## Files

- `app.py`: the `Registry`, the jobs and the standard-library web server.
- `telemetry.py`: job telemetry, dashboard data and the check. Identical in both demo
  repositories; keep them in sync.
- `index.html`: the dashboard (Tailwind CSS from the CDN, no build step).
- `k6/`: load and compatibility test scripts; see [TESTING.md](TESTING.md).
