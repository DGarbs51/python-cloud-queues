# python-cloud-queues

A plain Python app (no web framework) that tells you whether Python works on a
[Laravel Cloud](https://cloud.laravel.com) deployment. Deploy it, open it, press **Run all
checks**, and read the result: every row is pass, warning, fail or skipped, with a plain
explanation of anything that isn't a pass.

It checks the things a real Python app depends on:

| Group | What it checks |
|---|---|
| Web | server and process count vs `WEB_CONCURRENCY`, `PORT`, the proxy headers (`X-Forwarded-Proto`, `X-Forwarded-For`, `Cloud-Request-ID`), and that the async event loop isn't blocked (uvicorn only) |
| Logging | [laravel-cloud-logging](https://pypi.org/project/laravel-cloud-logging/) is installed and Cloud's log socket is reachable |
| Runtime | Python version vs `.python-version`, outbound HTTPS, `/tmp`, CPU and memory limits, subprocesses, threads |
| Services | Valkey/Redis `PING`, database `SELECT 1` (MySQL or Postgres, from `DATABASE_URL`), DNS for both |
| Queue | seven jobs through [laravel-cloud-queues](https://pypi.org/project/laravel-cloud-queues/): plain, async, delayed, retried, failing, timed out and a burst of 20 |
| Throughput | N async jobs (default 1,000) through the queue: jobs per second, p95 wait, lost and duplicated jobs |

Checks that only make sense on Cloud (proxy headers, log socket, cgroup limits) are skipped
when you run locally.

## Deploy to Laravel Cloud

One application, one environment per Python version. Each environment tracks a branch that
differs from `main` only in `.python-version`, which Cloud reads to pick the runtime. 3.14 is
the default.

| Branch | Python |
|---|---|
| `python-3.10` … `python-3.14` | 3.10 … 3.14 |

Pushing a branch deploys its environment. To ship a change to every version:

```sh
for v in 3.10 3.11 3.12 3.13 3.14; do
  git switch python-$v && git merge --no-edit main && git push
done
git switch main
```

Every environment uses the same settings, so results compare across versions:

- **Start command (ASGI, default):** `uvicorn asgi:app --host :: --port $PORT`
- **Start command (WSGI):** `gunicorn wsgi:app --bind [::]:$PORT` (reads `gunicorn.conf.py`)
- **App and Worker cluster:** `pro.g-2vcpu-4gb`, autoscaling 1–6 replicas (CPU 60%, memory 70%), scale-to-zero off.
- **Worker cluster:** 4 processes of `laravel-cloud-queues work app:registry`. Don't add
  `--stop-when-empty`: Cloud restarts any worker that exits, which the timeout case relies on.
- **Valkey cache** attached (Cloud injects `REDIS_URL`) and `LARAVEL_CLOUD_QUEUES_BACKEND=redis`.
- **Database** (optional) attached; Cloud injects `DATABASE_URL`. Set `DB_SSL_VERIFY=0` for
  Cloud's self-signed database proxy.
- Build and deploy commands empty.

Use `--host ::`, not `--host ''`: with several workers uvicorn binds an IPv4-only socket for
`''`, which Cloud's IPv6 network can't reach.

To test WSGI, change only the start command and rerun the checks. The page header shows which
server answered; the async row is skipped under gunicorn.

The app has no login. Protect it at the network level.

## Run locally

You need [uv](https://docs.astral.sh/uv/), Valkey/Redis (Laravel Herd bundles one on
`127.0.0.1:6379`) and optionally MySQL.

```sh
cp .env.example .env
uv run --env-file .env uvicorn asgi:app --host :: --port 8000   # http://localhost:8000
uv run --env-file .env laravel-cloud-queues work app:registry   # second terminal; more for more workers
```

Logs are JSON lines, as on Cloud. To read them in a terminal, pipe through the jq filter:

```sh
uv run --env-file .env uvicorn asgi:app --host :: --port 8000 2>&1 | jq -Rr --unbuffered -f scripts/pretty.jq
```

Locally nothing restarts a worker that exits, so the queue's timeout case only passes with a
restart loop: `while true; do uv run --env-file .env laravel-cloud-queues work app:registry; done`.

## HTTP load

Throughput through the queue is on the page. For requests per second through Cloud's ingress,
run [k6](https://k6.io) from your machine:

```sh
k6 run -e BASE_URL=https://your-app.laravel.cloud k6/http.js
```

It ramps to 50 virtual users and fails if more than 1% of requests fail or p95 latency is over
500 ms.

## Tests

```sh
uv run python app.py --self-check              # router and logging
uv run --env-file .env python test_checks.py   # check contract
uv run --env-file .env python test_throughput.py
uv run python test_servers.py                  # boots uvicorn and gunicorn with the Cloud start commands
```

## Files

- `app.py`: queue jobs, the queue check and the `handle()` router both servers share.
- `asgi.py`, `wsgi.py`, `gunicorn.conf.py`: the uvicorn and gunicorn entrypoints.
- `checks.py`: the Web, Logging, Runtime and Services checks behind `GET /api/checks`.
- `throughput.py`: the throughput test behind `/api/throughput`.
- `telemetry.py`: job telemetry and the queue check verdicts.
- `logs.py`: logging setup through laravel-cloud-logging.
- `index.html`: the page (Tailwind from a CDN, no build step).
- `k6/http.js`: HTTP load script. `scripts/pretty.jq`: readable local logs.
