# python-cloud-queues

A plain Python app (no web framework) that tells you whether Python works on a
[Laravel Cloud](https://cloud.laravel.com) deployment. Deploy it, open it, press **Run all
checks**, and read the result: every row is pass, warning, fail or skipped, with a plain
explanation of anything that isn't a pass.

It checks the things a real Python app depends on:

| Group | What it checks |
|---|---|
| Web | server and process count vs `WEB_CONCURRENCY`, `PORT`, the proxy headers (`X-Forwarded-Proto`, `X-Forwarded-For`, `Cloud-Request-ID`), the async event loop isn't blocked, a WebSocket upgrade through Cloud's proxy (ASGI servers only), and that the pod's nginx reaches the app over IPv6 |
| Logging | [laravel-cloud-logging](https://pypi.org/project/laravel-cloud-logging/) is installed and Cloud's log socket is reachable |
| Runtime | Python version vs `.python-version`, outbound HTTPS, `/tmp`, CPU and memory limits, subprocesses, threads |
| Services | Valkey/Redis `PING`, database `SELECT 1` (MySQL or Postgres, from `DATABASE_URL`), DNS for both |
| Queue | seven jobs through [laravel-cloud-queues](https://pypi.org/project/laravel-cloud-queues/): plain, async, delayed, retried, failing, timed out and a burst of 20 |
| Throughput | N async jobs (default 1,000) through the queue: jobs per second, p95 wait, lost and duplicated jobs |

Checks that only make sense on Cloud (proxy headers, log socket, cgroup limits) are skipped
when you run locally.

## Deploy to Laravel Cloud

One application, one environment per branch. Each branch differs from `main` only in
`.python-version` (which Cloud reads to pick the runtime) or `.web-server` (which `serve.py`
reads to pick the web server). `main` is Python 3.14 on uvicorn.

| Branch | Python | Web server |
|---|---|---|
| `python-3.10` … `python-3.14` | 3.10 … 3.14 | uvicorn |
| `server-gunicorn`, `server-uwsgi`, `server-waitress` | `main`'s | WSGI |
| `server-granian-wsgi`, `server-hypercorn-wsgi` | `main`'s | WSGI |
| `server-granian-asgi`, `server-hypercorn-asgi`, `server-daphne` | `main`'s | ASGI |

These are the servers Cloud's [Python deploy guide](https://cloud.laravel.com/docs/deploy-guides/python#run-a-production-server)
supports. To test a server on another Python, change `.python-version` on its branch and push.

Pushing a branch deploys its environment. To ship a change to every branch:

```sh
for b in python-3.10 python-3.11 python-3.12 python-3.13 python-3.14 \
         server-gunicorn server-uwsgi server-waitress server-granian-wsgi server-granian-asgi \
         server-hypercorn-wsgi server-hypercorn-asgi server-daphne; do
  git switch $b && git merge --no-edit main && git push
done
git switch main
```

Every environment uses the same settings, so results compare across versions:

- **Start command:** `python serve.py`. It execs the server named in `.web-server` with the
  command listed in `serve.py` (`[::]:$PORT`, workers from `WEB_CONCURRENCY`). The `python-3.x`
  environments may keep `uvicorn asgi:app --host :: --port $PORT`; it runs the same thing.
- **App and Worker cluster:** `pro.g-2vcpu-4gb`, autoscaling 1–6 replicas (CPU 60%, memory 70%), scale-to-zero off.
- **Worker cluster:** 4 processes of `laravel-cloud-queues work app:registry`. Don't add
  `--stop-when-empty`: Cloud restarts any worker that exits, which the timeout case relies on.
- **Valkey cache** attached (Cloud injects `REDIS_URL`) and `LARAVEL_CLOUD_QUEUES_BACKEND=redis`.
- **Database** (optional) attached; Cloud injects `DATABASE_URL`. Set `DB_SSL_VERIFY=0` for
  Cloud's self-signed database proxy.
- Deploy command empty. Build command (temporary, see [cloud-bootstrap/](cloud-bootstrap/README.md)):
  `if [ -d cloud-bootstrap ]; then mkdir -p "$(python -m site --user-site)" && cp cloud-bootstrap/laravel_cloud_bootstrap.py cloud-bootstrap/zz_laravel_cloud_bootstrap.pth "$(python -m site --user-site)/"; fi`

The server must accept both IPv6 and IPv4 on `$PORT`: Cloud's startup probes connect over IPv6,
while the pod's nginx currently connects to `127.0.0.1`. `[::]` is dual-stack for most servers;
waitress makes it IPv6-only, so `serve.py` also gives it `0.0.0.0`. Use `--host ::`, not
`--host ''`: with several workers uvicorn binds an IPv4-only socket for `''`, which the probes
can't reach.

The page header shows which server answered. Under WSGI servers the async and WebSocket rows
are skipped. waitress (threads) and daphne have no process count, so they run one process
whatever `WEB_CONCURRENCY` says, and waitress exits on SIGTERM without draining requests.

The app has no login. Protect it at the network level.

## Run locally

You need [uv](https://docs.astral.sh/uv/), Valkey/Redis (Laravel Herd bundles one on
`127.0.0.1:6379`) and optionally MySQL.

```sh
cp .env.example .env
uv run --env-file .env uvicorn asgi:app --host :: --port 8000   # http://localhost:8000
uv run --env-file .env laravel-cloud-queues work app:registry   # second terminal; more for more workers
```

In a terminal, logs print as readable lines; piped output and Cloud get the JSON lines Cloud
parses (`LOG_FORMAT=json` or `line` forces either). To read, filter or follow a running process
like `php artisan pail`, run it through laravel-cloud-logging's viewer:

```sh
uv run --env-file .env python -m laravel_cloud_logging.pretty -- uvicorn asgi:app --host :: --port 8000
uv run --env-file .env python -m laravel_cloud_logging.pretty --level warning -- laravel-cloud-queues work app:registry
```

It keeps the command's exit code and forwards `SIGTERM`, so supervisors still see crashes.
`solo.yml` runs Web and Worker this way.

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
uv run python test_servers.py [server ...]     # boots every server in serve.py the way Cloud does
```

## Files

- `app.py`: queue jobs, the queue check and the `handle()` router both servers share.
- `serve.py`, `.web-server`: the start command; picks the server.
- `asgi.py`, `wsgi.py`, `gunicorn.conf.py`: the ASGI and WSGI entrypoints (and gunicorn's settings).
- `checks.py`: the Web, Logging, Runtime and Services checks behind `GET /api/checks`.
- `throughput.py`: the throughput test behind `/api/throughput`.
- `telemetry.py`: job telemetry and the queue check verdicts.
- `logs.py`: logging setup through laravel-cloud-logging.
- `index.html`: the page (Tailwind from a CDN, no build step).
- `k6/http.js`: HTTP load script. `solo.yml`: local Web, Worker and test processes for [Solo](https://soloterm.com).
