# Porting to FastAPI, Django and Flask

This guide is for an agent (or a person) building a framework sibling of this repository:
the same load, compatibility and queue tests, served by FastAPI, Django or Flask, so the
platform can be tested with each framework's conventions. The rule that makes the ports
cheap: **keep the HTTP contract identical**. Same paths, methods, JSON bodies, status codes
and fields, so `k6/`, `index.html` and `TESTING.md` are reused unchanged and results compare
across frameworks.

[`fastapi-cloud-queues`](https://github.com/DGarbs51/fastapi-cloud-queues)
(`~/code/fastapi-cloud-queues`) is the existing FastAPI sibling. It has the dashboard and the
Run check, but not yet the load, environment and compatibility endpoints; porting this
repository's additions there is the FastAPI job.

## File map

| This repository | Role | FastAPI | Django | Flask |
|---|---|---|---|---|
| `app.py` | registry, jobs, `handle()` router, stdlib server, `SERVER` switch | `main.py` (app, routes, jobs) | `<project>/settings.py`, `urls.py`, `views.py`; jobs in `jobs.py` | `app.py` (app, routes, jobs) |
| `telemetry.py` | dashboard telemetry and the Run check | copy unchanged | copy unchanged | copy unchanged |
| `db.py` | engines, `LoadRow` model, `python -m db init` | copy unchanged | Django model + migration (see [Database](#database)) | copy unchanged |
| `compat.py` | runtime compatibility probes | copy unchanged | copy unchanged | copy unchanged |
| `index.html` | dashboard | copy unchanged | serve as a template or static file | copy unchanged |
| `wsgi.py`, `asgi.py`, `gunicorn.conf.py` | server adapters over `handle()` | not needed (uvicorn) | Django's own `wsgi.py`/`asgi.py` | not needed (`app:app`) |
| `k6/`, `TESTING.md` | load tests and test plan | copy unchanged | copy unchanged | copy unchanged |
| `pyproject.toml`, `uv.lock` | dependencies | add `fastapi`, `uvicorn` | add `django`, `gunicorn` (or `uvicorn`) | add `flask`, `gunicorn` |
| `.python-version` | runtime pick, one branch per version | same | same | same |

`telemetry.py` must stay byte-identical across all siblings; change it in every repository
at once or not at all.

## Contract to preserve

The authoritative contract is the one this repository implements; read `app.py`'s `handle()`
for the details. In short:

- `GET /` (dashboard), `GET /api/stats`, `POST /api/dispatch/<kind>`, `POST /api/check`,
  `POST /api/reset`: unchanged from the original demo.
- `GET /api/ping` -> `{"ok": true}`, touching neither Valkey nor the database.
- `GET /api/env`: allowlisted runtime fields only; never dump `os.environ`. Set `server` to
  what actually serves the request (`uvicorn`, `gunicorn`, `django`, ...).
- `POST /api/load`, `GET /api/load`, `GET /api/load/<run>`, `POST /api/load/<run>/cancel`:
  load runs, with background dispatch (the request returns 202 at once, well inside the
  20 s proxy timeout) and the Valkey admission lock `lcq-load:active` (409 while a run is
  active, or when more than 20000 jobs are queued).
- `POST /api/hold`: one memory hold per web process.
- `GET /api/compat`, `POST /api/compat/worker`.
- Optional server-feature routes: `GET /api/stream` (SSE), `POST /api/upload` (streamed
  hash), websocket `/ws/echo` (ASGI only).

Behaviour that must survive the port:

- Every POST requires `Content-Type: application/json` and returns 415 otherwise. Frameworks
  that parse forms or accept any type by default need an explicit guard (a FastAPI
  dependency, a Flask `before_request`, a Django decorator or middleware). There is no auth
  and no CSRF token: the JSON content type forces a CORS preflight, and the Cloud edge IP
  allowlist restricts access. In Django, mark these views `csrf_exempt`; the content-type
  guard replaces the token.
- Validation: integers must be real JSON integers (reject booleans, floats, strings), positive
  and within the caps; bodies up to 64 KiB; errors are `400 {"error": "..."}`. Do not let a
  framework coerce `"10"` into an int (Pydantic's lax mode does: use `StrictInt`).
- Error bodies are `{"error": "..."}`, not the framework's default (`{"detail": ...}` in
  FastAPI, HTML in Django and Flask). Add an error handler for 404, 405, 415 and 400.
- Routes have no trailing slash. Make sure the framework does not redirect to add or remove
  one (Django's `APPEND_SLASH`, Werkzeug's strict slashes): a redirected POST loses its body.

## From `handle()` to framework views

`handle(method, path, headers, body) -> (status, headers, body)` is a pure function, so the
quickest port wraps it in a catch-all view and changes nothing else. That is acceptable as a
first step, but it hides the framework's routing, request parsing and middleware, which is
what a framework port is supposed to test. The target is one view per route, each calling the
same plain functions `handle()` calls (`start_load`, `load_status`, `env_info`, ...: split
them out of `handle()` if they are inline):

```python
# FastAPI
@app.post("/api/load", status_code=202, dependencies=[Depends(require_json)])
def load(body: LoadRequest) -> dict[str, object]: ...

# Flask
@app.post("/api/load")
def load(): ...           # request.get_json(), return jsonify(...), 202

# Django (urls.py: path("api/load", views.load))
@csrf_exempt
@require_POST
def load(request): ...    # json.loads(request.body), JsonResponse(..., status=202)
```

Background dispatch uses a plain thread, as in `app.py`, in every framework, so it behaves
the same everywhere and outlives the request that started it.

## Jobs and the registry

Reuse the job functions and names unchanged (`load.sync_sleep`, `load.async_sleep`,
`load.cpu`, `load.mem`, `load.db_write`, `load.db_read`, `load.db_async`, `load.db_sync`,
`demo.compat`, and the `demo.*` check jobs), with the same `timeout` and `tries`. Job names
are the wire format: a renamed job is a different job.

- **FastAPI:** `queues = LaravelCloudQueues(app)` (`laravel-cloud-queues[fastapi,redis]`),
  `@queues.job(...)`, dispatch with `await job.dispatch_async()` in async views. The worker
  target is the app: `laravel-cloud-queues work main:app`.
- **Flask and Django:** there is no framework integration; use a plain `Registry()` as in
  `app.py`, `@registry.job(...)`, `job.dispatch()`. The worker target is the registry:
  `laravel-cloud-queues work jobs:registry`. For Django, the module that defines the
  registry must call `django.setup()` (with `DJANGO_SETTINGS_MODULE` set) before importing
  anything that uses models, because the worker imports it outside Django's startup.

The worker CLI runs one job at a time per process; concurrency is processes x replicas,
whatever the framework.

## Database

Same env vars (`DB_HOST`, `DB_PORT`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`, or
`DATABASE_URL`), TLS in Cloud and none locally (`DB_SSL=0`), the same `load_rows` table and
small pools (one connection per engine per process, no overflow): six worker replicas of four
processes must fit under the cluster's `max_connections`.

- **FastAPI and Flask:** copy `db.py` (SQLAlchemy 2.x, `pymysql` sync and `aiomysql` async
  engines) unchanged, including `python -m db init` as the deploy command.
- **Django:** use the Django ORM with a `LoadRow` model and one migration
  (`python manage.py migrate` as the deploy command). Set `CONN_MAX_AGE` below the server's
  idle timeout (the SQLAlchemy version recycles at 280 s), `CONN_HEALTH_CHECKS = True`, and
  `OPTIONS = {"ssl": {"ca": certifi.where()}}` in Cloud. The Django ORM is synchronous in
  practice (its async API runs queries in a thread), so `load.db_async` cannot be a real
  concurrent comparison: either keep SQLAlchemy `aiomysql` for that one job or report it as
  "not comparable" rather than drawing conclusions from it.

## Starting the servers

`$PORT` is set by Cloud; bind IPv6 and IPv4 (`::`), because the cluster network is IPv6.

| Framework | Start command | Worker processes |
|---|---|---|
| FastAPI | `uvicorn main:app --host :: --port $PORT` | `--workers $WEB_CONCURRENCY` if set |
| Django (WSGI) | `gunicorn <project>.wsgi:application -b [::]:$PORT` | gunicorn reads `WEB_CONCURRENCY` |
| Django (ASGI) | `uvicorn <project>.asgi:application --host :: --port $PORT` | as FastAPI |
| Flask | `gunicorn app:app -b [::]:$PORT` | gunicorn reads `WEB_CONCURRENCY` |

Cloud injects `WEB_CONCURRENCY=3` on 1 vCPU pods. Log the effective worker count at startup
and report it in `/api/env`: whether the platform's value is sensible for Python is itself a
test (TESTING.md, server modes). Memory holds and admission are per process, so with several
workers `/api/hold` lands on one of them.

## Laravel Cloud environment setup

Per Python version (3.10 to 3.14), one environment on its own branch:

1. Create the environment from the repository's `python-3.x` branch (each branch differs
   from `main` only in `.python-version`).
2. App cluster and Worker cluster: `dedicated.c-1vcpu-2gb`, custom autoscaling, minimum 1,
   maximum 6, CPU threshold 60%, memory threshold 70%.
3. Worker cluster background process: `laravel-cloud-queues work <target>` with 4
   processes. No `--stop-when-empty`.
4. Attach a Valkey cache (flex 250 MB); Cloud injects `REDIS_URL`. Set
   `LARAVEL_CLOUD_QUEUES_BACKEND=redis`.
5. Attach the environment's own schema on the shared MySQL cluster and note the injected
   variable names.
6. Leave the build command empty (Cloud installs from `uv.lock` with uv); set the start
   command from the table above and the deploy command (`python -m db init` or
   `python manage.py migrate`).
7. Check the edge IP allowlist is on: the endpoints have no login.

Push to `main`, merge `main` into every `python-3.x` branch, canary on 3.14 (check the release
SHA, `/api/env` and Run check), then roll out to the rest.

## Checklist

- [ ] `telemetry.py`, `compat.py`, `index.html` and `k6/` copied unchanged.
- [ ] Every route in [Contract to preserve](#contract-to-preserve) answers with the same
      status codes and JSON fields; `/api/env` reports the real server and framework.
- [ ] POSTs without `Content-Type: application/json` get 415; bad ints get 400 with
      `{"error": ...}`; unknown routes get 404 JSON.
- [ ] Job names, timeouts and tries match; the worker target resolves
      (`laravel-cloud-queues work <target>` starts and lists the jobs).
- [ ] Database pools are one connection per engine per process; schema created by the deploy
      command only, never at import.
- [ ] Imports and runs on Python 3.10 and 3.14:
      `uv run --python 3.10 python -m compileall -q . && uv run --python 3.14 python -m compileall -q .`
- [ ] Locally (Herd Valkey and MySQL), with the server and 4 workers running, every script
      passes: `BASE_URL=http://127.0.0.1:8000 k6 run k6/queue.js`, `e2e.js`, `compat.js`,
      and `stream.js` if the server supports it.
- [ ] README links the framework's siblings and this guide.
