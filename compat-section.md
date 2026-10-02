## Runtime compatibility probes

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
