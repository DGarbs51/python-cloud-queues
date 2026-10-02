"""Runtime compatibility probes for the Laravel Cloud Python runtime.

run_probes() returns one dict per probe:
    {"name", "group", "min_python", "status": pass|fail|skip|info, "detail"}

It never raises and never signals the calling process. Anything that can block
or crash (DNS, outbound HTTPS, multiprocessing, signals, subinterpreters, DB)
runs in a child interpreter that is killed with its whole process group after
CHILD_TIMEOUT. All probes run concurrently and run_probes() returns within
BUDGET seconds even if a probe hangs. Newer syntax is checked by compiling
source strings, so this module imports on Python 3.10.

Must import and run on Python 3.10 through 3.14.
    uv run python -c "import compat, json; print(json.dumps(compat.run_probes(), indent=1))"
"""

from __future__ import annotations

import asyncio
import builtins
import codecs
import importlib
import importlib.util
import json
import locale
import os
import platform
import shutil
import signal
import site
import subprocess
import sys
import sysconfig
import tempfile
import textwrap
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from importlib import metadata
from typing import Callable
from urllib.parse import urlsplit

CHILD_TIMEOUT = 10
BUDGET = 25.0
PUBLIC_HOST = "pypi.org"
DEAD_BATTERIES = (
    "aifc", "audioop", "cgi", "cgitb", "chunk", "crypt", "imghdr", "lib2to3", "mailcap", "msilib",
    "nis", "nntplib", "ossaudiodev", "pipes", "sndhdr", "spwd", "sunau", "telnetlib", "uu", "xdrlib",
)
OPTIONAL_DEPS = {
    "sqlalchemy": "SQLAlchemy",
    "pymysql": "PyMySQL",
    "aiomysql": "aiomysql",
    "redis": "redis",
    "laravel_cloud_queues": "laravel-cloud-queues",
    "certifi": "certifi",
}

PROBES: list[tuple[str, str, tuple[int, int], Callable[[], tuple[str, str]]]] = []


def probe(name: str, group: str, min_python: tuple[int, int] = (3, 10)):
    def register(fn: Callable[[], tuple[str, str]]) -> Callable[[], tuple[str, str]]:
        PROBES.append((name, group, min_python, fn))
        return fn

    return register


def run_probes() -> list[dict[str, str]]:
    started = time.monotonic()
    pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="compat")
    futures = [pool.submit(_run_one, *entry) for entry in PROBES]
    wait(futures, timeout=BUDGET)
    # Unfinished probes keep their thread until their own child timeout reaps them.
    pool.shutdown(wait=False, cancel_futures=True)
    results = []
    for (name, group, min_python, _), future in zip(PROBES, futures):
        if future.done() and not future.cancelled():
            results.append(future.result())
        else:
            elapsed = time.monotonic() - started
            results.append(_entry(name, group, min_python, "fail", f"did not finish within the {elapsed:.0f} s budget"))
    return results


def _run_one(name: str, group: str, min_python: tuple[int, int], fn: Callable[[], tuple[str, str]]) -> dict[str, str]:
    if sys.version_info < min_python:
        return _entry(name, group, min_python, "skip", f"requires Python {_version(min_python)}, running {platform.python_version()}")
    try:
        status, detail = fn()
    except BaseException as exc:  # noqa: BLE001 - a probe must never take down the caller
        status, detail = "fail", f"{type(exc).__name__}: {exc}"
    return _entry(name, group, min_python, status, detail)


def _entry(name: str, group: str, min_python: tuple[int, int], status: str, detail: str) -> dict[str, str]:
    return {"name": name, "group": group, "min_python": _version(min_python), "status": status, "detail": detail}


def _version(v: tuple[int, int]) -> str:
    return f"{v[0]}.{v[1]}"


def _child(code: str, timeout: float = CHILD_TIMEOUT) -> tuple[str, str]:
    """Run code in a fresh interpreter; it must assign result = (status, detail)."""
    source = textwrap.dedent(code) + "\nimport json\nprint(json.dumps(result))\n"
    proc = subprocess.Popen(
        [sys.executable, "-c", source],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_group(proc)
        return "fail", f"timed out after {timeout:.0f} s"
    finally:
        if proc.poll() is None:
            _kill_group(proc)
    if proc.returncode != 0:
        tail = err.strip().splitlines()[-1:] or [f"exit {proc.returncode}"]
        return "fail", f"child exited {proc.returncode}: {tail[0]}"
    status, detail = json.loads(out.strip().splitlines()[-1])
    return status, detail


def _kill_group(proc: subprocess.Popen) -> None:
    # The child leads its own session, so this never reaches the calling process group.
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    proc.communicate()


def _exec(source: str) -> dict[str, object]:
    namespace: dict[str, object] = {}
    # dont_inherit: keep this module's `from __future__ import annotations` out of the probed source.
    exec(compile(textwrap.dedent(source), "<compat>", "exec", dont_inherit=True), namespace)
    return namespace


def _read(path: str) -> str | None:
    try:
        with open(path) as fh:
            return fh.read().strip()
    except OSError:
        return None


def _writable(directory: str) -> tuple[bool, str]:
    try:
        with tempfile.NamedTemporaryFile(dir=directory, prefix=".compat-") as fh:
            fh.write(b"ok")
            fh.flush()
        return True, f"{directory} writable"
    except OSError as exc:
        return False, f"{directory} not writable: {exc}"


def _is_utf8(encoding: str | None) -> bool:
    try:
        return codecs.lookup(encoding or "").name == "utf-8"
    except LookupError:
        return False


# Interpreter and version features


@probe("interpreter", "version")
def _interpreter() -> tuple[str, str]:
    return "info", (
        f"{platform.python_implementation()} {platform.python_version()} {platform.machine()} "
        f"{platform.platform()} executable={sys.executable} prefix={sys.prefix}"
    )


@probe("tomllib", "version", (3, 11))
def _tomllib() -> tuple[str, str]:
    tomllib = importlib.import_module("tomllib")
    assert tomllib.loads('a = 1\n[b]\nc = "d"') == {"a": 1, "b": {"c": "d"}}
    return "pass", "tomllib.loads round trip"


@probe("ExceptionGroup", "version", (3, 11))
def _exception_group() -> tuple[str, str]:
    group = builtins.ExceptionGroup("g", [ValueError(1), KeyError(2)])
    match, rest = group.split(ValueError)
    assert len(match.exceptions) == 1 and len(rest.exceptions) == 1
    return "pass", "builtin ExceptionGroup.split works"


@probe("asyncio.TaskGroup", "version", (3, 11))
def _task_group() -> tuple[str, str]:
    async def main() -> list[int]:
        async with asyncio.TaskGroup() as tg:
            tasks = [tg.create_task(asyncio.sleep(0, n)) for n in range(3)]
        return [t.result() for t in tasks]

    assert asyncio.run(main()) == [0, 1, 2]
    return "pass", "3 tasks completed"


@probe("asyncio.timeout", "version", (3, 11))
def _asyncio_timeout() -> tuple[str, str]:
    async def main() -> bool:
        try:
            async with asyncio.timeout(0.01):
                await asyncio.sleep(1)
        except TimeoutError:
            return True
        return False

    assert asyncio.run(main())
    return "pass", "TimeoutError raised after 10 ms"


@probe("sys.monitoring", "version", (3, 12))
def _sys_monitoring() -> tuple[str, str]:
    monitoring = sys.monitoring
    return "pass", f"DEBUGGER_ID tool={monitoring.get_tool(monitoring.DEBUGGER_ID)!r}"


@probe("dead batteries removed (PEP 594)", "version", (3, 13))
def _dead_batteries() -> tuple[str, str]:
    found = [f"{m} ({importlib.util.find_spec(m).origin})" for m in DEAD_BATTERIES if importlib.util.find_spec(m)]
    if found:
        return "info", "still importable (backport packages?): " + ", ".join(found)
    return "pass", f"all {len(DEAD_BATTERIES)} removed modules absent"


@probe("os.process_cpu_count", "version", (3, 13))
def _process_cpu_count() -> tuple[str, str]:
    return "pass", f"process_cpu_count={os.process_cpu_count()} cpu_count={os.cpu_count()}"


@probe("JIT", "version", (3, 13))
def _jit() -> tuple[str, str]:
    configured = "--enable-experimental-jit" in (sysconfig.get_config_var("CONFIG_ARGS") or "")
    jit = getattr(sys, "_jit", None)
    if jit is not None:
        return "info", f"configured={configured} available={jit.is_available()} enabled={jit.is_enabled()}"
    return "info", f"configured={configured} (sys._jit not present)"


@probe("free-threaded build", "version", (3, 13))
def _free_threaded() -> tuple[str, str]:
    build = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    gil = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else True
    return "info", f"Py_GIL_DISABLED={build} gil_enabled={gil}"


@probe("concurrent.interpreters", "version", (3, 14))
def _interpreters() -> tuple[str, str]:
    return _child(
        """
        from concurrent import interpreters
        from concurrent.futures import InterpreterPoolExecutor
        interp = interpreters.create()
        interp.exec("x = 6 * 7")
        interp.close()
        with InterpreterPoolExecutor(max_workers=2) as pool:
            values = list(pool.map(pow, [2, 3], [10, 2]))
        assert values == [1024, 9], values
        result = ("pass", "subinterpreter exec + InterpreterPoolExecutor map")
        """
    )


@probe("annotationlib", "version", (3, 14))
def _annotationlib() -> tuple[str, str]:
    ns = _exec(
        """
        import annotationlib
        def f(x: Undefined) -> int: ...
        annotations = annotationlib.get_annotations(f, format=annotationlib.Format.FORWARDREF)
        """
    )
    annotations = ns["annotations"]
    assert type(annotations["x"]).__name__ == "ForwardRef" and annotations["return"] is int, annotations
    return "pass", "deferred annotation resolved as ForwardRef"


@probe("multiprocessing default start method", "version", (3, 14))
def _default_start_method() -> tuple[str, str]:
    # Checked in a child: asking in-process would pin the caller's start method.
    status, detail = _child(
        """
        import multiprocessing
        result = ("", multiprocessing.get_start_method())
        """
    )
    if status == "fail":
        return status, detail
    expected = {"linux": "forkserver", "darwin": "spawn"}.get(sys.platform)
    return ("pass" if detail == expected else "info"), f"{detail} (expected {expected} on {sys.platform})"


# Syntax (compiled from source strings so this file stays 3.10-compatible)


@probe("except*", "syntax", (3, 11))
def _except_star() -> tuple[str, str]:
    ns = _exec(
        """
        caught = []
        try:
            raise ExceptionGroup("g", [ValueError(1), TypeError(2)])
        except* ValueError:
            caught.append("value")
        except* TypeError:
            caught.append("type")
        """
    )
    assert ns["caught"] == ["value", "type"], ns["caught"]
    return "pass", "both handlers ran"


@probe("PEP 695 type parameters", "syntax", (3, 12))
def _pep695() -> tuple[str, str]:
    ns = _exec(
        """
        type Pair[T] = tuple[T, T]
        def first[T](items: list[T]) -> T:
            return items[0]
        class Box[T]:
            def __init__(self, value: T) -> None:
                self.value = value
        value = first([7]) + Box(1).value
        params = len(Pair.__type_params__)
        """
    )
    assert ns["value"] == 8 and ns["params"] == 1
    return "pass", "type alias, generic function and class"


@probe("PEP 701 f-strings", "syntax", (3, 12))
def _pep701() -> tuple[str, str]:
    ns = _exec(
        """
        items = ["a", "b"]
        value = f"{"-".join(items)}{f"{'\\n'.join(items)}"!r}"
        """
    )
    assert ns["value"] == "a-b'a\\nb'", ns["value"]
    return "pass", "nested quotes and backslash in replacement field"


@probe("t-strings (PEP 750)", "syntax", (3, 14))
def _t_strings() -> tuple[str, str]:
    ns = _exec(
        """
        from string.templatelib import Template
        name = "cloud"
        template = t"hello {name}"
        ok = isinstance(template, Template) and template.values == ("cloud",)
        """
    )
    assert ns["ok"]
    return "pass", "string.templatelib.Template built"


# Build modules


@probe("ssl", "build")
def _ssl() -> tuple[str, str]:
    import ssl

    ssl.create_default_context()
    paths = ssl.get_default_verify_paths()
    return "pass", f"{ssl.OPENSSL_VERSION} TLSv1.3={ssl.HAS_TLSv1_3} cafile={paths.cafile} capath={paths.capath}"


@probe("sqlite3", "build")
def _sqlite3() -> tuple[str, str]:
    import sqlite3

    with sqlite3.connect(":memory:") as db:
        version = db.execute("select sqlite_version()").fetchone()[0]
        json_ok = db.execute("select json_extract('{\"a\": 1}', '$.a')").fetchone()[0] == 1
    db.close()
    return "pass", f"sqlite {version} json1={json_ok}"


def _round_trip(module_name: str) -> tuple[str, str]:
    module = importlib.import_module(module_name)
    data = b"laravel cloud " * 1000
    assert module.decompress(module.compress(data)) == data
    return "pass", f"{module_name} round trip {len(data)} bytes"


for _name in ("zlib", "lzma", "bz2"):
    probe(_name, "build")(lambda name=_name: _round_trip(name))
probe("compression.zstd", "build", (3, 14))(lambda: _round_trip("compression.zstd"))


@probe("ctypes", "build")
def _ctypes() -> tuple[str, str]:
    import ctypes

    libc = ctypes.CDLL(None)
    assert libc.getpid() == os.getpid()
    return "pass", f"libc getpid via ctypes, pointer size {ctypes.sizeof(ctypes.c_void_p)}"


@probe("_decimal", "build")
def _decimal() -> tuple[str, str]:
    module = importlib.import_module("_decimal")
    return "pass", f"libmpdec {module.__libmpdec_version__}"


@probe("readline", "build")
def _readline() -> tuple[str, str]:
    import readline

    backend = getattr(readline, "backend", "libedit" if "libedit" in (readline.__doc__ or "") else "readline")
    return "pass", f"{backend} {readline._READLINE_LIBRARY_VERSION}"


@probe("uuid", "build")
def _uuid() -> tuple[str, str]:
    import uuid

    uuid.uuid4()
    if importlib.util.find_spec("_uuid") is None:
        return "info", "_uuid C module missing (built without libuuid); pure-Python fallback in use"
    return "pass", "_uuid C module present"


@probe("dbm", "build")
def _dbm() -> tuple[str, str]:
    import dbm

    backends = [m for m in ("dbm.gnu", "dbm.ndbm", "dbm.sqlite3", "dbm.dumb") if _importable(m)]
    with tempfile.TemporaryDirectory() as tmp:
        with dbm.open(os.path.join(tmp, "probe"), "c") as db:
            db[b"k"] = b"v"
            assert db[b"k"] == b"v"
    return "pass", "backends: " + ", ".join(backends)


def _importable(name: str) -> bool:
    try:
        importlib.import_module(name)
        return True
    except ImportError:
        return False


@probe("hashlib", "build")
def _hashlib() -> tuple[str, str]:
    import hashlib

    wanted = ["md5", "sha256", "sha3_256", "blake2b", "shake_128"]
    missing = [a for a in wanted if a not in hashlib.algorithms_available]
    scrypt = hasattr(hashlib, "scrypt")
    detail = f"{len(hashlib.algorithms_available)} algorithms, scrypt={scrypt}"
    if missing:
        return "fail", f"{detail}, missing {missing}"
    return "pass", detail


@probe("zoneinfo", "build")
def _zoneinfo() -> tuple[str, str]:
    import zoneinfo

    system = any(os.path.exists(os.path.join(p, "America", "New_York")) for p in zoneinfo.TZPATH)
    source = "system tzdata" if system else ("tzdata package" if importlib.util.find_spec("tzdata") else "none")
    try:
        tz = zoneinfo.ZoneInfo("America/New_York")
    except zoneinfo.ZoneInfoNotFoundError as exc:
        return "fail", f"{exc}; source={source} TZPATH={list(zoneinfo.TZPATH)}"
    # 2024-03-10 02:00 local is the spring-forward gap: offset moves from -5 h to -4 h.
    before = datetime(2024, 3, 10, 1, 59, tzinfo=tz).utcoffset()
    after = datetime(2024, 3, 10, 3, 0, tzinfo=tz).utcoffset()
    hours = (before.total_seconds() / 3600, after.total_seconds() / 3600)
    if hours != (-5.0, -4.0):
        return "fail", f"DST transition wrong: offsets {hours} via {source}"
    return "pass", f"DST transition correct via {source}; TZPATH={list(zoneinfo.TZPATH)}"


# Container and platform


@probe("locale and encoding", "container")
def _locale() -> tuple[str, str]:
    preferred = locale.getpreferredencoding(False)
    with tempfile.TemporaryFile("w") as fh:
        default_open = fh.encoding
    detail = (
        f"preferred={preferred} open()={default_open} fs={sys.getfilesystemencoding()} "
        f"utf8_mode={sys.flags.utf8_mode} LC_CTYPE={locale.setlocale(locale.LC_CTYPE)} "
        f"LANG={os.environ.get('LANG')} LC_ALL={os.environ.get('LC_ALL')}"
    )
    return ("pass" if _is_utf8(preferred) and _is_utf8(default_open) else "fail"), detail


@probe("cpu count vs cgroup", "container")
def _cpu() -> tuple[str, str]:
    affinity = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
    process = os.process_cpu_count() if hasattr(os, "process_cpu_count") else None
    cpu_max = _read("/sys/fs/cgroup/cpu.max")
    quota = ""
    if cpu_max and not cpu_max.startswith("max"):
        limit, period = cpu_max.split()
        quota = f" (= {int(limit) / int(period):g} CPUs)"
    return "info", f"os.cpu_count={os.cpu_count()} affinity={affinity} process_cpu_count={process} cgroup cpu.max={cpu_max}{quota}"


@probe("cgroup memory", "container")
def _memory() -> tuple[str, str]:
    events = _read("/sys/fs/cgroup/memory.events") or ""
    oom = dict(line.split() for line in events.splitlines()).get("oom_kill")
    return "info", (
        f"memory.max={_read('/sys/fs/cgroup/memory.max')} memory.current={_read('/sys/fs/cgroup/memory.current')} "
        f"oom_kill={oom}"
    )


@probe("cgroup pids", "container")
def _pids() -> tuple[str, str]:
    return "info", f"pids.max={_read('/sys/fs/cgroup/pids.max')} pids.current={_read('/sys/fs/cgroup/pids.current')}"


@probe("/dev/shm", "container")
def _dev_shm() -> tuple[str, str]:
    if not os.path.isdir("/dev/shm"):
        return "skip", "/dev/shm not present on this platform"
    stat = os.statvfs("/dev/shm")
    size = f"size={stat.f_frsize * stat.f_blocks // 2**20} MiB free={stat.f_frsize * stat.f_bavail // 2**20} MiB"
    try:
        with tempfile.NamedTemporaryFile(dir="/dev/shm", prefix="compat-") as fh:
            fh.write(b"\0" * 2**20)
            fh.flush()
    except OSError as exc:
        return "fail", f"{size}; 1 MiB write failed: {exc}"
    return "pass", f"{size}; 1 MiB write ok"


def _pool_smoke(method: str) -> tuple[str, str]:
    import multiprocessing

    if method not in multiprocessing.get_all_start_methods():
        return "skip", f"{method} not available on {sys.platform}"
    return _child(
        f"""
        import multiprocessing
        with multiprocessing.get_context({method!r}).Pool(2) as pool:
            values = pool.map(abs, [-1, -2, -3])
        assert values == [1, 2, 3], values
        result = ("pass", "Pool(2).map ok")
        """
    )


for _method in ("fork", "spawn", "forkserver"):
    probe(f"multiprocessing {_method}", "container")(lambda method=_method: _pool_smoke(method))


@probe("subprocess", "container")
def _subprocess() -> tuple[str, str]:
    out = subprocess.run(["sh", "-c", "echo ok"], capture_output=True, text=True, timeout=5).stdout.strip()
    return ("pass" if out == "ok" else "fail"), f"sh -c echo -> {out!r}"


@probe("threads", "container")
def _threads() -> tuple[str, str]:
    done = []
    threads = [threading.Thread(target=done.append, args=(n,)) for n in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    return ("pass" if len(done) == 32 else "fail"), f"{len(done)}/32 threads ran; active={threading.active_count()}"


@probe("writable /tmp", "container")
def _tmp() -> tuple[str, str]:
    ok, detail = _writable(tempfile.gettempdir())
    return ("pass" if ok else "fail"), detail


@probe("writable cwd", "container")
def _cwd() -> tuple[str, str]:
    ok, detail = _writable(os.getcwd())
    return ("pass" if ok else "info"), detail


@probe("writable HOME", "container")
def _home() -> tuple[str, str]:
    ok, detail = _writable(os.path.expanduser("~"))
    return ("pass" if ok else "info"), f"HOME={os.environ.get('HOME')}: {detail}"


@probe("user site packages", "container")
def _user_site() -> tuple[str, str]:
    user_site = site.getusersitepackages()
    on_path = user_site in sys.path
    detail = (
        f"ENABLE_USER_SITE={site.ENABLE_USER_SITE} PYTHONUSERBASE={os.environ.get('PYTHONUSERBASE')} "
        f"user_site={user_site} on sys.path={on_path} sys.path={sys.path}"
    )
    # Laravel Cloud installs dependencies into PYTHONUSERBASE; they only import if that site is on sys.path.
    if os.environ.get("PYTHONUSERBASE") and not on_path:
        return "fail", detail
    return "pass", detail


@probe("uid/gid", "container")
def _uid() -> tuple[str, str]:
    import grp
    import pwd

    uid, gid = os.getuid(), os.getgid()
    user = pwd.getpwuid(uid).pw_name if _safe(lambda: pwd.getpwuid(uid)) else "?"
    group = grp.getgrgid(gid).gr_name if _safe(lambda: grp.getgrgid(gid)) else "?"
    return "info", f"uid={uid}({user}) gid={gid}({group}) euid={os.geteuid()} groups={os.getgroups()}"


def _safe(fn: Callable[[], object]) -> bool:
    try:
        fn()
        return True
    except (KeyError, OSError):
        return False


def _resolve(host: str | None, unset: str) -> tuple[str, str]:
    if not host:
        return "skip", unset
    # getaddrinfo has no timeout, so it runs in a child.
    status, detail = _child(
        f"""
        import socket
        found = {{}}
        for label, family in (("A", socket.AF_INET), ("AAAA", socket.AF_INET6)):
            try:
                found[label] = sorted({{a[4][0] for a in socket.getaddrinfo({host!r}, 443, family, socket.SOCK_STREAM)}})
            except OSError as exc:
                found[label] = str(exc)
        result = ("", found)
        """
    )
    if status == "fail":
        return status, f"{host}: {detail}"
    families = [k for k, v in detail.items() if isinstance(v, list)]
    return ("pass" if len(families) == 2 else "info" if families else "fail"), f"{host}: {detail}"


def _url_host(*names: str) -> str | None:
    for name in names:
        if os.environ.get(name):
            return urlsplit(os.environ[name]).hostname
    return None


@probe(f"DNS {PUBLIC_HOST}", "container")
def _dns_public() -> tuple[str, str]:
    return _resolve(PUBLIC_HOST, "")


@probe("DNS Redis host", "container")
def _dns_redis() -> tuple[str, str]:
    host = os.environ.get("REDIS_HOST") or _url_host("REDIS_URL", "LARAVEL_CLOUD_QUEUES_REDIS_URL")
    return _resolve(host, "REDIS_HOST / REDIS_URL not set")


@probe("DNS DB host", "container")
def _dns_db() -> tuple[str, str]:
    return _resolve(os.environ.get("DB_HOST") or _url_host("DATABASE_URL"), "DB_HOST / DATABASE_URL not set")


def _https(use_certifi: bool) -> tuple[str, str]:
    if use_certifi and importlib.util.find_spec("certifi") is None:
        return "skip", "certifi not installed"
    return _child(
        f"""
        import ssl, urllib.request
        if {use_certifi!r}:
            import certifi
            context = ssl.create_default_context(cafile=certifi.where())
        else:
            context = ssl.create_default_context()
        request = urllib.request.Request("https://{PUBLIC_HOST}/", method="HEAD")
        try:
            with urllib.request.urlopen(request, timeout=5, context=context) as response:
                result = ("pass", f"HTTP {{response.status}} from {PUBLIC_HOST}")
        except Exception as exc:
            result = ("fail", f"{{type(exc).__name__}}: {{exc}}")
        """
    )


probe("HTTPS default ssl context", "container")(lambda: _https(False))
probe("HTTPS certifi", "container")(lambda: _https(True))


@probe("SIGTERM handler", "container")
def _sigterm() -> tuple[str, str]:
    # The child signals only itself; the calling process is never signalled.
    return _child(
        """
        import os, signal, time
        received = []
        signal.signal(signal.SIGTERM, lambda signum, frame: received.append(signum))
        os.kill(os.getpid(), signal.SIGTERM)
        deadline = time.monotonic() + 2
        while not received and time.monotonic() < deadline:
            time.sleep(0.01)
        result = ("pass" if received else "fail", "handler ran" if received else "handler did not run")
        """
    )


@probe("stdout/stderr buffering", "container")
def _stdio() -> tuple[str, str]:
    def describe(stream: object) -> str:
        if stream is None:
            return "None"
        return (
            f"tty={stream.isatty()} line_buffering={getattr(stream, 'line_buffering', '?')} "
            f"write_through={getattr(stream, 'write_through', '?')}"
        )

    # `python -u` shows up as write_through on stdout; there is no sys.flags field for it.
    unbuffered = bool(os.environ.get("PYTHONUNBUFFERED"))
    out = sys.stdout
    flushes = out is not None and (unbuffered or getattr(out, "line_buffering", False) or getattr(out, "write_through", False))
    detail = f"PYTHONUNBUFFERED={os.environ.get('PYTHONUNBUFFERED')}; stdout {describe(out)}; stderr {describe(sys.stderr)}"
    return ("pass" if flushes else "info"), detail


@probe("C compiler", "container")
def _compiler() -> tuple[str, str]:
    configured = (sysconfig.get_config_var("CC") or "").split()[:1]
    found = {name: shutil.which(name) for name in ["gcc", "cc", *configured]}
    detail = ", ".join(f"{k}={v}" for k, v in found.items())
    return ("pass" if any(found.values()) else "info"), detail


@probe("uv", "container")
def _uv() -> tuple[str, str]:
    path = shutil.which("uv")
    if not path:
        return "info", f"uv not on PATH ({os.environ.get('PATH')})"
    version = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=5).stdout.strip()
    return "pass", f"{path}: {version}"


@probe("Laravel Cloud release vars", "container")
def _release_vars() -> tuple[str, str]:
    # Names only: values may be sensitive and /api/compat is reachable over HTTP.
    names = sorted(k for k in os.environ if k.startswith("LARAVEL_CLOUD_") and not k.startswith("LARAVEL_CLOUD_QUEUES_"))
    if not names:
        return "skip", "no LARAVEL_CLOUD_* vars (not running on Laravel Cloud)"
    missing = [k for k in ("LARAVEL_CLOUD_COMMIT_SHA", "LARAVEL_CLOUD_ENV_NAME") if k not in names]
    return ("fail" if missing else "pass"), f"present: {names}" + (f"; missing {missing}" if missing else "")


@probe("DB TLS connect", "container")
def _db_tls() -> tuple[str, str]:
    if not (os.environ.get("DB_HOST") or os.environ.get("DATABASE_URL")):
        return "skip", "DB_HOST / DATABASE_URL not set"
    if importlib.util.find_spec("pymysql") is None:
        return "skip", "pymysql not installed"
    # The child reads credentials from its inherited environment; none are put in the source or detail.
    return _child(
        """
        import os, ssl
        from urllib.parse import unquote, urlsplit
        import pymysql
        env = os.environ
        if env.get("DB_HOST"):
            host, port = env["DB_HOST"], int(env.get("DB_PORT") or 3306)
            user, password, database = env.get("DB_USERNAME"), env.get("DB_PASSWORD") or "", env.get("DB_DATABASE")
        else:
            url = urlsplit(env["DATABASE_URL"])
            host, port = url.hostname, url.port or 3306
            user, password = unquote(url.username or ""), unquote(url.password or "")
            database = url.path.lstrip("/") or None
        tls = env.get("DB_SSL", "1") != "0"
        context = None
        ca = "system"
        if tls:
            try:
                import certifi
                ca = "certifi"
                context = ssl.create_default_context(cafile=certifi.where())
            except ImportError:
                context = ssl.create_default_context()
        conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database,
                               ssl=context, connect_timeout=5, read_timeout=5)
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()[0]
            cur.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            row = cur.fetchone()
            cipher = row[1] if row else ""
        conn.close()
        if tls and not cipher:
            result = ("fail", f"connected to MySQL {version} but no TLS cipher negotiated")
        elif tls:
            result = ("pass", f"MySQL {version} over TLS {cipher}, CA {ca}")
        else:
            result = ("info", f"MySQL {version} without TLS (DB_SSL=0)")
        """
    )


# Optional dependencies (import status only; none are required by this module)


def _dependency(module_name: str, dist: str) -> tuple[str, str]:
    if importlib.util.find_spec(module_name) is None:
        return "info", "not installed"
    module = importlib.import_module(module_name)
    try:
        version = metadata.version(dist)
    except metadata.PackageNotFoundError:
        version = getattr(module, "__version__", "?")
    return "pass", f"{version} from {os.path.dirname(module.__file__ or '')}"


for _module, _dist in OPTIONAL_DEPS.items():
    probe(f"import {_module}", "deps")(lambda module=_module, dist=_dist: _dependency(module, dist))


if __name__ == "__main__":
    print(json.dumps(run_probes(), indent=1))
