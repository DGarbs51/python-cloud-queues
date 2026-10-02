"""Dependency-free checks: python3.10 test_cloud_logging.py (also run on 3.14).

With app dependencies and Herd running: python test_cloud_logging.py --local
Captures go to results/l8-logsample; uses Valkey db 6 and local MySQL root.
"""

import io
import json
import logging
import os
import re
import subprocess
import sys
import warnings
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from cloud_logging import JsonFormatter, configure


def check():
    output, old_handlers = io.StringIO(), io.StringIO()
    names = ('uvicorn', 'uvicorn.error', 'uvicorn.access', 'gunicorn.error',
             'gunicorn.access', 'celery', 'django', 'django.server', 'werkzeug',
             'asyncio', 'py.warnings')
    # Frameworks can install text handlers, disable propagation, or disable loggers.
    for name in ('', *names):
        logger = logging.getLogger(name)
        logger.addHandler(logging.StreamHandler(old_handlers))
        logger.propagate = False
        logger.disabled = name != ''

    with redirect_stdout(output), patch.dict(os.environ, {'LOG_LEVEL': 'debug'}):
        hook = sys.excepthook
        configure()
        configure()
        assert sys.excepthook is hook
        assert len(logging.getLogger().handlers) == 1
        assert logging.getLogger().level == logging.DEBUG
        for name in names:
            logger = logging.getLogger(name)
            assert not logger.handlers and logger.propagate and not logger.disabled
            logger.info('routed %s', name)
        with warnings.catch_warnings():
            warnings.simplefilter('always')
            warnings.warn('captured warning', UserWarning)
        entries = [json.loads(line) for line in output.getvalue().splitlines()]
        assert len(entries) == len(names) + 1
        assert [entry['logger'] for entry in entries] == [*names, 'py.warnings']
        assert not old_handlers.getvalue()
        output.seek(0)
        output.truncate()

        levels = {1: 'debug', 10: 'debug', 19: 'debug', 20: 'info', 29: 'info',
                  30: 'warning', 39: 'warning', 40: 'error', 49: 'error',
                  50: 'critical', 60: 'critical'}
        configure(1)
        for level in levels:
            logging.log(level, 'line one\nline two\r\n雪\u2028end')
        entries = [json.loads(line) for line in output.getvalue().splitlines()]
        assert len(entries) == len(levels)
        for entry, expected in zip(entries, levels.values()):
            assert entry['level'] == expected
            assert entry['msg'] == 'line one\nline two\r\n雪\u2028end'
            assert re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z', entry['ts'])
        output.seek(0)
        output.truncate()

        class Value:
            def __str__(self):
                return 'not JSON\nbut printable'

        circular = []
        circular.append(circular)
        try:
            try:
                raise ValueError('cause\nsecond line')
            except ValueError as exc:
                raise RuntimeError('outer') from exc
        except RuntimeError:
            logging.exception('failed %s', 'job', stack_info=True, extra={
                'request_id': '123', 'nested': {'items': [True, None, 3]},
                'object': Value(), 'nan': float('nan'), 'circular': circular,
                'ts': 'spoofed', 'level': 'info', 'logger': 'spoofed',
                'exc': 'spoofed', 'stack': 'spoofed',
            })
        lines = output.getvalue().splitlines()
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry['msg'] == 'failed job' and entry['level'] == 'error'
        assert entry['logger'] == 'root' and entry['ts'] != 'spoofed'
        assert 'ValueError: cause\nsecond line' in entry['exc']
        assert 'RuntimeError: outer' in entry['exc'] and 'direct cause' in entry['exc']
        assert 'Stack (most recent call last)' in entry['stack']
        assert entry['request_id'] == '123' and entry['nested'] == {'items': [True, None, 3]}
        assert entry['object'] == 'not JSON\nbut printable'
        assert entry['nan'] == 'nan' and entry['circular'] == '[[...]]'
        assert 'args' not in entry and 'levelname' not in entry

        class Broken:
            def __str__(self):
                raise ValueError('cannot format')

        formatter = JsonFormatter()
        for msg, args, extra in (('%d', ('bad',), {}), (Broken(), (), {}),
                                 ('bad extra', (), {'broken': Broken()})):
            record = logging.LogRecord('test', 20, __file__, 1, msg, args, None)
            record.__dict__.update(extra)
            fallback = formatter.format(record)
            assert len(fallback.splitlines()) == 1
            assert json.loads(fallback) == {'level': 'error', 'msg': 'log record formatting failed'}
        configure('warning')
        assert logging.getLogger().level == logging.WARNING
        with patch.dict(os.environ, {}, clear=True):
            configure()
            assert logging.getLogger().level == logging.INFO
        configure('invalid')
        assert logging.getLogger().level == logging.INFO

    for threaded in (False, True):
        script = ('from cloud_logging import configure\nconfigure(exceptions=True)\n'
                  'def crash():\n    raise RuntimeError("uncaught\\nsecond line")\n')
        script += ('import threading\nt = threading.Thread(target=crash)\nt.start()\nt.join()\n'
                   if threaded else 'crash()\n')
        result = subprocess.run([sys.executable, '-c', script], capture_output=True, text=True,
                                cwd=Path(__file__).parent, env={**os.environ, 'LOG_LEVEL': 'INFO'})
        assert result.returncode == (0 if threaded else 1), result.stderr
        assert not result.stderr, result.stderr
        lines = result.stdout.splitlines()
        assert len(lines) == 1, result.stdout
        entry = json.loads(lines[0])
        assert entry['level'] == 'error' and 'RuntimeError: uncaught\nsecond line' in entry['exc']
    print('cloud_logging checks passed on Python', sys.version.split()[0])


def local_check():
    import http.client
    import signal
    import socket
    import time
    import uuid

    root = Path(__file__).resolve().parent
    captures = root / 'results' / 'l8-logsample'
    captures.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, 'LOG_CONFIG': 'sample', 'LOG_LEVEL': 'INFO',
           'PYTHONDONTWRITEBYTECODE': '1', 'PYTHONUNBUFFERED': '1',
           'PATH': str(Path(sys.executable).parent) + os.pathsep + os.environ['PATH'],
           'LARAVEL_CLOUD_QUEUES_BACKEND': 'redis',
           'LARAVEL_CLOUD_QUEUES_REDIS_URL': 'redis://127.0.0.1:6379/6',
           'LARAVEL_CLOUD_QUEUES_REDIS_PREFIX': 'l8-proof-' + uuid.uuid4().hex + ':',
           'LARAVEL_CLOUD_QUEUES_REDIS_QUEUE': 'l8-proof',
           'DB_HOST': '127.0.0.1', 'DB_PORT': '3306', 'DB_USERNAME': 'root',
           'DB_PASSWORD': '', 'DB_DATABASE': 'python_cloud_queues', 'DB_SSL': '0'}
    env.pop('DATABASE_URL', None)

    def records(label):
        parsed, raw = [], []
        for line in (captures / (label + '.stdout')).read_text().splitlines():
            try:
                parsed.append(json.loads(line))
            except json.JSONDecodeError:
                raw.append(line)
        assert not (captures / (label + '.stderr')).read_text()
        assert all(line.startswith(('sigterm pid=', 'lifespan startup pid=',
                                    'lifespan shutdown pid=', 'gunicorn workers=')) for line in raw), raw
        print(label, 'JSON records:', len(parsed), 'existing print lines:', len(raw))
        return parsed

    for mode, workers in (('stdlib', 1), ('gunicorn', 1), ('uvicorn', 1), ('uvicorn', 2)):
        with socket.socket(socket.AF_INET6) as sock:
            sock.bind(('::1', 0))
            port = sock.getsockname()[1]
        label = mode + '-' + str(workers)
        with (captures / (label + '.stdout')).open('w') as stdout, \
                (captures / (label + '.stderr')).open('w') as stderr:
            proc = subprocess.Popen([sys.executable, '-B', 'app.py'], cwd=root,
                                    env={**env, 'SERVER': mode, 'PORT': str(port),
                                         'WEB_CONCURRENCY': str(workers)},
                                    stdout=stdout, stderr=stderr, start_new_session=True)
            try:
                deadline = time.monotonic() + 20
                while True:
                    assert proc.poll() is None, label + ' exited during startup'
                    connection = http.client.HTTPConnection('::1', port, timeout=2)
                    try:
                        connection.request('GET', '/api/ping')
                        response = connection.getresponse()
                        assert response.status == 200 and json.loads(response.read())['ok']
                        break
                    except OSError:
                        assert time.monotonic() < deadline, label + ' did not start'
                        time.sleep(0.1)
                    finally:
                        connection.close()
                for route in ('/api/env', '/api/stats'):
                    connection = http.client.HTTPConnection('::1', port, timeout=5)
                    try:
                        connection.request('GET', route)
                        response = connection.getresponse()
                        assert response.status == 200
                        data = json.loads(response.read())
                        if route == '/api/env':
                            assert data['db'] == 'ok', data['db']
                    finally:
                        connection.close()
            finally:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
                    raise
        entries = records(label)
        assert sum(e.get('logger') == 'cloud_demo' and e.get('msg') == 'startup'
                   for e in entries) == workers
        if mode != 'stdlib':
            startup = next(e for e in entries if e['logger'] == mode + '.error'
                           and ('Starting gunicorn' in e['msg'] or 'Started server process' in e['msg']))
            accesses = [e for e in entries if e['logger'] == mode + '.access' and '/api/ping' in e['msg']]
            assert len(accesses) == 1, accesses
            print(json.dumps(startup))
            print(json.dumps(accesses[0]))

    dispatch = subprocess.run([sys.executable, '-B', '-c',
                               'import app; print(app.quick.dispatch().uuid)'], cwd=root,
                              env=env, capture_output=True, text=True, check=True)
    job = dispatch.stdout.splitlines()[-1]
    with (captures / 'worker.stdout').open('w') as stdout, \
            (captures / 'worker.stderr').open('w') as stderr:
        subprocess.run([str(Path(sys.executable).parent / 'laravel-cloud-queues'),
                        'work', 'app:registry', '--max-jobs', '1', '--max-time', '10',
                        '--sleep', '0.1'], cwd=root, env=env, stdout=stdout, stderr=stderr,
                       check=True, timeout=20)
    entries = records('worker')
    assert sum(e.get('logger') == 'cloud_demo' and e.get('msg') == 'startup' for e in entries) == 1
    assert any(e.get('logger') == 'laravel_cloud_queues.worker' for e in entries)
    import redis
    store = redis.Redis.from_url(env['LARAVEL_CLOUD_QUEUES_REDIS_URL'])
    events = [json.loads(e) for e in store.lrange('lcq-demo:job:' + job, 0, -1)]
    assert [e['event'] for e in events] == ['started', 'processed'], events
    print('Herd proof passed: three servers, uvicorn multiprocess, MySQL, and one completed queue job')
    print('Captures:', captures)


if __name__ == '__main__':
    if sys.argv[1:] == ['--local']:
        local_check()
    else:
        check()
