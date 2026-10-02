"""Copy this stdlib-only module into your app; call configure() at startup.

Plain script / Flask: configure() before logging / creating the Flask app.
FastAPI / uvicorn: configure(); uvicorn.run(app, log_config=None).
For multiple uvicorn workers, also configure() in the imported app module.
Gunicorn (gunicorn.conf.py): from cloud_logging import configure;
    logconfig_dict = configure(); accesslog = '-'
Django settings: LOGGING_CONFIG = None; configure()
Celery: set worker_hijack_root_logger=False, then connect a function calling
    configure() to celery.signals.setup_logging (use weak=False).
RQ: configure() after the worker's logging setup, before processing jobs.
Optional uncaught main/thread exceptions: configure(exceptions=True).

Cloud collects stdout and stderr equally; stdout keeps one stream and ordering.
Only the JSON 'level' sets severity, never the choice of stream. Tracebacks and
newlines are escaped into one physical line. No redaction: keep secrets out.
"""

import json
import logging
import logging.config
import os
import sys
import threading
from datetime import datetime, timezone

_STANDARD = set(logging.LogRecord('', 0, '', 0, '', (), None).__dict__)
_RESERVED = _STANDARD | {'message', 'asctime', 'ts', 'level', 'logger', 'exc', 'stack'}
_LEVELS = ((50, 'critical'), (40, 'error'), (30, 'warning'), (20, 'info'), (10, 'debug'))
_LOGGERS = ('uvicorn', 'uvicorn.error', 'uvicorn.access', 'gunicorn',
            'gunicorn.error', 'gunicorn.access', 'celery', 'django',
            'django.server', 'werkzeug', 'asyncio', 'py.warnings')


class JsonFormatter(logging.Formatter):
    def format(self, record):
        try:
            data = dict(
                ts=datetime.fromtimestamp(record.created, timezone.utc)
                .isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
                level=next((name for number, name in _LEVELS if record.levelno >= number), 'debug'),
                msg=record.getMessage(), logger=record.name,
            )
            if record.exc_info:
                data['exc'] = self.formatException(record.exc_info)
            elif record.exc_text:
                data['exc'] = record.exc_text
            if record.stack_info:
                data['stack'] = self.formatStack(record.stack_info)
            for key, value in record.__dict__.items():
                if key not in _RESERVED:
                    try:
                        json.dumps(value, allow_nan=False)
                    except (TypeError, ValueError, OverflowError, RecursionError):
                        value = str(value)
                    data[key] = value
            return json.dumps(data, allow_nan=False, separators=(',', ':'))
        except Exception:
            return '{"level":"error","msg":"log record formatting failed"}'


def _uncaught(kind, value, tb):
    if issubclass(kind, KeyboardInterrupt):
        sys.__excepthook__(kind, value, tb)
    elif not issubclass(kind, SystemExit):
        logging.getLogger('uncaught').error('Uncaught exception', exc_info=(kind, value, tb))


def configure(level=None, *, exceptions=False):
    """Replace configured handlers, capture warnings, and return a dict for Gunicorn.

    Repeat calls are safe. Unknown level names fall back to INFO; numeric levels
    work too. Exception hooks are opt-in and preserve normal interrupts/exits.
    """
    level = os.environ.get('LOG_LEVEL', 'INFO') if level is None else level
    if isinstance(level, str):
        level = logging.getLevelName(level.upper())
    if not isinstance(level, int):
        level = logging.INFO
    config = {
        'version': 1, 'disable_existing_loggers': False,
        'formatters': {'json': {'()': JsonFormatter}},
        'handlers': {'stdout': {'class': 'logging.StreamHandler',
                                'stream': 'ext://sys.stdout', 'formatter': 'json'}},
        'root': {'handlers': ['stdout'], 'level': level},
        'loggers': {name: {'handlers': [], 'level': level, 'propagate': True}
                    for name in _LOGGERS},
    }
    logging.config.dictConfig(config)
    logging.captureWarnings(True)
    if exceptions:
        sys.excepthook = _uncaught
        threading.excepthook = lambda args: _uncaught(args.exc_type, args.exc_value, args.exc_traceback)
    return config
