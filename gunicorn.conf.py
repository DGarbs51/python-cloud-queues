"""gunicorn settings, read automatically from the working directory.

Start command on Cloud: gunicorn wsgi:app --bind [::]:$PORT
Workers come from WEB_CONCURRENCY, which gunicorn reads by default.
"""

from laravel_cloud_logging import configure

# JSON from the master too. No accesslog: Cloud's nginx already logs each request.
logconfig_dict = configure()
# Keep shutdown under Cloud's graceful shutdown timeout.
graceful_timeout = 30
