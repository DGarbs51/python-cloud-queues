"""gunicorn settings, read automatically from the working directory.

Start command on Cloud: gunicorn wsgi:app --bind [::]:$PORT
Workers come from WEB_CONCURRENCY, which gunicorn reads by default.
"""

from laravel_cloud_logging import configure

# JSON from the master too. No accesslog: Cloud's nginx already logs each request.
logconfig_dict = configure()
# Under Cloud's 30 s shutdown budget minus its 5 s pre-drain (serve.GRACE for the other servers).
graceful_timeout = 20

# k6-gunicorn-gthread only: the common tuned-sync setup (gthread, 8 threads per worker, like Cloud Run's Python
# sample), next to k6-gunicorn's untuned defaults. Workers still come from WEB_CONCURRENCY.
worker_class = "gthread"
threads = 8
