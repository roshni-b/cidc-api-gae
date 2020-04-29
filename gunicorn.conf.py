"""Configuration for the gunicorn WSGI server."""
import os
from multiprocessing import cpu_count

bind = f':{os.environ.get("PORT", 8080)}'
loglevel = "DEBUG"
timeout = 60
# See https://docs.gunicorn.org/en/stable/design.html#how-many-workers
workers = cpu_count() * 2 + 1
