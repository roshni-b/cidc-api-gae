"""Configuration for the gunicorn WSGI server."""
import os

from gevent.monkey import patch_all
from psycogreen.gevent import patch_psycopg

# The "gevent" worker class that we select below uses
# greenlets under the hood. Greenlets monkeypatch I/O
# funcionality to support async-io.
# gevent docs: http://www.gevent.org/
patch_all()
patch_psycopg()  # our postgres db driver needs to be patched directly

# Use async workers: https://docs.gunicorn.org/en/stable/design.html#async-workers
worker_class = "gevent"
# See https://docs.gunicorn.org/en/stable/settings.html
port = os.environ.get("PORT", 8080)
# Development mode - restart the server on code changes
reload = os.environ.get("ENV") == "dev"
# Include debug level-logs if we're in development mode
loglevel = "DEBUG" if reload else "INFO"
# Cancel ongoing requests after 500 seconds of waiting
timeout = 500
# Send all logs to stdout (where App Engine reads them from)
errorlog = "-"
