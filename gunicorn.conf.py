"""Configuration for the gunicorn WSGI server."""
import os

is_dev = os.environ.get("ENV") == "dev"

# See https://docs.gunicorn.org/en/stable/settings.html
port = os.environ.get("PORT", 8080)
loglevel = "DEBUG" if is_dev else "INFO"
reload = is_dev
timeout = 60
