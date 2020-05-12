"""Configuration for the gunicorn WSGI server."""
import os

bind = f':{os.environ.get("PORT", 8080)}'
loglevel = "DEBUG"
timeout = 60
