import sys
import logging

from flask import Flask, current_app, has_app_context

from .settings import ENV, TESTING


def init_logger(app: Flask):
    """Configure `app`'s loggers."""
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)


# Configure root logger as a fallback when no current_app is available
defaultLogger = logging.getLogger()
defaultLogger.setLevel(logging.DEBUG if ENV == "dev" or TESTING else logging.INFO)


def logger():
    """The current logger, depending on whether a flask app_context has been pushed."""
    if has_app_context() and not TESTING:
        return current_app.logger
    return defaultLogger
