"""Configuration for the CIDC API."""

from . import db
from . import secrets

get_sqlalchemy_database_uri = db.get_sqlachemy_database_uri
get_secret_manager = secrets.get_secrets_manager
