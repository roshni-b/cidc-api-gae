from os import environ
from copy import deepcopy

from dotenv import load_dotenv

from . import db
from models import get_DOMAIN

load_dotenv()


## Configure application environment
ENV = environ.get("ENV", "staging")
assert ENV in ("dev", "staging", "prod")
DEBUG = ENV == "dev" and environ.get("DEBUG")
TESTING = environ.get("TESTING") == "True"
## End application environment config

## Configure Auth0
AUTH0_DOMAIN = environ.get("AUTH0_DOMAIN")
AUTH0_CLIENT_ID = environ.get("AUTH0_CLIENT_ID")
ALGORITHMS = ["RS256"]
## End Auth0 config

## Configure GCP
GOOGLE_CLOUD_PROJECT = environ.get("GOOGLE_CLOUD_PROJECT")
GOOGLE_UPLOAD_BUCKET = environ.get("GOOGLE_UPLOAD_BUCKET")
GOOGLE_UPLOAD_TOPIC = environ.get("GOOGLE_UPLOAD_TOPIC")
GOOGLE_UPLOAD_ROLE = "roles/storage.objectCreator"
GOOGLE_EMAILS_TOPIC = environ.get("GOOGLE_EMAILS_TOPIC")
## End GCP config

## Configure database
SQLALCHEMY_DATABASE_URI = db.get_sqlachemy_database_uri(TESTING)
SQLALCHEMY_TRACK_MODIFICATIONS = False
## End database config

## Configure application constants
SUPPORTED_ASSAYS = ["wes"]
HINT_TO_SCHEMA = {
    "wes": "templates/metadata/wes_template.json",
    "pbmc": "templates/manifests/pbmc_template.json",
}
SCHEMA_TO_HINT = dict((schema, hint) for hint, schema in HINT_TO_SCHEMA.items())
## End configure constants

## Configure Eve REST API
RESOURCE_METHODS = ["GET", "POST"]
ITEM_METHODS = ["GET", "PUT", "PATCH"]
DOMAIN = get_DOMAIN()
## End Eve REST API config
