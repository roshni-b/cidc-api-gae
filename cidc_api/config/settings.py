import tempfile
from os import environ, path
from copy import deepcopy

from dotenv import load_dotenv

from . import db
from . import get_secret_manager
from cidc_api.models import get_DOMAIN

load_dotenv()


## Configure application environment
ENV = environ.get("ENV", "staging")
assert ENV in ("dev", "staging", "prod")
DEBUG = ENV == "dev" and environ.get("DEBUG")
TESTING = environ.get("TESTING") == "True"
MIN_CLI_VERSION = "0.8.0"
TEMPLATES_DIR = path.join("/tmp", "templates")
## End application environment config

## Configure Dev CFn
DEV_CFUNCTIONS_SERVER = environ.get("DEV_CFUNCTIONS_SERVER")
## End Dev CFn Config

secrets = get_secret_manager(TESTING)

## Configure Auth0
AUTH0_DOMAIN = environ.get("AUTH0_DOMAIN")
AUTH0_CLIENT_ID = environ.get("AUTH0_CLIENT_ID")
ALGORITHMS = ["RS256"]
## End Auth0 config

## Configure GCP
GOOGLE_CLOUD_PROJECT = environ.get("GOOGLE_CLOUD_PROJECT")
GOOGLE_UPLOAD_BUCKET = environ.get("GOOGLE_UPLOAD_BUCKET")
GOOGLE_UPLOAD_TOPIC = environ.get("GOOGLE_UPLOAD_TOPIC")
GOOGLE_DATA_BUCKET = environ.get("GOOGLE_DATA_BUCKET")
GOOGLE_UPLOAD_ROLE = environ.get("GOOGLE_UPLOAD_ROLE")
GOOGLE_PATIENT_SAMPLE_TOPIC = environ.get("GOOGLE_PATIENT_SAMPLE_TOPIC")
GOOGLE_EMAILS_TOPIC = environ.get("GOOGLE_EMAILS_TOPIC")
GOOGLE_ARTIFACT_UPLOAD_TOPIC = environ.get("GOOGLE_ARTIFACT_UPLOAD_TOPIC")

# Download the credentials file to a temporary file,
# then set the GOOGLE_APPLICATION_CREDENTIALS env variable
# to its path.
#
# NOTE: doing this shouldn't be necessary from within App Engine,
# but for some reason, google.cloud.storage.Blob.generate_signed_url
# fails with a credentials-related error unless this is explicitly
# set.
if not environ.get("GOOGLE_APPLICATION_CREDENTIALS") and not TESTING:
    creds_file_name = tempfile.mktemp(".json")
    with open(creds_file_name, "w") as creds_file:
        creds_file.write(secrets.get("APP_ENGINE_CREDENTIALS"))
    environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_file_name

## End GCP config

## Configure database
SQLALCHEMY_DATABASE_URI = db.get_sqlachemy_database_uri(TESTING)
SQLALCHEMY_TRACK_MODIFICATIONS = False
## End database config

## Configure Eve REST API
RESOURCE_METHODS = ["GET", "POST"]
ITEM_METHODS = ["GET", "PATCH"]
CACHE_CONTROL = "no-cache"
DOMAIN = get_DOMAIN()
PAGINATION_DEFAULT = 200
PAGINATION_LIMIT = 200
## End Eve REST API config
