import tempfile
from os import environ, path
from copy import deepcopy

from dotenv import load_dotenv

load_dotenv()


## Configure application environment
ENV = environ.get("ENV")
assert ENV in (
    "dev",
    "staging",
    "prod",
), "ENV environment variable must be set to 'dev', 'staging', or 'prod'"
DEBUG = ENV == "dev" and environ.get("DEBUG")
TESTING = environ.get("TESTING") == "True"
## End application environment config

## Configure Dev CFn
DEV_CFUNCTIONS_SERVER = environ.get("DEV_CFUNCTIONS_SERVER")
## End Dev CFn Config

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
# End GCP config
