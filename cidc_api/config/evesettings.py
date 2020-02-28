import os
import tempfile

from cidc_api.config import db, get_secret_manager
from cidc_api.models import get_DOMAIN

TESTING = os.environ.get("TESTING")
TEMPLATES_DIR = os.path.join("/tmp", "templates")
MIN_CLI_VERSION = "0.8.2"

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
MEDIA_ENDPOINT = None
## End Eve REST API config

# Download the credentials file to a temporary file,
# then set the GOOGLE_APPLICATION_CREDENTIALS env variable
# to its path.
#
# NOTE: doing this shouldn't be necessary from within App Engine,
# but for some reason, google.cloud.storage.Blob.generate_signed_url
# fails with a credentials-related error unless this is explicitly
# set.
if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and not TESTING:
    secret_manager = get_secret_manager()
    creds_file_name = tempfile.mktemp(".json")
    with open(creds_file_name, "w") as creds_file:
        creds_file.write(secret_manager.get("APP_ENGINE_CREDENTIALS"))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_file_name
