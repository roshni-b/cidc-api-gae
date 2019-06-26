from os import environ

from eve_sqlalchemy.config import DomainConfig, ResourceConfig
from dotenv import load_dotenv

from secrets import CloudStorageSecretManager
from models import Users

load_dotenv()

# Configure secrets manager
secrets = CloudStorageSecretManager(environ.get("SECRETS_BUCKET_NAME"))

# Auth0 configuration
AUTH0_DOMAIN = environ.get("AUTH0_DOMAIN")
AUTH0_AUDIENCE = environ.get("AUTH0_AUDIENCE")
AUTH0_CLIENT_ID = environ.get("AUTH0_CLIENT_ID")
AUTH0_CLIENT_SECRET = secrets.get("AUTH0_CLIENT_SECRET")

# Deployment environment
ENV = environ.get("ENV", "staging")
assert ENV in ("dev", "staging", "prod")

DEBUG = ENV == "dev" and environ.get("DEBUG")

# Database configuration
POSTGRES_URI = environ.get("POSTGRES_URI")
if not POSTGRES_URI:
    from sqlalchemy.engine.url import URL

    # If POSTGRES_URI env variable is not set,
    # we're connecting to a Cloud SQL instance.

    config: dict = {
        "drivername": "postgres",
        "username": environ.get("CLOUD_SQL_DB_USER"),
        "password": secrets.get("CLOUD_SQL_DB_PASS"),
        "database": environ.get("CLOUD_SQL_DB_NAME"),
    }

    if environ.get("CLOUD_SQL_INSTANCE_NAME"):
        # If CLOUD_SQL_INSTANCE_NAME is defined, we're connecting
        # via a unix socket from inside App Engine.
        config["query"] = {
            "host": "/cloudsql/%s" % environ.get("CLOUD_SQL_INSTANCE_NAME")
        }
    elif environ.get("CLOUD_SQL_DB_HOST"):
        # If CLOUD_SQL_DB_HOST is defined, we're connecting via
        # the database's public IP address. Google secures this
        # IP address for us, but it's still a good idea to keep
        # it a secret.
        config["host"] = secrets.get("CLOUD_SQL_DB_HOST")
    else:
        raise EnvironmentError(
            "POSTGRES_URI, CLOUD_SQL_INSTANCE_NAME, or CLOUD_SQL_DB_IP must be defined to connect to the database"
        )

    POSTGRES_URI = str(URL(**config))

SQLALCHEMY_DATABASE_URI = POSTGRES_URI
SQLALCHEMY_TRACK_MODIFICATIONS = False


RESOURCE_METHODS = ["GET", "POST"]
ITEM_METHODS = ["GET", "PUT", "PATCH"]


DOMAIN = DomainConfig({"users": ResourceConfig(Users)}).render()
