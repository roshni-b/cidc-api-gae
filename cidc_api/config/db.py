from os import environ

from .secrets import get_secrets_manager


def get_sqlachemy_database_uri(testing: bool = False) -> str:
    """Get the PostgreSQL DB URI from environment variables"""

    db_uri = environ.get("POSTGRES_URI")
    secrets = get_secrets_manager(testing)
    if testing:
        # Connect to the test database
        db_uri = environ.get("TEST_POSTGRES_URI")
    elif not db_uri:
        from sqlalchemy.engine.url import URL

        # If POSTGRES_URI env variable is not set,
        # we're connecting to a Cloud SQL instance.

        config: dict = {
            "drivername": "postgresql",
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
        elif environ.get("CLOUD_SQL_PROXY_HOST") and environ.get(
            "CLOUD_SQL_PROXY_PORT"
        ):
            # If CLOUD_SQL_PROXY_HOST/PORT are defined, we're connecting
            # to Cloud SQL via a local cloud_sql_proxy.
            config["host"] = environ.get("CLOUD_SQL_PROXY_HOST")
            config["port"] = environ.get("CLOUD_SQL_PROXY_PORT")
        else:
            raise Exception(
                "Either POSTGRES_URI, CLOUD_SQL_INSTANCE_NAME, or "
                + "CLOUD_SQL_PROXY_HOST/PORT must be defined to connect "
                + "to a database."
            )

        db_uri = str(URL(**config))

    assert db_uri

    return db_uri
