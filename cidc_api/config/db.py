from os import environ

from .secrets import get_secrets_manager


def get_sqlachemy_database_uri(testing: bool = False) -> str:
    """Get the PostgreSQL DB URI from environment variables"""

    db_uri = environ.get("POSTGRES_URI")
    secrets = get_secrets_manager(testing)
    if testing:
        # Connect to the test database
        db_uri = environ.get("TEST_POSTGRES_URI", "fake-conn-string")
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
            socket_dir = environ.get("CLOUD_SQL_SOCKET_DIR", "/cloudsql/")

            # If CLOUD_SQL_INSTANCE_NAME is defined, we're connecting
            # via a unix socket from inside App Engine.
            config["query"] = {
                "host": f'{socket_dir}{environ.get("CLOUD_SQL_INSTANCE_NAME")}'
            }
        else:
            raise Exception(
                "Either POSTGRES_URI or CLOUD_SQL_INSTANCE_NAME must be defined to connect "
                + "to a database."
            )

        db_uri = str(URL(**config))

    assert db_uri

    return db_uri
