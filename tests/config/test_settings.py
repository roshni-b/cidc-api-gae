import os
import importlib
from unittest.mock import MagicMock

from cidc_api.config import settings


def test_google_app_credentials(monkeypatch):
    """
    Check that the settings file loads credentials from the secrets bucket
    when none are provided on application startup.
    """
    get_secret_manager = MagicMock()
    get_secret_manager.return_value = secret_manager = MagicMock()
    secret_manager.get.return_value = "foobar"
    monkeypatch.setattr(
        "cidc_api.config.secrets.get_secrets_manager", get_secret_manager
    )

    monkeypatch.setattr(
        "cidc_api.config.db.get_sqlalchemy_database_uri",
        lambda _: os.environ["POSTGRES_URI"],
    )

    # Simulate application-startup conditions
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
    os.environ["TESTING"] = "False"

    importlib.reload(settings)

    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"].endswith(".json")

    os.environ["TESTING"] = "True"
