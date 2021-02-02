import os
import importlib

import pytest
from webdriver_manager.chrome import ChromeDriverManager
from flask import Flask

# The below imports depend on these environment variables,
# so set it before importing them.
os.environ["TESTING"] = "True"
os.environ["DEBUG"] = "False"

from cidc_api.app import app
from cidc_api.models import (
    UploadJobs,
    Users,
    DownloadableFiles,
    TrialMetadata,
    Permissions,
)

# Install the Chrome web driver and add it to the PATH env variable
chromedriver_dir = os.path.dirname(ChromeDriverManager().install())
os.environ["PATH"] = f"{os.environ['PATH']}:{chromedriver_dir}"


@pytest.fixture
def empty_app():
    return Flask(__name__)


@pytest.fixture
def cidc_api():
    """An instance of the CIDC API"""
    return app


@pytest.fixture
def clean_cidc_api():
    """An instance of the CIDC API that hasn't yet handled any requests."""
    import cidc_api.app

    importlib.reload(cidc_api.app)

    return cidc_api.app.app


@pytest.fixture
def clean_db(cidc_api):
    """Provide a clean test database session"""
    with cidc_api.app_context():
        session = cidc_api.extensions["sqlalchemy"].db.session
        session.query(UploadJobs).delete()
        session.query(Users).delete()
        session.query(DownloadableFiles).delete()
        session.query(TrialMetadata).delete()
        session.query(Permissions).delete
        session.commit()

    return session
