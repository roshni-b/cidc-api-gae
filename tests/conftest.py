import os
import importlib

import pytest
from flask import Flask

os.environ["TESTING"] = "True"

from cidc_api.app import app
from cidc_api.models import (
    UploadJobs,
    Users,
    DownloadableFiles,
    TrialMetadata,
    Permissions,
)


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
