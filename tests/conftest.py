import os
import sys

import pytest
from flask import _request_ctx_stack

# Add cidc_api modules to path
test_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(test_dir, "..", "cidc_api"))

# Can only import cidc_api modules after the above paths are set
from models import (
    Users,
    TrialMetadata,
    AssayUploads,
    Permissions,
    DownloadableFiles,
    ManifestUploads,
)

os.environ["TESTING"] = "True"


@pytest.fixture
def app():
    """Return a 'fresh' instance of the app"""
    from app import app as eve_app

    yield eve_app

    # Clear the global app context between tests
    del sys.modules["app"]


TEST_EMAIL = "test@email.com"


@pytest.fixture
def test_user():
    return Users(email=TEST_EMAIL)


@pytest.fixture
def app_no_auth(app, test_user, monkeypatch):
    """Return a 'fresh' instance of the app with auth disabled"""

    def fake_auth(*args):
        _request_ctx_stack.top.current_user = test_user
        return True

    monkeypatch.setattr(app.auth, "authorized", fake_auth)

    # Create test user
    client = app.test_client()
    client.post("new_users", json={"email": TEST_EMAIL})

    return app


@pytest.fixture
def db(app):
    """Provide a clean test database session"""
    session = app.data.driver.session
    session.query(AssayUploads).delete()
    session.query(ManifestUploads).delete()
    session.query(Users).delete()
    session.query(DownloadableFiles).delete()
    session.query(TrialMetadata).delete()
    session.query(Permissions).delete
    session.commit()

    return session
