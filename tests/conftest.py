import os
import sys

import pytest
from flask import _request_ctx_stack

# Add cidc-api modules to path
test_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(test_dir, "..", "cidc-api"))

# Can only import cidc-api modules after the above paths are set
from models import Users

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

    return app