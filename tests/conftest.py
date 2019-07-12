import os
import sys

import pytest

# Add cidc-api modules to path
test_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(test_dir, "..", "cidc-api"))

os.environ["TESTING"] = "True"


@pytest.fixture
def app():
    """Return a 'fresh' instance of the app"""
    from app import app as eve_app

    yield eve_app

    # Clear the global app context between tests
    del sys.modules["app"]


@pytest.fixture
def app_no_auth(app, monkeypatch):
    """Return a 'fresh' instance of the app with auth disabled"""
    monkeypatch.setattr(app.auth, "authorized", lambda *args: True)
    return app
