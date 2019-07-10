import sys
import json
import pytest

from errors import AuthError, ServerError
from test_errors import make_raiser


@pytest.fixture
def app():
    from app import app as eve_app

    yield eve_app

    # Clear the global app context between tests
    del sys.modules["app"]


@pytest.mark.parametrize("error", [ServerError("foo", "bar"), AuthError("foo", "bar")])
def test_error_handlers(monkeypatch, app, error):
    """Test that the error handlers catch errors as expected"""
    app.before_request(make_raiser(error))
    response = app.test_client().get("/")
    data = json.loads(response.data)
    assert data == error.json()
    assert response.status_code == error.status_code
