import json
import pytest

from app import app as eve_app
from auth import AuthError
from test_auth import throw_auth_error


@pytest.fixture
def app():
    return eve_app


@pytest.fixture
def client(app):
    return app.test_client()


def test_auth_error(monkeypatch, app, client):
    """Test that the AuthError handler is registered and catches errors"""
    app.before_request(throw_auth_error)
    response = client.get("/")
    data = json.loads(response.data)
    assert data == {"error_code": "foo", "message": "bar"}
    assert response.status_code == 401
