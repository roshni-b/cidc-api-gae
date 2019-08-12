import pytest
from datetime import datetime
from jose import jwt
from unittest.mock import MagicMock
from flask import _request_ctx_stack
from werkzeug.exceptions import Unauthorized

from auth import BearerAuth
from models import Users

from .test_models import db_test

TOKEN = "test-token"
RESOURCE = "test-resource"
EMAIL = "test@email.com"
PUBLIC_KEY = {"kid": 1, "foo": "bar"}
JWKS = {"keys": [PUBLIC_KEY, {"kid": 2, "baz": "buz"}]}
HEADER = {"kid": 1}
PAYLOAD = {"email": EMAIL}


def make_raiser(exception):
    def raise_e(*args, **kwargs):
        raise exception

    return raise_e


throw_auth_error = make_raiser(Unauthorized("foo"))


@pytest.fixture
def bearer_auth(monkeypatch):
    monkeypatch.setattr("config.secrets.CloudStorageSecretManager", MagicMock)
    return BearerAuth()


def test_check_auth_smoketest(monkeypatch, app, bearer_auth):
    """Check that authentication succeeds if no errors are thrown"""
    # No authentication errors
    monkeypatch.setattr(bearer_auth, "token_auth", lambda _: PAYLOAD)
    # No authorization errors
    def fake_role_auth(*args):
        _request_ctx_stack.top.current_user = Users(email=EMAIL)
        return True

    monkeypatch.setattr(bearer_auth, "role_auth", fake_role_auth)
    # No database errors
    monkeypatch.setattr("models.Users.create", lambda _: Users(email=EMAIL))
    # Authentication should succeed
    with app.test_request_context("/"):
        authenticated = bearer_auth.check_auth(TOKEN, [], RESOURCE, "GET")
        assert authenticated
        assert _request_ctx_stack.top.current_user.email == EMAIL


def test_check_auth_auth_error(monkeypatch, bearer_auth):
    """Check that authentication fails if an Unauthorized is thrown by BearerAuth.token_auth"""
    # Raise an Unauthorized exception
    monkeypatch.setattr(bearer_auth, "token_auth", throw_auth_error)
    # Authentication should fail and bubble this error up
    with pytest.raises(Unauthorized):
        bearer_auth.check_auth(TOKEN, [], RESOURCE, "GET")


def test_token_auth(monkeypatch, bearer_auth):
    """
    Ensure that token auth succeeds if its subroutines succeed and bubbles
    up errors if subroutines error.
    """

    def get_public_key(token):
        assert token == TOKEN
        return PUBLIC_KEY

    def decode_id_token(token, public_key):
        assert token == TOKEN
        assert public_key == PUBLIC_KEY
        return PAYLOAD

    monkeypatch.setattr(bearer_auth, "get_issuer_public_key", get_public_key)
    monkeypatch.setattr(bearer_auth, "decode_id_token", decode_id_token)

    assert bearer_auth.token_auth(TOKEN) == PAYLOAD

    # Should raise an Unauthorized exception if get_issuer_public_key raises one
    monkeypatch.setattr(bearer_auth, "get_issuer_public_key", throw_auth_error)
    with pytest.raises(Unauthorized):
        bearer_auth.token_auth(TOKEN)

    # Reset get_issuer_public_key to not raise an exception
    monkeypatch.setattr(bearer_auth, "get_issuer_public_key", get_public_key)

    # Should raise an Unauthorized exception if decode_id_token raises one
    monkeypatch.setattr(bearer_auth, "decode_id_token", throw_auth_error)
    with pytest.raises(Unauthorized):
        bearer_auth.token_auth(TOKEN)


def test_get_issuer_public_key(monkeypatch, bearer_auth):
    """Test that public key-finding logic works"""

    def get_unverified_header(token):
        return HEADER

    def make_response(json_result):
        """Simulate a response from the /.well-known/jwks.json endpoint"""

        def response_dot_get(url):
            class MockResponse:
                def json(self):
                    return json_result

            return MockResponse()

        return response_dot_get

    monkeypatch.setattr("jose.jwt.get_unverified_header", get_unverified_header)

    # The response contains the public key we're looking for
    monkeypatch.setattr("requests.get", make_response(JWKS))
    assert bearer_auth.get_issuer_public_key(TOKEN) == PUBLIC_KEY

    # The response doesn't contain the public key we're looking for
    monkeypatch.setattr("requests.get", make_response({"keys": []}))
    with pytest.raises(Unauthorized):
        bearer_auth.get_issuer_public_key(TOKEN)


def test_decode_id_token(monkeypatch, bearer_auth):
    """Test that id_token-decoding logic works"""

    error_msg = "test error text"

    monkeypatch.setattr(
        "jose.jwt.decode", make_raiser(jwt.ExpiredSignatureError(error_msg))
    )
    with pytest.raises(Unauthorized, match=error_msg) as e:
        bearer_auth.decode_id_token(TOKEN, PUBLIC_KEY)

    monkeypatch.setattr("jose.jwt.decode", make_raiser(jwt.JWTClaimsError(error_msg)))
    with pytest.raises(Unauthorized, match=error_msg):
        bearer_auth.decode_id_token(TOKEN, PUBLIC_KEY)

    monkeypatch.setattr("jose.jwt.decode", make_raiser(jwt.JWTError(error_msg)))
    with pytest.raises(Unauthorized, match=error_msg):
        bearer_auth.decode_id_token(TOKEN, PUBLIC_KEY)

    def no_email(*args, **kwargs):
        return {}

    monkeypatch.setattr("jose.jwt.decode", no_email)
    with pytest.raises(Unauthorized):
        bearer_auth.decode_id_token(TOKEN, PUBLIC_KEY)

    def correct_email(*args, **kwargs):
        return PAYLOAD

    monkeypatch.setattr("jose.jwt.decode", correct_email)
    assert bearer_auth.decode_id_token(TOKEN, PUBLIC_KEY) == PAYLOAD


@db_test
def test_role_auth(bearer_auth, app, db):
    """Check that role-based authorization works as expected."""
    profile = {"email": EMAIL}

    with app.test_request_context():
        # Unregistered user should not be authorized to do anything to any resource except "users"
        with pytest.raises(Unauthorized, match="not registered"):
            bearer_auth.role_auth(profile, [], "some-resource", "some-http-method")

        # Unregistered user should not be able to GET users
        with pytest.raises(Unauthorized, match="not registered"):
            bearer_auth.role_auth(profile, [], "users", "GET")

        # Unregistered user should be able to POST users
        assert bearer_auth.role_auth(profile, [], "new_users", "POST")

    # Add the user to the db but don't approve yet
    Users.create(profile)

    with app.test_request_context():
        # Unapproved user isn't authorized to do anything
        with pytest.raises(Unauthorized, match="pending approval"):
            bearer_auth.role_auth(profile, [], "new_users", "POST")

        # Give the user a role but don't approve them
        db.query(Users).filter_by(email=EMAIL).update(dict(role="cimac-user"))
        db.commit()

        # Unapproved user *with an authorized role* still shouldn't be authorized
        with pytest.raises(Unauthorized, match="pending approval"):
            bearer_auth.role_auth(profile, ["cimac-user"], "new_users", "POST")

    # Approve the user
    db.query(Users).filter_by(email=EMAIL).update(dict(approval_date=datetime.now()))
    db.commit()

    with app.test_request_context():
        # If user doesn't have required role, they should not be authorized.
        with pytest.raises(Unauthorized, match="not authorized to access"):
            bearer_auth.role_auth(
                profile, ["cidc-admin"], "some-resource", "some-http-method"
            )

        # If user has an allowed role, they should be authorized
        assert bearer_auth.role_auth(
            profile, ["cimac-user"], "some-resource", "some-http-method"
        )

        # If the resource has no role restrictions, they should be authorized
        assert bearer_auth.role_auth(profile, [], "some-resource", "some-http-method")

