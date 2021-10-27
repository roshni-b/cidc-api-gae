import os

os.environ["TZ"] = "UTC"
from datetime import datetime, date

import pytest
from jose import jwt
from flask import Flask, g
from werkzeug.exceptions import Unauthorized, BadRequest, PreconditionFailed

from cidc_api.models import Users, CIDCRole
from cidc_api.shared import auth


def test_get_set_current_user(empty_app: Flask):
    """Check that getting and setting the current request's user works as expected."""
    # Test getting without setting
    with empty_app.app_context():
        with pytest.raises(
            AssertionError, match="no user associated with the current request"
        ):
            auth.get_current_user()

    # Test setting an instance of the wrong type
    with empty_app.app_context():
        with pytest.raises(
            AssertionError, match="must be an instance of the `Users` model"
        ):
            auth._set_current_user("foobar")

    # Test a successful setting and getting
    user = Users(email="test@email.com")
    with empty_app.app_context():
        auth._set_current_user(user)
        assert auth.get_current_user() == user


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


def test_validate_api_auth():
    """Check that validate_api_auth catches endpoints that don't have auth configured."""
    test_app = Flask("foo")

    @test_app.route("/public")
    @auth.public
    def public_endpoint():
        pass

    @test_app.route("/private")
    @auth.requires_auth("private_endpoint")
    def private_endpoint():
        pass

    @test_app.route("/unprotected_1")
    def unprotected_endpoint_1():
        pass

    @test_app.route("/unprotected_2")
    def unprotected_endpoint_2():
        pass

    with pytest.raises(
        AssertionError, match="unprotected_endpoint_1, unprotected_endpoint_2"
    ):
        auth.validate_api_auth(test_app)


def test_requires_auth(empty_app, monkeypatch):
    """
    Check that the requires_auth decorator behaves as expected
    """

    @empty_app.route("/test")
    @auth.requires_auth("test")
    def test_endpoint():
        return "ok!"

    client = empty_app.test_client()

    # 401 when no auth headers provided
    response = client.get("/test")
    assert response.status_code == 401

    # 401 when user is unauth'd
    monkeypatch.setattr("cidc_api.shared.auth.check_auth", lambda *args: False)
    response = client.get("/test")
    assert response.status_code == 401

    # 200 when user is auth'd
    monkeypatch.setattr("cidc_api.shared.auth.check_auth", lambda *args: True)
    response = client.get("/test")
    assert response.status_code == 200
    assert response.data == b"ok!"


def test_authenticate_and_get_user(cidc_api, monkeypatch):
    """Check that authenticate_and_get_user works as expected"""
    # Auth success
    monkeypatch.setattr("cidc_api.shared.auth.check_auth", lambda *args: True)
    test_user = Users(email="test@email.com")
    with cidc_api.app_context():
        auth._set_current_user(test_user)
        user = auth.authenticate_and_get_user()
        assert user == test_user

    # Auth failure
    monkeypatch.setattr("cidc_api.shared.auth.check_auth", make_raiser(Unauthorized))
    user = auth.authenticate_and_get_user()
    assert user is None


def test_check_auth_smoketest(monkeypatch, cidc_api):
    """Check that authentication succeeds if no errors are thrown"""
    # No authentication errors
    monkeypatch.setattr(auth, "authenticate", lambda: PAYLOAD)
    # No authorization errors
    def fake_role_auth(*args):
        auth._set_current_user(Users(email=EMAIL))
        return True

    monkeypatch.setattr(auth, "authorize", fake_role_auth)
    # No database errors
    monkeypatch.setattr("cidc_api.models.Users.create", lambda: Users(email=EMAIL))
    # Authentication should succeed
    with cidc_api.test_request_context("/"):
        authenticated = auth.check_auth([], RESOURCE, "GET")
        assert authenticated
        assert auth.get_current_user().email == EMAIL


def test_check_auth_auth_error(monkeypatch):
    """Check that authentication fails if an Unauthorized is thrown by BearerAuth.token_auth"""
    # Raise an Unauthorized exception
    monkeypatch.setattr(auth, "authenticate", throw_auth_error)
    # Authentication should fail and bubble this error up
    with pytest.raises(Unauthorized):
        auth.check_auth([], RESOURCE, "GET")


def test_enforce_cli_version(empty_app):
    target_version = "1.1.1"
    empty_app.config["MIN_CLI_VERSION"] = target_version

    def test_with_user_agent(client, version):
        with empty_app.test_request_context(
            "/", headers={"User-Agent": f"{client}/{version}"}
        ):
            auth._enforce_cli_version()

    test_with_user_agent("asdlfj", "")

    match = "upgrade to the most recent version"

    # Reject too-low cidc-cli clients
    too_low = ["0.1.2", "0.1.0"]
    for v in too_low:
        with pytest.raises(PreconditionFailed, match=match):
            test_with_user_agent("cidc-cli", v)

    # Accept high-enough CLI clients
    high_enough = ["1.1.2", "1.11.1"]
    for v in high_enough:
        test_with_user_agent("cidc-cli", v)

    # Accept non-CLI clients
    test_with_user_agent("Mozilla/2.0 Firefox", "")

    # Reject weird user-agent strings
    with empty_app.test_request_context(
        "/", headers={"User-Agent": f"not a valid user agent"}
    ):
        with pytest.raises(BadRequest, match="could not parse User-Agent string"):
            auth._enforce_cli_version()


def test_authenticate(empty_app, monkeypatch):
    """
    Ensure that authentication succeeds if its subroutines succeed and bubbles
    up errors if subroutines error.
    """

    def get_public_key(token):
        assert token == TOKEN
        return PUBLIC_KEY

    def decode_id_token(token, public_key):
        assert token == TOKEN
        assert public_key == PUBLIC_KEY
        return PAYLOAD

    monkeypatch.setattr(auth, "_get_issuer_public_key", get_public_key)
    monkeypatch.setattr(auth, "_decode_id_token", decode_id_token)

    token_request_context = empty_app.test_request_context(
        "/", headers={"Authorization": f"Bearer {TOKEN}"}
    )

    with token_request_context:
        user = auth.authenticate()
        assert user.email == EMAIL

    # Should raise an Unauthorized exception if get_issuer_public_key raises one
    monkeypatch.setattr(auth, "_get_issuer_public_key", throw_auth_error)
    with token_request_context:
        with pytest.raises(Unauthorized, match="foo"):
            auth.authenticate()

    # Reset get_issuer_public_key to not raise an exception
    monkeypatch.setattr(auth, "_get_issuer_public_key", get_public_key)

    # Should raise an Unauthorized exception if decode_id_token raises one
    monkeypatch.setattr(auth, "_decode_id_token", throw_auth_error)
    with token_request_context:
        with pytest.raises(Unauthorized, match="foo"):
            auth.authenticate()


def test_extract_token(empty_app):
    """Test that _extract_token handles edge cases"""
    token = "Case-Sensitive-Test-Token"

    # No auth header
    with empty_app.test_request_context("/"):
        with pytest.raises(Unauthorized):
            auth._extract_token()

    # No auth header but id_token present in JSON request body
    with empty_app.test_request_context("/", json={"id_token": token}):
        assert auth._extract_token() == token

    # Non-bearer auth headers
    with empty_app.test_request_context("/", headers={"authorization": ""}):
        with pytest.raises(Unauthorized):
            auth._extract_token()
    with empty_app.test_request_context("/", headers={"authorization": "Basic foo"}):
        with pytest.raises(Unauthorized):
            auth._extract_token()

    # Bearer missing token
    with empty_app.test_request_context("/", headers={"authorization": "Bearer"}):
        with pytest.raises(Unauthorized):
            auth._extract_token()

    # Well-formed auth header
    with empty_app.test_request_context(
        "/", headers={"authorization": f"Bearer {token}"}
    ):
        assert auth._extract_token() == token


def test_get_issuer_public_key(monkeypatch):
    """Test that public key-finding logic works"""
    error_str = "uh oh!"

    def get_unverified_header_error(token):
        raise jwt.JWTError(error_str)

    monkeypatch.setattr("jose.jwt.get_unverified_header", get_unverified_header_error)
    with pytest.raises(Unauthorized, match=error_str):
        auth._get_issuer_public_key(TOKEN)

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
    assert auth._get_issuer_public_key(TOKEN) == PUBLIC_KEY

    # The response doesn't contain the public key we're looking for
    monkeypatch.setattr("requests.get", make_response({"keys": []}))
    with pytest.raises(Unauthorized):
        auth._get_issuer_public_key(TOKEN)


def test_decode_id_token(monkeypatch):
    """Test that id_token-decoding logic works"""

    error_msg = "test error text"

    monkeypatch.setattr(
        "jose.jwt.decode", make_raiser(jwt.ExpiredSignatureError(error_msg))
    )
    with pytest.raises(Unauthorized, match=error_msg) as e:
        auth._decode_id_token(TOKEN, PUBLIC_KEY)

    monkeypatch.setattr("jose.jwt.decode", make_raiser(jwt.JWTClaimsError(error_msg)))
    with pytest.raises(Unauthorized, match=error_msg):
        auth._decode_id_token(TOKEN, PUBLIC_KEY)

    monkeypatch.setattr("jose.jwt.decode", make_raiser(jwt.JWTError(error_msg)))
    with pytest.raises(Unauthorized, match=error_msg):
        auth._decode_id_token(TOKEN, PUBLIC_KEY)

    def no_email(*args, **kwargs):
        return {}

    monkeypatch.setattr("jose.jwt.decode", no_email)
    with pytest.raises(Unauthorized):
        auth._decode_id_token(TOKEN, PUBLIC_KEY)

    def correct_email(*args, **kwargs):
        return PAYLOAD

    monkeypatch.setattr("jose.jwt.decode", correct_email)
    assert auth._decode_id_token(TOKEN, PUBLIC_KEY) == PAYLOAD


def test_authorize(cidc_api, clean_db):
    """Check that authorization works as expected."""
    user = Users(**PAYLOAD)

    with cidc_api.app_context():
        # Unregistered user should not be authorized to do anything to any resource except "users"
        with pytest.raises(Unauthorized, match="not registered"):
            auth.authorize(user, [], "some-resource", "some-http-method")

        # We can't track accesses for users who aren't registered
        assert user._accessed is None

        # Unregistered user should not be able to GET users
        with pytest.raises(Unauthorized, match="not registered"):
            auth.authorize(user, [], "users", "GET")
        assert user._accessed is None

        # Unregistered user should not be able to GET self
        with pytest.raises(Unauthorized, match="not registered"):
            auth.authorize(user, [], "self", "GET")
        assert user._accessed is None

        # Unregistered user should be able to POST users
        assert auth.authorize(user, [], "self", "POST")

        # Add the user to the db but don't approve yet
        user.insert()

        # Unapproved user isn't authorized to do anything
        with pytest.raises(Unauthorized, match="pending approval"):
            auth.authorize(user, [], "self", "POST")

        # Check that we tracked this user's last access
        assert user._accessed.date() == date.today()
        _accessed = user._accessed

        # Ensure unapproved user can access their own data
        assert auth.authorize(user, [], "self", "GET")

        # Give the user a role but don't approve them
        user.role = CIDCRole.CIMAC_USER.value
        user.update()

        # Unapproved user *with an authorized role* still shouldn't be authorized
        with pytest.raises(Unauthorized, match="pending approval"):
            auth.authorize(user, [CIDCRole.CIMAC_USER.value], "self", "POST")

        # Approve the user
        user.approval_date = datetime.now()
        user.update()

        # If user doesn't have required role, they should not be authorized.
        with pytest.raises(Unauthorized, match="not authorized to access"):
            auth.authorize(
                user, [CIDCRole.ADMIN.value], "some-resource", "some-http-method"
            )

        # If user has an allowed role, they should be authorized
        assert auth.authorize(
            user, [CIDCRole.CIMAC_USER.value], "some-resource", "some-http-method"
        )

        # If the resource has no role restrictions, they should be authorized
        assert auth.authorize(user, [], "some-resource", "some-http-method")

        # Disable user
        user.disabled = True
        user.update()

        # If user has an allowed role but is disabled, they should be unauthorized
        with pytest.raises(Unauthorized, match="disabled"):
            auth.authorize(
                user, [CIDCRole.CIMAC_USER.value], "some-resource", "some-http-method"
            )

        # Ensure unapproved user can access their own data
        assert auth.authorize(user, [], "self", "GET")

        # If the resource has no role restrictions, they should be still unauthorized
        with pytest.raises(Unauthorized, match="disabled"):
            auth.authorize(user, [], "some-resource", "some-http-method")

        # Check that user's last access wasn't updated by all activity,
        # since it occurred on the same day as previous accesses
        assert user._accessed == _accessed
