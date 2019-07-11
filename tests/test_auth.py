import pytest
from jose import jwt
from unittest.mock import MagicMock

from errors import AuthError
from auth import BearerAuth

from test_errors import make_raiser

TOKEN = "test-token"
RESOURCE = "test-resource"
EMAIL = "test@email.com"
PUBLIC_KEY = {"kid": 1, "foo": "bar"}
JWKS = {"keys": [PUBLIC_KEY, {"kid": 2, "baz": "buz"}]}
HEADER = {"kid": 1}
PAYLOAD = {"email": EMAIL}


throw_auth_error = make_raiser(AuthError("foo", "bar"))


@pytest.fixture
def bearer_auth(monkeypatch):
    monkeypatch.setattr("secrets.CloudStorageSecretManager", MagicMock)
    return BearerAuth()


def test_check_auth_smoketest(monkeypatch, bearer_auth):
    """Check that authentication succeeds if no errors are thrown"""
    # No AuthErrors
    monkeypatch.setattr(bearer_auth, "token_auth", lambda _: PAYLOAD)
    # No database errors
    monkeypatch.setattr("models.Users.create", lambda _: True)
    # Authentication should succeed
    authenticated = bearer_auth.check_auth(TOKEN, [], RESOURCE, "GET")
    assert authenticated


def test_check_auth_auth_error(monkeypatch, bearer_auth):
    """Check that authentication fails if an AuthError is thrown by BearerAuth.token_auth"""
    # Raise an AuthError
    monkeypatch.setattr(bearer_auth, "token_auth", throw_auth_error)
    # Authentication should fail and bubble this error up
    with pytest.raises(AuthError):
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

    # Should raise an AuthError if get_issuer_public_key raises one
    monkeypatch.setattr(bearer_auth, "get_issuer_public_key", throw_auth_error)
    with pytest.raises(AuthError):
        bearer_auth.token_auth(TOKEN)

    # Reset get_issuer_public_key to not raise an exception
    monkeypatch.setattr(bearer_auth, "get_issuer_public_key", get_public_key)

    # Should raise an AuthError if decode_id_token raises one
    monkeypatch.setattr(bearer_auth, "decode_id_token", throw_auth_error)
    with pytest.raises(AuthError):
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
    with pytest.raises(AuthError, match="no_public_key"):
        bearer_auth.get_issuer_public_key(TOKEN)


def test_decode_id_token(monkeypatch, bearer_auth):
    """Test that id_token-decoding logic works"""

    def make_raiser(exception):
        def raise_e(*args, **kwargs):
            raise exception()

        return raise_e

    monkeypatch.setattr("jose.jwt.decode", make_raiser(jwt.ExpiredSignatureError))
    with pytest.raises(AuthError, match="expired_token"):
        bearer_auth.decode_id_token(TOKEN, PUBLIC_KEY)

    monkeypatch.setattr("jose.jwt.decode", make_raiser(jwt.JWTClaimsError))
    with pytest.raises(AuthError, match="invalid_claims"):
        bearer_auth.decode_id_token(TOKEN, PUBLIC_KEY)

    monkeypatch.setattr("jose.jwt.decode", make_raiser(jwt.JWTError))
    with pytest.raises(AuthError, match="invalid_signature"):
        bearer_auth.decode_id_token(TOKEN, PUBLIC_KEY)

    def no_email(*args, **kwargs):
        return {}

    monkeypatch.setattr("jose.jwt.decode", no_email)
    with pytest.raises(AuthError, match="id_token_required"):
        bearer_auth.decode_id_token(TOKEN, PUBLIC_KEY)

    def correct_email(*args, **kwargs):
        return PAYLOAD

    monkeypatch.setattr("jose.jwt.decode", correct_email)
    assert bearer_auth.decode_id_token(TOKEN, PUBLIC_KEY) == PAYLOAD
