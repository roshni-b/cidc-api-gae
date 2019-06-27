from unittest.mock import patch, MagicMock

import auth
from app import app

# TODO: actually test a roundtrip auth flow with auth0?

TOKEN = "test-token"
RESOURCE = "test-resource"
EMAIL = "test@email.com"


@patch("models.Users.find_or_create")
@patch("auth.get_user_email", return_value=EMAIL)
def test_bearer_auth_success(get_user_email: MagicMock, find_or_create: MagicMock):
    """Check that auth succeeds when get_user_email succeeds."""
    with app.app_context():
        ba = auth.BearerAuth()
        authenticated = ba.check_auth(TOKEN, [], RESOURCE, "GET")
        assert authenticated
        get_user_email.assert_called_once_with(TOKEN)
        find_or_create.assert_called_once_with(EMAIL)


@patch("models.Users.find_or_create")
@patch("auth.get_user_email", side_effect=Exception)
def test_bearer_auth_failure(get_user_email: MagicMock, find_or_create: MagicMock):
    """Check that auth succeeds when get_user_email fails."""
    with app.app_context():
        ba = auth.BearerAuth()
        authenticated = ba.check_auth(TOKEN, [], RESOURCE, "GET")
        assert not authenticated
        get_user_email.assert_called_once_with(TOKEN)
        find_or_create.assert_not_called()
