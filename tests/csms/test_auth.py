import pytest
from unittest.mock import MagicMock

from cidc_api.csms import auth


def test_get_token_smoketest(monkeypatch):
    def fake_error_post():
        return {
            "errorCode": "invalid_client",
            "errorSummary": "Client authentication failed. Either the client or the client credentials are invalid.",
        }

    error_mock = MagicMock()
    error_mock.return_value = MagicMock(json=fake_error_post)
    monkeypatch.setattr(auth.requests, "post", error_mock)
    with pytest.raises(Exception, match="Client authentication failed"):
        auth.get_token()

    def fake_post():
        return {
            "access_token": True,
            "expires_in": 100,
        }

    post_mock = MagicMock()
    post_mock.return_value = MagicMock(json=fake_post)
    monkeypatch.setattr(auth.requests, "post", post_mock)

    assert auth.get_token()
    post_mock.assert_called_once()
    post_mock.reset_mock()

    assert auth.get_token()
    post_mock.assert_not_called()


def test_get_with_authorization(monkeypatch):
    token_mock = MagicMock()
    token_mock.return_value = "fake-token"
    monkeypatch.setattr(auth, "get_token", token_mock)

    get_mock = MagicMock()
    monkeypatch.setattr(auth.requests, "get", get_mock)

    auth.get_with_authorization("/foobar", headers={"beep": "boop"}, test="baz")
    get_mock.assert_called_with(
        f"{auth.CSMS_BASE_URL}/foobar",
        test="baz",
        headers={
            "Authorization": "Bearer fake-token",
            "accept": "*/*",
            "beep": "boop",
        },
    )
