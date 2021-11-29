import pytest
from unittest.mock import MagicMock

from cidc_api.csms import auth
from tests.csms.utils import mock_get_with_authorization


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


def test_get_with_paging(monkeypatch):
    global called
    called = 0

    def get_with_authorization(url, params: dict = {}):
        offset = params.get("offset")
        assert offset is not None

        global called
        called += 1
        ret = MagicMock()
        ret.json.return_value = (
            {"data": [{"foo": offset}]} if offset < 5 else {"data": []}
        )
        ret.status_code = 200 if "samples" in url else 300
        return ret

    monkeypatch.setattr(auth, "get_with_authorization", get_with_authorization)

    response = [v for v in auth.get_with_paging("samples")]
    assert len(response) == 5
    assert called == 6
    assert set([r["foo"] for r in response]) == set([0, 1, 2, 3, 4])

    called = 0
    response = [v for v in auth.get_with_paging("manifests")]
    assert len(response) == 0
    assert called == 1


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
