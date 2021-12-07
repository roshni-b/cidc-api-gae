"""Shortcuts that are useful across API tests."""
from unittest.mock import MagicMock

from cidc_api.models import Users, CIDCRole


def mock_current_user(user: Users, monkeypatch):
    """
    Override CIDC API authentication to set requests' current user to `user`.
    `monkeypatch` is an instance of the `pytest` monkeypatch fixture.
    """
    monkeypatch.setattr("cidc_api.shared.auth.authenticate", lambda *args: user)


def make_admin(user_id, app):
    """Update the user with id `user_id`'s role to cidc-admin."""
    make_role(user_id, CIDCRole.ADMIN.value, app)


def make_role(user_id, role, app):
    """Update the user with id `user_id`'s role to `role`."""
    with app.app_context():
        user = Users.find_by_id(user_id)
        user.role = role
        user.update()


def mock_gcloud_client(monkeypatch) -> MagicMock:
    """
    Mock `grant_download_permission` and `revoke_download_permission` methods on gcloud_client.

    NOTE: only mocks usages of these methods within the `cidc_api.models.models` module.
    """
    gcloud_client = MagicMock()
    gcloud_client.revoke_download_access = MagicMock()
    gcloud_client.grant_download_access = MagicMock()

    def reset_mocks():
        gcloud_client.grant_download_access.reset_mock()
        gcloud_client.revoke_download_access.reset_mock()

    gcloud_client.reset_mocks = reset_mocks

    monkeypatch.setattr(
        "cidc_api.models.models.grant_download_access",
        gcloud_client.grant_download_access,
    )
    monkeypatch.setattr(
        "cidc_api.models.models.revoke_download_access",
        gcloud_client.revoke_download_access,
    )

    return gcloud_client
