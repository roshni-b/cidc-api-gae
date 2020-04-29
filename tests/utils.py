"""Shortcuts that are useful across API tests."""

from cidc_api.models import Users, CIDCRole


def mock_current_user(user: Users, monkeypatch):
    """
    Override CIDC API authentication to set requests' current user to `user`.
    `monkeypatch` is an instance of the `pytest` monkeypatch fixture.
    """
    monkeypatch.setattr("cidc_api.shared.auth.authenticate", lambda *args: user)


def make_admin(user_id, app):
    """Update the user with id `user_id`'s role to cidc-admin."""
    with app.app_context():
        user = Users.find_by_id(user_id)
        user.role = CIDCRole.ADMIN.value
        user.update()
