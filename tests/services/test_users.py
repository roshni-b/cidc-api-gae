from datetime import datetime

from cidc_api.models import Users, CIDCRole

NEW_USERS = "new_users"
USERS = "users"

EMAIL = "test@email.com"
AUTH_HEADER = {"Authorization": "Bearer foo"}

profile = {"email": EMAIL}
other_profile = {"email": "foo@bar.org"}


def fake_token_auth(*args):
    return profile


def test_enforce_self_creation(app, db, monkeypatch):
    """Check that users can only create themselves"""
    monkeypatch.setattr(app.auth, "token_auth", fake_token_auth)

    client = app.test_client()

    # If there's a mismatch between the requesting user's email
    # and the email of the user to create, the user should not be created
    response = client.post(NEW_USERS, json=other_profile, headers=AUTH_HEADER)
    assert response.status_code == 401
    assert "not authorized to create use" in response.json["_error"]["message"]

    # Self-creation should work just fine
    response = client.post(NEW_USERS, json=profile, headers=AUTH_HEADER)
    assert response.status_code == 201  # Created


def test_get_self(app, db, monkeypatch):
    """Check that a low-privilege user can get their own account info"""
    monkeypatch.setattr(app.auth, "token_auth", fake_token_auth)

    client = app.test_client()

    # Create two new users
    with app.app_context():
        user = Users.create(profile)
        user.role = "cimac-user"
        user.approval_date = datetime.now()
        other_user = Users.create(other_profile)
        db.commit()

    # Check that a low-privs user can look themselves up at the users/self endpoint
    response = client.get(USERS + "/self", headers=AUTH_HEADER)
    assert response.status_code == 200
    user = response.json
    assert user["email"] == profile["email"]

    # Check that a low-privs user cannot look up other users
    response = client.get(USERS, headers=AUTH_HEADER)
    assert response.status_code == 401


def test_add_approval_date(app, db, monkeypatch):
    """Test that a user's approval_date is updated when their role is changed for the first time."""
    monkeypatch.setattr(app.auth, "token_auth", fake_token_auth)

    # Create one registered admin and one new user
    with app.app_context():
        db.add(
            Users(role=CIDCRole.ADMIN.value, approval_date=datetime.now(), **profile)
        )
        db.commit()
        Users.create(other_profile)

    client = app.test_client()

    def get_new_user():
        response = client.get(
            USERS + '?where={"email": "%s"}' % other_profile["email"],
            headers=AUTH_HEADER,
        )
        return response.json["_items"][0]

    def update_role_and_get_approval_date(role: str):
        new_user = get_new_user()
        response = client.patch(
            f"{USERS}/{new_user['id']}",
            headers={**AUTH_HEADER, "If-Match": new_user["_etag"]},
            json={"role": role},
        )
        assert response.status_code == 200
        updated_new_user = get_new_user()
        approval_date = updated_new_user.get("approval_date")
        assert approval_date is not None
        return approval_date

    # Approval date should be set on first role update
    first_approval = update_role_and_get_approval_date(CIDCRole.DEVELOPER.value)
    second_approval = update_role_and_get_approval_date(CIDCRole.ADMIN.value)
    assert first_approval == second_approval
