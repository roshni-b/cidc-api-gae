USERS = "users"

EMAIL = "test@email.com"
AUTH_HEADER = {"Authorization": "Bearer foo"}


def test_enforce_self_creation(app, db, monkeypatch):
    """Check that users can only create themselves"""
    profile = {"email": EMAIL}

    def fake_token_auth(*args):
        return profile

    monkeypatch.setattr(app.auth, "token_auth", fake_token_auth)

    client = app.test_client()

    # If there's a mismatch between the requesting user's email
    # and the email of the user to create, the user should not be created
    other_profile = {"email": "foo@bar.org"}
    response = client.post(USERS, json=other_profile, headers=AUTH_HEADER)
    assert response.status_code == 401
    assert "not authorized to create use" in response.json["_error"]["message"]

    # Self-creation should work just fine
    response = client.post(USERS, json=profile, headers=AUTH_HEADER)
    assert response.status_code == 201  # Created
