"""Test constraints enforced by the Eve DOMAIN schema"""

from datetime import datetime

from .test_models import db_test

EMAIL = "test@email.com"


@db_test
def test_new_users_POST(app_no_auth, db):
    """Check that you can't create a new use with a role or approval date"""
    client = app_no_auth.test_client()

    # Someone malicious tries to create a new user for themselves with admin access
    user_with_role = {"email": EMAIL, "role": "cidc-admin"}

    response = client.post("/new_users", json=user_with_role)
    assert response.status_code == 422  # Unprocessable Entity

    # Someone malicious tries to create a new user for themselves with approval
    user_with_approval = {"email": EMAIL, "approval_date": datetime.now()}
    assert response.status_code == 422
