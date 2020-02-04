from unittest.mock import MagicMock
from datetime import datetime

from cidc_api.models import Users, Permissions, TrialMetadata, CIDCRole

from ..test_models import db_test
from ..conftest import TEST_EMAIL


def test_update_permissions_filters(app, db, monkeypatch):
    """Check that permissions are filtered for non-admin users"""
    payload = {"email": TEST_EMAIL}

    def token_auth(*args):
        return payload

    monkeypatch.setattr(app.auth, "token_auth", token_auth)

    with app.app_context():
        # Create users
        user = Users.create(payload)
        user.role = CIDCRole.CIMAC_USER.value
        user.approval_date = datetime.now()
        other_user = Users.create({"email": TEST_EMAIL + ".org"})

        user_id = user.id

        # Create trial
        trial_id = "foo"
        TrialMetadata.create(trial_id, {})

        # Create permissions
        def create_permission(uid, assay):
            db.add(
                Permissions(
                    granted_by_user=uid,
                    granted_to_user=uid,
                    trial_id=trial_id,
                    upload_type=assay,
                )
            )

        create_permission(user.id, "wes")
        create_permission(user.id, "olink")
        create_permission(other_user.id, "olink")

        db.commit()

    client = app.test_client()

    res = client.get("permissions", headers={"Authorization": "Bearer adlkjadsfl"})
    assert res.status_code == 200
    assert len(res.json["_items"]) == 2
    for perm in res.json["_items"]:
        assert perm["granted_to_user"] == user_id

    # Update user's role to admin
    with app.app_context():
        user = Users.find_by_id(user_id)
        user.role = CIDCRole.ADMIN.value
        db.commit()

    # Check that user can now read all permissions
    res = client.get("permissions", headers={"Authorization": "Bearer adlkjadsfl"})
    assert res.status_code == 200
    assert len(res.json["_items"]) == 3
