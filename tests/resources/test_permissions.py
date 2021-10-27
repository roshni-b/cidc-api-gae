import os

os.environ["TZ"] = "UTC"
from cidc_api.models.models import ALL_UPLOAD_TYPES
from unittest.mock import MagicMock
from datetime import datetime
from typing import Tuple

from cidc_api.models import (
    Users,
    Permissions,
    PermissionSchema,
    TrialMetadata,
    CIDCRole,
)
from cidc_api.config.settings import GOOGLE_MAX_DOWNLOAD_PERMISSIONS

from ..utils import mock_current_user, make_admin, mock_gcloud_client

TRIAL_ID = "foo"


def setup_permissions(cidc_api, monkeypatch) -> Tuple[int, int]:
    """
    Create two users, one trial, and three permissions in `db`.
    Two permissions will belong to the first user, and the third will
    belong to the second one. Returns the first and second user ids 
    as a tuple.
    """
    current_user = Users(
        id=1,
        email="test@email.com",
        role=CIDCRole.CIMAC_USER.value,
        approval_date=datetime.now(),
    )
    other_user = Users(id=2, email="other@email.org")

    mock_current_user(current_user, monkeypatch)

    with cidc_api.app_context():
        # Create users
        current_user.insert()
        other_user.insert()

        # Create trial
        TrialMetadata.create(
            TRIAL_ID,
            {
                "protocol_identifier": TRIAL_ID,
                "allowed_collection_event_names": [],
                "allowed_cohort_names": [],
                "participants": [],
            },
        )

        # Create permissions
        def create_permission(uid, assay):
            Permissions(
                granted_by_user=uid,
                granted_to_user=uid,
                trial_id=TRIAL_ID,
                upload_type=assay,
            ).insert()

        create_permission(current_user.id, "ihc")
        create_permission(current_user.id, "olink")
        create_permission(other_user.id, "olink")

        return current_user.id, other_user.id


def test_list_permissions(cidc_api, clean_db, monkeypatch):
    """Check that listing permissions works as expected."""
    mock_gcloud_client(monkeypatch)
    current_user_id, other_user_id = setup_permissions(cidc_api, monkeypatch)

    client = cidc_api.test_client()

    # Check that user can get their own permissions
    res = client.get(f"permissions?user_id={current_user_id}")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 2
    assert res.json["_meta"]["total"] == 2
    for perm in res.json["_items"]:
        assert perm["granted_to_user"] == current_user_id

    # Check that a non-admin user can't get another user's permissions
    res = client.get(f"permissions?user_id={other_user_id}")
    assert res.status_code == 401
    assert "cannot view permissions for other users" in res.json["_error"]["message"]

    # Check that an admin can read the other user's permissions
    make_admin(current_user_id, cidc_api)
    res = client.get(f"permissions?user_id={other_user_id}")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 1
    assert res.json["_meta"]["total"] == 1


def test_get_permission(cidc_api, clean_db, monkeypatch):
    """Check that getting a single permission by ID works as expected."""
    mock_gcloud_client(monkeypatch)
    current_user_id, other_user_id = setup_permissions(cidc_api, monkeypatch)

    with cidc_api.app_context():
        current_user_perm = Permissions.find_for_user(current_user_id)[0]
        other_user_perm = Permissions.find_for_user(other_user_id)[0]

    client = cidc_api.test_client()

    # Check that getting a permission that doesn't exist yields 404
    res = client.get("permissions/123212321")
    assert res.status_code == 404

    # Check that a non-admin getting another user's permission yields 404
    res = client.get(f"permissions/{other_user_perm.id}")
    assert res.status_code == 404

    # Check that a non-admin can get their own permission
    res = client.get(f"permissions/{current_user_perm.id}")
    assert res.status_code == 200
    assert res.json == PermissionSchema().dump(current_user_perm)

    # Check that an admin can get another user's permission
    make_admin(current_user_id, cidc_api)
    res = client.get(f"permissions/{other_user_perm.id}")
    assert res.status_code == 200
    assert res.json == PermissionSchema().dump(other_user_perm)


def test_create_permission(cidc_api, clean_db, monkeypatch):
    """Check that creating a new permission works as expected."""
    gcloud_client = mock_gcloud_client(monkeypatch)
    current_user_id, other_user_id = setup_permissions(cidc_api, monkeypatch)

    client = cidc_api.test_client()

    # Non-admins should be blocked from posting to this endpoint
    gcloud_client.reset_mocks()
    res = client.post("permissions")
    assert res.status_code == 401
    assert "not authorized to access this endpoint" in res.json["_error"]["message"]
    gcloud_client.grant_lister_access.assert_not_called()
    gcloud_client.grant_download_access.assert_not_called()
    gcloud_client.revoke_lister_access.assert_not_called()
    gcloud_client.revoke_download_access.assert_not_called()

    make_admin(current_user_id, cidc_api)
    perm = {
        "granted_to_user": other_user_id,
        "trial_id": TRIAL_ID,
        "upload_type": "ihc",
    }

    # When an IAM grant error occurs, the permission db record shouldn't be created
    gcloud_client.reset_mocks()
    gcloud_client.grant_download_access.side_effect = Exception("oops")
    res = client.post("permissions", json=perm)
    assert "IAM grant failed" in res.json["_error"]["message"]
    assert res.status_code == 500
    with cidc_api.app_context():
        assert clean_db.query(Permissions).filter_by(**perm).all() == []
    gcloud_client.grant_download_access.side_effect = None

    # Admins can't create permissions with invalid upload types
    gcloud_client.reset_mocks()
    res = client.post("permissions", json={**perm, "upload_type": "foo"})
    assert res.status_code == 422
    assert "invalid upload type: foo" in res.json["_error"]["message"]

    # Admins should be able to create new permissions
    gcloud_client.reset_mocks()
    res = client.post("permissions", json=perm)
    assert res.status_code == 201
    assert "id" in res.json
    assert {**res.json, **perm} == res.json
    with cidc_api.app_context():
        assert Permissions.find_by_id(res.json["id"])
    gcloud_client.grant_lister_access.assert_called_once()
    gcloud_client.grant_download_access.assert_called_once()
    gcloud_client.revoke_lister_access.assert_not_called()
    gcloud_client.revoke_download_access.assert_not_called()

    # Re-insertion is not allowed
    gcloud_client.reset_mocks()
    res = client.post("permissions", json=perm)
    assert res.status_code == 400
    assert "unique constraint" in res.json["_error"]["message"]
    gcloud_client.grant_lister_access.assert_not_called()
    gcloud_client.grant_download_access.assert_not_called()
    gcloud_client.revoke_lister_access.assert_not_called()
    gcloud_client.revoke_download_access.assert_not_called()

    # The permission grantee must exist
    gcloud_client.reset_mocks()
    perm["granted_to_user"] = 999999999  # user doesn't exist
    res = client.post("permissions", json=perm)
    assert res.status_code == 400
    assert "user must exist, but no user found" in res.json["_error"]["message"]
    gcloud_client.grant_lister_access.assert_not_called()
    gcloud_client.grant_download_access.assert_not_called()
    gcloud_client.revoke_lister_access.assert_not_called()
    gcloud_client.revoke_download_access.assert_not_called()

    with cidc_api.app_context():
        clean_db.query(Permissions).delete()
        clean_db.commit()

    # # ----- This subtest has become unwieldy as GOOGLE_MAX_DOWNLOAD_PERMISSIONS is so large -----
    # # The permission grantee must have <= GOOGLE_MAX_DOWNLOAD_PERMISSIONS
    # perm["granted_to_user"] = current_user_id
    # inserts_fail_eventually = False
    # upload_types = list(ALL_UPLOAD_TYPES)
    # for i in range(GOOGLE_MAX_DOWNLOAD_PERMISSIONS + 1):
    #     gcloud_client.reset_mocks()
    #     perm["upload_type"] = upload_types[i % len(upload_types)]
    #     res = client.post("permissions", json=perm)
    #     if res.status_code != 201:
    #         assert res.status_code == 400
    #         assert (
    #             "greater than or equal to the maximum number of allowed granular permissions"
    #             in res.json["_error"]["message"]
    #         )
    #         gcloud_client.grant_lister_access.assert_not_called()
    #         gcloud_client.grant_download_access.assert_not_called()
    #         gcloud_client.revoke_lister_access.assert_not_called()
    #         gcloud_client.revoke_download_access.assert_not_called()
    #         inserts_fail_eventually = True
    #         break
    # assert inserts_fail_eventually


def test_delete_permission(cidc_api, clean_db, monkeypatch):
    """Check that deleting a permission works as expected."""
    gcloud_client = mock_gcloud_client(monkeypatch)
    current_user_id, other_user_id = setup_permissions(cidc_api, monkeypatch)

    with cidc_api.app_context():
        perm = Permissions.find_for_user(current_user_id)[0]

    client = cidc_api.test_client()

    # Non-admins are not allowed to delete
    gcloud_client.reset_mock()
    res = client.delete(f"permissions/{perm.id}")
    assert res.status_code == 401
    assert "not authorized to access this endpoint" in res.json["_error"]["message"]
    gcloud_client.grant_lister_access.assert_not_called()
    gcloud_client.grant_download_access.assert_not_called()
    gcloud_client.revoke_lister_access.assert_not_called()
    gcloud_client.revoke_download_access.assert_not_called()

    make_admin(current_user_id, cidc_api)

    # Requester must supply an If-Match header
    gcloud_client.reset_mock()
    res = client.delete(f"permissions/{perm.id}")
    assert res.status_code == 428
    gcloud_client.grant_lister_access.assert_not_called()
    gcloud_client.grant_download_access.assert_not_called()
    gcloud_client.revoke_lister_access.assert_not_called()
    gcloud_client.revoke_download_access.assert_not_called()

    headers = {"If-Match": "foobar"}

    # Returns NotFound if no record exists
    gcloud_client.reset_mock()
    res = client.delete(f"permissions/1232123", headers=headers)
    assert res.status_code == 404
    gcloud_client.grant_lister_access.assert_not_called()
    gcloud_client.grant_download_access.assert_not_called()
    gcloud_client.revoke_lister_access.assert_not_called()
    gcloud_client.revoke_download_access.assert_not_called()

    # A mismatched ETag leads to a PreconditionFailed error
    gcloud_client.reset_mock()
    res = client.delete(f"permissions/{perm.id}", headers=headers)
    assert res.status_code == 412
    gcloud_client.grant_lister_access.assert_not_called()
    gcloud_client.grant_download_access.assert_not_called()
    gcloud_client.revoke_lister_access.assert_not_called()
    gcloud_client.revoke_download_access.assert_not_called()

    headers["If-Match"] = perm._etag

    # A well-formed delete request fails if IAM revoke fails
    gcloud_client.reset_mock()
    gcloud_client.revoke_download_access.side_effect = Exception("oops")
    res = client.delete(f"permissions/{perm.id}", headers=headers)
    assert "IAM revoke failed" in res.json["_error"]["message"]
    assert res.status_code == 500
    with cidc_api.app_context():
        assert Permissions.find_by_id(perm.id) is not None
    gcloud_client.revoke_download_access.side_effect = None

    # A matching ETag leads to a successful deletion
    gcloud_client.reset_mock()
    res = client.delete(f"permissions/{perm.id}", headers=headers)
    assert res.status_code == 204
    with cidc_api.app_context():
        assert Permissions.find_by_id(perm.id) is None

    gcloud_client.grant_lister_access.assert_not_called()
    gcloud_client.grant_download_access.assert_not_called()
    gcloud_client.revoke_lister_access.assert_not_called()  # there's a second permission still
    gcloud_client.revoke_download_access.assert_called_once()
