from datetime import datetime
from typing import Tuple

from cidc_schemas.prism import PROTOCOL_ID_FIELD_NAME
from cidc_api.models import (
    Users,
    DownloadableFiles,
    DownloadableFileSchema,
    TrialMetadata,
    Permissions,
    CIDCRole,
)

from ..utils import mock_current_user, make_admin


def setup_user(cidc_api, monkeypatch) -> int:
    current_user = Users(
        email="test@email.com",
        role=CIDCRole.CIMAC_USER.value,
        approval_date=datetime.now(),
    )
    mock_current_user(current_user, monkeypatch)

    with cidc_api.app_context():
        current_user.insert()
        return current_user.id


trial_id = "test-trial"
upload_types = ["olink", "cytof"]


def setup_downloadable_files(cidc_api) -> Tuple[int, int]:
    """Insert two downloadable files into the database."""
    trial_id = "test-trial"
    metadata_json = {PROTOCOL_ID_FIELD_NAME: trial_id, "participants": []}
    trial = TrialMetadata(trial_id=trial_id, metadata_json=metadata_json)

    def make_file(object_url, upload_type, analysis_friendly) -> DownloadableFiles:
        return DownloadableFiles(
            trial_id=trial_id,
            upload_type=upload_type,
            object_url=object_url,
            data_format="",
            uploaded_timestamp=datetime.now(),
            file_size_bytes=0,
            file_name="",
            analysis_friendly=analysis_friendly,
        )

    file1, file2 = [make_file(i, t, i == 1) for i, t in enumerate(upload_types)]

    with cidc_api.app_context():
        trial.insert()
        file1.insert()
        file2.insert()

        return file1.id, file2.id


def test_list_downloadable_files(cidc_api, clean_db, monkeypatch):
    """Check that getting a list of files works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    file_id_1, file_id_2 = setup_downloadable_files(cidc_api)

    client = cidc_api.test_client()

    # Non-admins can't get files they don't have permissions for
    res = client.get("/downloadable_files")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 0
    assert res.json["_meta"]["total"] == 0

    # Give the user one permission
    with cidc_api.app_context():
        perm = Permissions(
            granted_to_user=user_id, trial_id=trial_id, upload_type=upload_types[0]
        )
        perm.insert()

    # Non-admins can view files for which they have permission
    res = client.get("/downloadable_files")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 1
    assert res.json["_meta"]["total"] == 1
    assert res.json["_items"][0]["id"] == file_id_1

    # Non-admin filter queries exclude files they aren't allowed to view
    res = client.get(
        f"/downloadable_files?upload_types={upload_types[1]}&analysis_friendly=true"
    )
    assert res.status_code == 200
    assert len(res.json["_items"]) == 0
    assert res.json["_meta"]["total"] == 0

    # Admins can view all files regardless of permissions
    make_admin(user_id, cidc_api)
    res = client.get("/downloadable_files")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 2
    assert res.json["_meta"]["total"] == 2
    assert set([f["id"] for f in res.json["_items"]]) == set([file_id_1, file_id_2])

    # Admin filter queries include any files that fit the criteria
    res = client.get(
        f"/downloadable_files?upload_types={upload_types[1]}&analysis_friendly=false"
    )
    assert res.status_code == 200
    assert len(res.json["_items"]) == 1
    assert res.json["_meta"]["total"] == 1
    assert res.json["_items"][0]["id"] == file_id_2


def test_get_downloadable_file(cidc_api, clean_db, monkeypatch):
    """Check that getting a single file works as expected."""
    user_id = setup_user(cidc_api, monkeypatch)
    file_id_1, file_id_2 = setup_downloadable_files(cidc_api)

    client = cidc_api.test_client()

    # Non-admins get 404s for single files they don't have permision to view
    res = client.get(f"/downloadable_files/{file_id_1}")
    assert res.status_code == 404

    # Give the user one permission
    with cidc_api.app_context():
        perm = Permissions(
            granted_to_user=user_id, trial_id=trial_id, upload_type=upload_types[0]
        )
        perm.insert()

    # Non-admins can get single files that they have permision to view
    res = client.get(f"/downloadable_files/{file_id_1}")
    assert res.status_code == 200
    assert res.json["id"] == file_id_1

    # Admins can get any file regardless of permissions
    make_admin(user_id, cidc_api)
    res = client.get(f"/downloadable_files/{file_id_2}")
    assert res.status_code == 200
    assert res.json["id"] == file_id_2

    # Non-existent files yield 404
    res = client.get(f"/downloadable_files/123212321")
    assert res.status_code == 404


def test_get_filter_facets(cidc_api, clean_db, monkeypatch):
    """Check that getting filter facets works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    setup_downloadable_files(cidc_api)

    client = cidc_api.test_client()

    # A user with no permissions can't view any facets
    res = client.get("/downloadable_files/filter_facets")
    assert res.status_code == 200
    assert res.json["trial_id"] == []
    assert res.json["upload_type"] == []

    # A user with one permission can only view facets related to that permission
    with cidc_api.app_context():
        perm = Permissions(
            granted_to_user=user_id, trial_id=trial_id, upload_type=upload_types[0]
        )
        perm.insert()
    res = client.get("/downloadable_files/filter_facets")
    assert res.status_code == 200
    assert res.json["trial_id"] == [trial_id]
    assert res.json["upload_type"] == [upload_types[0]]

    # An admin can view all available facets
    make_admin(user_id, cidc_api)
    data = client.get("/downloadable_files/filter_facets").json
    assert data["trial_id"] == [trial_id]
    assert set(data["upload_type"]) == set(upload_types)
    assert len(data["upload_type"]) == len(upload_types)


def test_get_download_url(cidc_api, clean_db, monkeypatch):
    """Check that generating a GCS signed URL works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    file_id, _ = setup_downloadable_files(cidc_api)

    client = cidc_api.test_client()

    # A query missing the required parameters should yield 422
    res = client.get("/downloadable_files/download_url")
    assert res.status_code == 422
    assert res.json["_error"]["message"]["query"]["id"] == [
        "Missing data for required field."
    ]

    # A missing file should yield 404
    res = client.get("/downloadable_files/download_url?id=123212321")
    assert res.status_code == 404

    # No permission should also yield 404
    res = client.get(f"/downloadable_files/download_url?id={file_id}")
    assert res.status_code == 404

    with cidc_api.app_context():
        perm = Permissions(
            granted_to_user=user_id, trial_id=trial_id, upload_type=upload_types[0]
        )
        perm.insert()

    test_url = "foo"
    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client.get_signed_url", lambda *args: test_url
    )

    res = client.get(f"/downloadable_files/download_url?id={file_id}")
    assert res.status_code == 200
    assert res.json == test_url
