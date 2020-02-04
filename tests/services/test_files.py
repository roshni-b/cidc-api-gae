import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from flask import _request_ctx_stack
from werkzeug.datastructures import ImmutableMultiDict

from cidc_api.models import (
    Users,
    TrialMetadata,
    Permissions,
    DownloadableFiles,
    CIDCRole,
)
from cidc_api.services.files import update_file_filters, gcloud_client

FILE = {"object_url": "1"}
URL = "foo"
fake_metadata = {
    "artifact_category": "Assay Artifact from CIMAC",
    "object_url": "",
    "file_name": "",
    "file_size_bytes": 0,
    "md5_hash": "",
    "data_format": "TEXT",
    "uploaded_timestamp": datetime.now(),
}


def test_get_download_url(db, app_no_auth, test_user, monkeypatch):
    """Test the /downloadable_files/download_url endpoint"""
    tid = "test_trial"
    assay = "wes"
    with app_no_auth.app_context():
        trial = TrialMetadata.create(trial_id=tid, metadata_json={})
        user_record = Users.find_by_email(test_user.email)
        test_user.id = user_record.id
        user_record.approval_date = datetime.now()
        user_record.role = CIDCRole.CIMAC_USER.value
        db.commit()

    client = app_no_auth.test_client()

    # Malformed query
    res = client.get("/downloadable_files/download_url")
    assert res.status_code == 400
    assert "expected URL parameter" in res.json["_error"]["message"]

    # Missing file
    res = client.get("/downloadable_files/download_url?id=123")
    assert res.status_code == 404

    with app_no_auth.app_context():
        f = DownloadableFiles.create_from_metadata(tid, assay, fake_metadata)
        file_id = f.id
        db.commit()

    # No permission to view file
    res = client.get(f"/downloadable_files/download_url?id={file_id}")
    assert res.status_code == 404

    with app_no_auth.app_context():
        perm = Permissions(
            trial_id=tid, upload_type=assay, granted_to_user=test_user.id
        )
        db.add(perm)
        db.commit()

    test_url = "foo"
    monkeypatch.setattr(gcloud_client, "get_signed_url", lambda *args: test_url)

    res = client.get(f"/downloadable_files/download_url?id={file_id}")
    assert res.status_code == 200
    assert res.json == test_url


def test_get_filter_facets(db, app_no_auth, test_user, monkeypatch):
    trials = [f"trial_{i}" for i in range(3)]
    upload_types = [f"type_{i}" for i in range(3)]
    with app_no_auth.app_context():
        test_user.id = Users.find_by_email(test_user.email).id
        test_user.role = CIDCRole.CIMAC_USER.value
        for i, trial_id in enumerate(trials):
            TrialMetadata.create(trial_id=trial_id, metadata_json={})
            for upload_type in upload_types:
                DownloadableFiles.create_from_metadata(
                    trial_id=trial_id,
                    upload_type=upload_type,
                    file_metadata={
                        **fake_metadata,
                        "object_url": f"{trial_id}/{upload_type}",
                    },
                )
                # Only create permissions for trial_1
                if i == 1:
                    p = Permissions(
                        trial_id=trial_id,
                        upload_type=upload_type,
                        granted_to_user=test_user.id,
                    )
                    db.add(p)
        db.commit()

    client = app_no_auth.test_client()

    data = client.get("/downloadable_files/filter_facets").json
    assert data["trial_id"] == ["trial_1"]
    assert set(data["upload_type"]) == set(upload_types)
    assert len(data["upload_type"]) == len(upload_types)

    # Make user an admin
    with app_no_auth.app_context():
        test_user.role = CIDCRole.ADMIN.value
        db.commit()

    data = client.get("/downloadable_files/filter_facets").json
    assert set(data["trial_id"]) == set(trials)
    assert len(data["trial_id"]) == len(trials)
    assert set(data["upload_type"]) == set(upload_types)
    assert len(data["upload_type"]) == len(upload_types)


def test_update_file_filters(db, app_no_auth, test_user):
    """Test that update_file_filters updates filter params as expected"""

    # Set up necessary data in the database
    t1 = "test_trial_1"
    t2 = "test_trial_2"
    with app_no_auth.app_context():
        trial = TrialMetadata.create(trial_id=t1, metadata_json={})
        TrialMetadata.create(trial_id=t2, metadata_json={})
        for t in [t1, t2]:
            for a in ["wes", "olink"]:
                d = DownloadableFiles.create_from_metadata(
                    trial_id=t,
                    upload_type=a,
                    file_metadata=dict(
                        fake_metadata, object_url=f"{t}/{a}"  # so they're unique
                    ),
                )
        test_user.id = Users.find_by_email(test_user.email).id

    # Make sure we actually inserted files before running tests
    assert len(db.query(DownloadableFiles).all()) == 4

    client = app_no_auth.test_client()

    # Empty filter, no permissions
    res = client.get("/downloadable_files")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 0

    def add_permission(trial_id, upload_type):
        db.add(
            Permissions(
                granted_by_user=test_user.id,
                granted_to_user=test_user.id,
                trial_id=trial_id,
                upload_type=upload_type,
            )
        )
        db.commit()

    # Give the test user permission to view Olink for Trial 1 and WES for Trial 2
    add_permission(t1, "wes")
    add_permission(t2, "olink")

    trial_assay_pair = lambda trial: (trial["trial_id"], trial["upload_type"])

    # No filter with permissions
    res = client.get("/downloadable_files")
    assert res.status_code == 200
    trials = res.json["_items"]
    for trial in trials:
        assert trial_assay_pair(trial) in [(t1, "wes"), (t2, "olink")]

    # Facet-style filter
    facet_filter = f"(trial_id=={t1} or trial_id=={t2}) and (upload_type==wes or upload_type==olink)"
    res = client.get(f"/downloadable_files?where={facet_filter}")
    assert res.status_code == 200
    trials = res.json["_items"]
    for trial in trials:
        assert trial_assay_pair(trial) in [(t1, "wes"), (t2, "olink")]

    # A query on entirely disallowed data should return empty, but no permissions error.
    disallowed_filter = f"trial_id=={t1} and upload_type==olink"
    res = client.get(f"/downloadable_files?where={disallowed_filter}")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 0

    # Mongo-style JSON filters are not allowed
    json_filter = json.dumps({"trial_id": t1})
    res = client.get(f"/downloadable_files?where={json_filter}")
    assert res.status_code == 400
    assert "Mongo-style JSON filters are not supported" in res.json["_error"]["message"]

    # Injection attempt
    injection_filter = f"trial_id=={t1} and upload_type==olink) or (trial_id=={t1} and upload_type==wes"
    res = client.get(f"/downloadable_files?where={injection_filter}")
    assert res.status_code == 400
    assert "Could not parse filter" in res.json["_error"]["message"]

    # Admins should be able to access data regardless of permissions
    test_user.role = CIDCRole.ADMIN.value
    db.commit()
    disallowed_filter = f"trial_id=={t1} and upload_type==olink"
    res = client.get(f"/downloadable_files?where={disallowed_filter}")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 1
