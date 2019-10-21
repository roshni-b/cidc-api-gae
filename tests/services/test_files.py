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

from ..test_models import db_test

FILE = {"object_url": "1"}
URL = "foo"


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
        fake_metadata = {
            "artifact_category": "Assay Artifact from CIMAC",
            "object_url": "",
            "file_name": "",
            "file_size_bytes": 0,
            "md5_hash": "",
            "data_format": "TEXT",
            "uploaded_timestamp": datetime.now(),
        }
        f = DownloadableFiles.create_from_metadata(tid, assay, fake_metadata)
        file_id = f.id
        db.commit()

    # No permission to view file
    res = client.get(f"/downloadable_files/download_url?id={file_id}")
    assert res.status_code == 404

    with app_no_auth.app_context():
        perm = Permissions(trial_id=tid, assay_type=assay, granted_to_user=test_user.id)
        db.add(perm)
        db.commit()

    test_url = "foo"
    monkeypatch.setattr(gcloud_client, "get_signed_url", lambda *args: test_url)

    res = client.get(f"/downloadable_files/download_url?id={file_id}")
    assert res.status_code == 200
    assert res.json == test_url


@db_test
def test_update_file_filters(db, app_no_auth, test_user):
    """Test that update_file_filters updates filter params as expected"""

    # Set up necessary data in the database
    t1 = "test_trial_1"
    t2 = "test_trial_2"
    trial = TrialMetadata.create(trial_id=t1, metadata_json={})
    TrialMetadata.create(trial_id=t2, metadata_json={})
    fake_metadata = {
        "artifact_category": "Assay Artifact from CIMAC",
        "object_url": "",
        "file_name": "",
        "file_size_bytes": 0,
        "md5_hash": "",
        "data_format": "TEXT",
        "uploaded_timestamp": datetime.now(),
    }
    for t in [t1, t2]:
        for a in ["wes", "olink"]:
            d = DownloadableFiles.create_from_metadata(
                trial_id=t,
                assay_type=a,
                file_metadata=dict(
                    fake_metadata, object_url=f"{t}/{a}"  # so they're unique
                ),
            )

    # Make sure we actually inserted files before running tests
    assert len(db.query(DownloadableFiles).all()) == 4

    client = app_no_auth.test_client()

    # Empty filter, no permissions
    res = client.get("/downloadable_files")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 0

    def add_permission(trial_id, assay_type):
        db.add(
            Permissions(
                granted_by_user=test_user.id,
                granted_to_user=test_user.id,
                trial_id=trial_id,
                assay_type=assay_type,
            )
        )
        db.commit()

    # Give the test user permission to view Olink for Trial 1 and WES for Trial 2
    add_permission(t1, "wes")
    add_permission(t2, "olink")

    trial_assay_pair = lambda trial: (trial["trial"], trial["assay_type"])

    # No filter with permissions
    res = client.get("/downloadable_files")
    assert res.status_code == 200
    trials = res.json["_items"]
    for trial in trials:
        assert trial_assay_pair(trial) in [(t1, "wes"), (t2, "olink")]

    # Facet-style filter
    facet_filter = (
        f"(trial=={t1} or trial=={t2}) and (assay_type==wes or assay_type==olink)"
    )
    res = client.get(f"/downloadable_files?where={facet_filter}")
    assert res.status_code == 200
    trials = res.json["_items"]
    for trial in trials:
        assert trial_assay_pair(trial) in [(t1, "wes"), (t2, "olink")]

    # A query on entirely disallowed data should return empty, but no permissions error.
    disallowed_filter = f"trial=={t1} and assay_type==olink"
    res = client.get(f"/downloadable_files?where={disallowed_filter}")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 0

    # Mongo-style JSON filters are not allowed
    json_filter = json.dumps({"trial": t1})
    res = client.get(f"/downloadable_files?where={json_filter}")
    assert res.status_code == 400
    assert "Mongo-style JSON filters are not supported" in res.json["_error"]["message"]

    # Injection attempt
    injection_filter = (
        f"trial=={t1} and assay_type==olink) or (trial=={t1} and assay_type==wes"
    )
    res = client.get(f"/downloadable_files?where={injection_filter}")
    assert res.status_code == 400
    assert "Could not parse filter" in res.json["_error"]["message"]

    # Admins should be able to access data regardless of permissions
    test_user.role = CIDCRole.ADMIN.value
    db.commit()
    disallowed_filter = f"trial=={t1} and assay_type==olink"
    res = client.get(f"/downloadable_files?where={disallowed_filter}")
    assert res.status_code == 200
    assert len(res.json["_items"]) == 1
