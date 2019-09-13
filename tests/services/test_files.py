import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from flask import _request_ctx_stack
from werkzeug.datastructures import ImmutableMultiDict

from cidc_api.models import Users, TrialMetadata, Permissions, DownloadableFiles
from cidc_api.services.files import (
    update_file_filters,
    insert_download_url,
    insert_download_urls,
)

from ..test_models import db_test

FILE = {"object_url": "1"}
URL = "foo"


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
                trial_id=t, assay_type=a, file_metadata=fake_metadata
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


def test_insert_download_url(monkeypatch):
    """
    Test that we try to generate a signed download URL for the file in the payload.
    """
    get_signed_url = lambda url: URL
    monkeypatch.setattr("gcloud_client.get_signed_url", get_signed_url)

    f = FILE.copy()

    insert_download_url(f)
    assert f["download_link"] == URL


def test_insert_download_urls(monkeypatch):
    """
    Test that we try to generate a signed download URL for every file in the payload.
    """

    def insert_signed_url(f):
        f["download_link"] = URL

    monkeypatch.setattr(
        "cidc_api.services.files.insert_download_url", insert_signed_url
    )

    f1 = FILE.copy()
    f2 = FILE.copy()
    f2["object_url"] = "2"
    payload = {"_items": [f1, f2]}

    insert_download_urls(payload)
    for f in payload["_items"]:
        assert f["download_link"] == URL
