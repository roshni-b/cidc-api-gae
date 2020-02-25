"""Smoke tests ranging across the CIDC REST API.

This file doesn't contain tests for methods that don't directly correspond
to data resources, like those implemented in the services module (e.g., for
handling upload-related functionality).
"""
from unittest.mock import MagicMock
from datetime import datetime

from flask import _request_ctx_stack

import pytest
from cidc_api.models import (
    Users,
    DownloadableFiles,
    Permissions,
    TrialMetadata,
    UploadJobs,
    UploadJobStatus,
    CIDCRole,
)

TEST_RECORD_ID = 1

# Configuration for resource tests below. For each resource, the following keywords are supported:
#   `json` (required): a JSON instance of this resource.
#   `model` (required): the SQLAlchemy model for this resource.
#   `allowed_methods` (required): the HTTP methods this resource supports.
#   `POST_setup`: a list of other resources to add to the database before POSTing this resource.
#   `PATCH_json` (required if "PATCH" in `allowed_methods`): a JSON patch update for this resource.
#   `lookup_field`: the field whose value identifies individual items of a certain resource.
#   `filters`: a dictionary containing two entries representing possible filter queries:
#       `empty`: a query filter that should return empty results.
#       `one`: a query filter that should return exactly one result.
#   `additional_records`: a list of JSON instances of this resource to insert before testing pagination.
#   `mocks`: a list of functions that accept pytest's `monkeypatch` as their argument.
new_users = {
    "json": {"email": "test-admin@example.com"},
    "model": Users,
    "allowed_methods": {"POST"},
}

users = {
    "json": {
        **new_users["json"],
        "id": TEST_RECORD_ID,
        "role": CIDCRole.ADMIN.value,
        "approval_date": datetime.now(),
    },
    "model": Users,
    "allowed_methods": {"POST", "PATCH", "GET"},
    "PATCH_json": {"role": CIDCRole.CIMAC_USER.value},
}
users["additional_records"] = [
    {**users["json"], "id": 2, "email": "foo@bar.com"},
    {**users["json"], "id": 3, "email": "fizz@buzz.com"},
]
users["filters"] = {
    "empty": {
        "where": f"role=='{CIDCRole.CIMAC_USER.value}' and email=='{users['json']['email']}'"
    },
    "one": {
        "where": f"role=='{CIDCRole.CIMAC_USER.value}' or email=='{users['json']['email']}'"
    },
}

trial_metadata = {
    "json": {"id": TEST_RECORD_ID, "trial_id": "foo", "metadata_json": {}},
    "model": TrialMetadata,
    "allowed_methods": {"POST", "PATCH", "GET"},
    "lookup_field": "trial_id",
    "PATCH_json": {"metadata_json": {"foo": "bar"}},
}

downloadable_files = {
    "json": {
        "id": TEST_RECORD_ID,
        "trial_id": trial_metadata["json"]["trial_id"],
        "file_name": "",
        "upload_type": "olink",
        "data_format": "",
        "object_url": "",
        "uploaded_timestamp": datetime.now(),
        "file_size_bytes": 1,
    },
    "model": DownloadableFiles,
    "allowed_methods": {"GET"},
    "POST_setup": ["trial_metadata"],
    "PATCH_json": {"upload_type": "fizzbuzz"},
    "filters": {
        "empty": {
            "where": f"trial_id=='{trial_metadata['json']['trial_id']}' and upload_type=='not_olink'"
        },
        "one": {
            "where": f"trial_id=='{trial_metadata['json']['trial_id']}' and upload_type=='olink' and id==1"
        },
    },
}
downloadable_files["additional_records"] = [
    {**downloadable_files["json"], "id": 2, "object_url": "foo/bar"},
    {**downloadable_files["json"], "id": 3, "object_url": "fizz/buzz"},
]

permissions = {
    "json": {
        "id": TEST_RECORD_ID,
        "granted_to_user": TEST_RECORD_ID,
        "trial_id": trial_metadata["json"]["trial_id"],
        "upload_type": downloadable_files["json"]["upload_type"],
    },
    "model": Permissions,
    "allowed_methods": {"POST", "PATCH", "GET", "DELETE"},
    "POST_setup": ["users", "trial_metadata"],
    "PATCH_json": {"upload_type": "fizzbuzz"},
    "filters": {
        "empty": {"where": "granted_to_user==2"},
        "one": {"where": f"granted_to_user=={TEST_RECORD_ID}"},
    },
}

upload_jobs = {
    "json": {
        "id": TEST_RECORD_ID,
        "trial_id": trial_metadata["json"]["trial_id"],
        "uploader_email": users["json"]["email"],
        "upload_type": downloadable_files["json"]["upload_type"],
        "metadata_patch": {},
        "gcs_xlsx_uri": "",
        "multifile": False,
        "status": UploadJobStatus.STARTED.value,
    },
    "model": UploadJobs,
    "allowed_methods": {"PATCH", "GET"},
    "POST_setup": ["users", "trial_metadata"],
    "PATCH_json": {"upload_type": "fizzbuzz"},
    "mocks": [
        lambda monkeypatch: monkeypatch.setattr(
            "cidc_api.services.ingestion.gcloud_client.revoke_upload_access",
            MagicMock(),
        )
    ],
}

resource_requests = dict(
    new_users=new_users,
    users=users,
    trial_metadata=trial_metadata,
    downloadable_files=downloadable_files,
    permissions=permissions,
    upload_jobs=upload_jobs,
)


@pytest.fixture
def app_with_admin_user(app, monkeypatch):
    def fake_auth(*args):
        _request_ctx_stack.top.current_user = Users(**users["json"])
        return True

    monkeypatch.setattr(app.auth, "authorized", fake_auth)

    return app


ETAG = "test-etag"


@pytest.fixture
def db_with_records(db):
    extra = {"_etag": ETAG}
    db.add(Users(**users["json"], **extra))
    db.add(TrialMetadata(**trial_metadata["json"], **extra))
    db.commit()

    db.add(DownloadableFiles(**downloadable_files["json"], **extra))
    db.add(Permissions(**permissions["json"], **extra))
    db.add(UploadJobs(**upload_jobs["json"], **extra))
    db.commit()

    return db


def assert_dict_contains(base, target):
    assert isinstance(target, dict) and isinstance(base, dict)
    for key, value in target.items():
        assert key in base
        assert base[key] == value or isinstance(value, datetime)


def setup_mocks(config, monkeypatch):
    if "mocks" in config:
        for mock in config["mocks"]:
            mock(monkeypatch)


def get_lookup_value(config):
    return config["json"].get(config.get("lookup_field") or "id")


def resource_requests_with_key(key):
    return [rc for rc in resource_requests.items() if key in rc[1]]


@pytest.mark.parametrize("resource, config", resource_requests.items())
def test_resource_post(resource, config, app_with_admin_user, db, monkeypatch):
    setup_mocks(config, monkeypatch)
    client = app_with_admin_user.test_client()

    if "POST_setup" in config:
        for setup_resource in config["POST_setup"]:
            client.post(setup_resource, json=resource_requests[setup_resource]["json"])

    # Try to create the item with POST
    response = client.post(resource, json=config["json"])
    if "POST" in config["allowed_methods"]:
        assert response.status_code == 201
        # Make sure it was created
        item = db.query(config["model"]).one().__dict__
        assert_dict_contains(item, config["json"])
    else:
        assert response.status_code == 405


@pytest.mark.parametrize("resource, config", resource_requests.items())
def test_resource_and_item_get(
    resource, config, app_with_admin_user, db_with_records, monkeypatch
):
    setup_mocks(config, monkeypatch)
    client = app_with_admin_user.test_client()

    # resource-level GET
    response = client.get(resource)
    if "GET" in config["allowed_methods"]:
        assert response.status_code == 200
        item = response.json["_items"][0]
        assert_dict_contains(item, config["json"])
        if config.get("pagination"):
            assert response.json["_meta"]["total"] == 3
        else:
            assert response.json["_meta"]["total"] == 1
    else:
        assert response.status_code == 405

    # item-level GET
    lookup = get_lookup_value(config)
    response = client.get(f"{resource}/{lookup}")
    if "GET" in config["allowed_methods"]:
        assert response.status_code == 200
        assert_dict_contains(response.json, config["json"])
    else:
        assert response.status_code in (404, 405)


@pytest.mark.parametrize("resource, config", resource_requests.items())
def test_item_patch(
    resource, config, app_with_admin_user, db_with_records, monkeypatch
):
    client = app_with_admin_user.test_client()
    setup_mocks(config, monkeypatch)

    # Try to update the resource
    lookup = get_lookup_value(config)
    response = client.patch(f"{resource}/{lookup}", json=config.get("PATCH_json"))
    if "PATCH" in config["allowed_methods"]:
        # Need to match etag
        assert response.status_code == 428
        response = client.patch(
            f"{resource}/{lookup}",
            json=config.get("PATCH_json"),
            headers={"if-match": ETAG},
        )
        assert response.status_code == 200
        # Check that the record was updated
        item = db_with_records.query(config["model"]).one().__dict__
        assert_dict_contains(item, config["PATCH_json"])
    else:
        assert response.status_code in (404, 405)


@pytest.mark.parametrize("resource, config", resource_requests.items())
def test_item_put(resource, config, app_with_admin_user, db_with_records, monkeypatch):
    setup_mocks(config, monkeypatch)
    client = app_with_admin_user.test_client()

    # Try to PUT the resource - this is disallowed for all resources.
    lookup = get_lookup_value(config)
    response = client.put(f"{resource}/{lookup}", json=config["json"])
    if "PUT" in config["allowed_methods"]:
        assert response.status_code == 200
        assert response.json == config["json"]
    else:
        assert response.status_code in (404, 405)


@pytest.mark.parametrize("resource, config", resource_requests.items())
def test_item_delete(
    resource, config, app_with_admin_user, db_with_records, monkeypatch
):
    setup_mocks(config, monkeypatch)
    client = app_with_admin_user.test_client()

    # Try to DELETE the resource - this is disallowed for all resources.
    lookup = get_lookup_value(config)
    response = client.delete(f"{resource}/{lookup}", headers={"if-match": ETAG})
    if "DELETE" in config["allowed_methods"]:
        assert response.status_code == 204
    else:
        assert response.status_code in (404, 405)


@pytest.mark.parametrize("resource, config", resource_requests_with_key("filters"))
def test_resource_filters(
    resource, config, app_with_admin_user, db_with_records, monkeypatch
):
    if "filters" not in config:
        return

    setup_mocks(config, monkeypatch)
    client = app_with_admin_user.test_client()

    one_response = client.get(resource, query_string=config["filters"]["one"])
    assert one_response.status_code == 200
    assert len(one_response.json["_items"]) == 1
    item = one_response.json["_items"][0]
    assert_dict_contains(item, config["json"])

    empty_response = client.get(resource, query_string=config["filters"]["empty"])
    assert empty_response.status_code == 200
    assert empty_response.json["_items"] == []


@pytest.mark.parametrize(
    "resource, config", resource_requests_with_key("additional_records")
)
def test_resource_pagination(
    resource, config, app_with_admin_user, db_with_records, monkeypatch
):
    # Insert additional records for pagination testing
    for record in config["additional_records"]:
        db_with_records.add(config["model"](**record))
    db_with_records.commit()

    setup_mocks(config, monkeypatch)
    client = app_with_admin_user.test_client()

    # Check that max_results = 1 returns only one result
    response = client.get(resource, query_string={"max_results": 1})
    assert response.status_code == 200
    assert len(response.json["_items"]) == 1
    assert response.json["_items"][0]["id"] == TEST_RECORD_ID

    # Check that changing the sorting seems to work
    response = client.get(
        resource, query_string={"max_results": 1, "sort": "[('id', -1)]"}
    )
    assert response.status_code == 200
    assert response.json["_items"][0]["id"] == 3

    # Check that pagination seems to work
    page_1_response = client.get(resource, query_string={"max_results": 2, "page": 1})
    assert page_1_response.status_code == 200
    assert len(page_1_response.json["_items"]) == 2
    page_2_response = client.get(resource, query_string={"max_results": 2, "page": 2})
    assert page_2_response.status_code == 200
    assert len(page_2_response.json["_items"]) == 1


def test_endpoint_urls(app):
    """
    Ensure that the API has exactly the endpoints we expect.
    """
    expected_endpoints = {
        "/",
        "/downloadable_files",
        "/downloadable_files/download_url",
        "/downloadable_files/filter_facets",
        '/downloadable_files/<regex("[0-9]+"):id>',
        "/info/assays",
        "/info/analyses",
        "/info/manifests",
        "/info/extra_data_types",
        "/info/templates/<template_family>/<template_type>",
        "/ingestion/validate",
        "/ingestion/upload_manifest",
        "/ingestion/upload_assay",
        "/ingestion/upload_analysis",
        "/ingestion/extra-assay-metadata",
        "/ingestion/poll_upload_merge_status",
        "/permissions",
        '/permissions/<regex("[0-9]+"):id>',
        "/trial_metadata",
        '/trial_metadata/<regex("[a-zA-Z0-9_-]+"):trial_id>',
        "/upload_jobs",
        '/upload_jobs/<regex("[0-9]+"):id>',
        "/users",
        "/users/self",
        '/users/<regex("[0-9]+"):id>',
        "/new_users",
    }

    # Check that every endpoint included in the API is expected.
    endpoints = set([rule.rule for rule in app.url_map._rules])
    assert endpoints == expected_endpoints
