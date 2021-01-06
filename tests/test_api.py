"""Smoke tests ranging across the CIDC REST API.

This file doesn't contain tests for methods that don't directly correspond
to data resources, like endpoints that handle upload-related functionality.
"""
from unittest.mock import MagicMock
from datetime import datetime
from dateutil.parser import parse as parse_date


import pytest
from werkzeug.exceptions import BadRequest

from cidc_api.models import (
    Users,
    DownloadableFiles,
    Permissions,
    TrialMetadata,
    UploadJobs,
    UploadJobStatus,
    CIDCRole,
    BaseModel,
)

from .utils import mock_current_user, mock_gcloud_client

TEST_RECORD_ID = 1

# Configuration for resource tests below. For each resource, the following keywords are supported:
#   `json` (required): a JSON instance of this resource.
#   `model` (required): the SQLAlchemy model for this resource.
#   `allowed_methods` (required): the HTTP methods this resource supports.
#   `POST_setup`: a list of other resources to add to the database before POSTing this resource.
#   `PATCH_json` (required if "PATCH" in `allowed_methods`): a JSON patch update for this resource.
#   `lookup_func`: given a config, return the URL suffix for an item lookup, i.e., `<resource>/<suffix>`.
#   `filters`: a dictionary containing two entries representing possible filter queries:
#       `empty`: a query filter that should return empty results.
#       `one`: a query filter that should return exactly one result.
#   `additional_records`: a list of JSON instances of this resource to insert before testing pagination.
#   `mocks`: a list of functions that accept pytest's `monkeypatch` as their argument.
users = {
    "json": {
        "email": "test-admin@example.com",
        "id": TEST_RECORD_ID,
        "role": CIDCRole.ADMIN.value,
        "approval_date": str(datetime.now()),
    },
    "model": Users,
    "allowed_methods": {"POST", "PATCH", "GET"},
    "PATCH_json": {"role": CIDCRole.CIMAC_USER.value},
}
users["additional_records"] = [
    {**users["json"], "id": 2, "email": "foo@bar.com"},
    {**users["json"], "id": 3, "email": "fizz@buzz.com"},
]

trial_metadata = {
    "json": {
        "id": TEST_RECORD_ID,
        "trial_id": "foo",
        "metadata_json": {
            "protocol_identifier": "foo",
            "allowed_collection_event_names": [],
            "allowed_cohort_names": [],
            "participants": [],
        },
    },
    "model": TrialMetadata,
    "allowed_methods": {"POST", "PATCH", "GET"},
    "lookup_func": lambda cfg: cfg["trial_id"],
    "PATCH_json": {
        "metadata_json": {
            "protocol_identifier": "foo",
            "allowed_collection_event_names": ["bar"],
            "allowed_cohort_names": ["buzz"],
            "participants": [],
        }
    },
}

downloadable_files = {
    "json": {
        "id": TEST_RECORD_ID,
        "trial_id": trial_metadata["json"]["trial_id"],
        "file_name": "",
        "upload_type": "rna",
        "data_format": "",
        "object_url": f'{trial_metadata["json"]["trial_id"]}/rna/.../r1_123.fastq.gz',
        "facet_group": "/rna/reads_.bam",
        "uploaded_timestamp": datetime.now(),
        "file_size_bytes": 1,
    },
    "model": DownloadableFiles,
    "allowed_methods": {"GET"},
    "POST_setup": ["trial_metadata"],
    "PATCH_json": {"upload_type": "fizzbuzz"},
    "filters": {
        "empty": {
            "trial_ids": [trial_metadata["json"]["trial_id"]],
            "facets": "Clinical Type|Participants Info",
        },
        "one": {
            "trial_ids": [trial_metadata["json"]["trial_id"]],
            "facets": "Assay Type|RNA|Source",
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
        "granted_by_user": TEST_RECORD_ID,
        "trial_id": trial_metadata["json"]["trial_id"],
        "upload_type": downloadable_files["json"]["upload_type"],
    },
    "model": Permissions,
    "allowed_methods": {"POST", "GET", "DELETE"},
    "POST_setup": ["users", "trial_metadata"],
    "PATCH_json": {"upload_type": "fizzbuzz"},
    "filters": {"empty": {"user_id": 2}, "one": {"user_id": TEST_RECORD_ID}},
}

upload_token = "53b455a5-d25b-428b-8c83-86a3120188da"
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
        "token": upload_token,
    },
    "model": UploadJobs,
    "lookup_func": lambda cfg: f"{cfg['id']}?token={upload_token}",
    "allowed_methods": {"PATCH", "GET"},
    "POST_setup": ["users", "trial_metadata"],
    "PATCH_json": {
        "status": UploadJobStatus.UPLOAD_COMPLETED.value,
        "token": upload_token,
    },
    "mocks": [
        lambda monkeypatch: monkeypatch.setattr(
            "cidc_api.shared.gcloud_client.revoke_upload_access", MagicMock()
        ),
        lambda monkeypatch: monkeypatch.setattr(
            "cidc_api.shared.gcloud_client.publish_upload_success", MagicMock()
        ),
    ],
}

resource_requests = {
    "users": users,
    "trial_metadata": trial_metadata,
    "downloadable_files": downloadable_files,
    "permissions": permissions,
    "upload_jobs": upload_jobs,
}


def mock_admin_user(cidc_api, monkeypatch) -> int:
    user = Users(**{**users["json"], "email": "other@email.com", "id": None})
    mock_current_user(user, monkeypatch)

    with cidc_api.app_context():
        user.insert()
        return user.id


ETAG = "test-etag"


def setup_db_records(cidc_api):
    extra = {"_etag": ETAG}
    with cidc_api.app_context():
        Users(**users["json"], **extra).insert(compute_etag=False)
        TrialMetadata(**trial_metadata["json"], **extra).insert(compute_etag=False)
        DownloadableFiles(**downloadable_files["json"], **extra).insert(
            compute_etag=False
        )
        Permissions(**permissions["json"], **extra).insert(compute_etag=False)
        UploadJobs(**upload_jobs["json"], **extra).insert(compute_etag=False)


def assert_dict_contains(base, target):
    assert isinstance(target, dict) and isinstance(base, (dict, BaseModel))

    def equal_dates(d1, d2):
        if isinstance(d1, str):
            d1 = parse_date(d1)
        if isinstance(d2, str):
            d2 = parse_date(d2)
        return d1 == d2

    for key, value in target.items():
        if hasattr(base, key):
            base_val = getattr(base, key)
        else:
            assert key in base
            base_val = base[key]
        assert base_val == value or equal_dates(base_val, value)


def setup_mocks(config, monkeypatch):
    if "mocks" in config:
        for mock in config["mocks"]:
            mock(monkeypatch)


def get_lookup_value(config):
    lookup_func = config.get("lookup_func")
    return lookup_func(config["json"]) if lookup_func else config["json"]["id"]


def resource_requests_with_key(key):
    return [rc for rc in resource_requests.items() if key in rc[1]]


@pytest.mark.parametrize("resource, config", resource_requests.items())
def test_resource_post(resource, config, cidc_api, clean_db, monkeypatch):
    mock_gcloud_client(monkeypatch)
    mock_admin_user(cidc_api, monkeypatch)
    setup_mocks(config, monkeypatch)
    client = cidc_api.test_client()

    if "POST_setup" in config:
        for setup_resource in config["POST_setup"]:
            res = client.post(
                setup_resource, json=resource_requests[setup_resource]["json"]
            )
            assert res.status_code == 201, "error during POST test setup"

    # Try to create the item with POST
    response = client.post(resource, json=config["json"])
    if "POST" in config["allowed_methods"]:
        assert response.status_code == 201
        # Make sure it was created
        with cidc_api.app_context():
            item = config["model"].find_by_id(response.json["id"]).__dict__
            assert_dict_contains(item, config["json"])
    else:
        assert response.status_code == 405


@pytest.mark.parametrize("resource, config", resource_requests.items())
def test_resource_and_item_get(resource, config, cidc_api, clean_db, monkeypatch):
    mock_gcloud_client(monkeypatch)
    setup_mocks(config, monkeypatch)
    setup_db_records(cidc_api)
    mock_admin_user(cidc_api, monkeypatch)
    client = cidc_api.test_client()

    # resource-level GET
    response = client.get(resource)
    if "GET" in config["allowed_methods"]:
        assert response.status_code == 200
        item = response.json["_items"][0]
        assert_dict_contains(item, config["json"])
        if config.get("pagination"):
            assert response.json["_meta"]["total"] == 3
        elif resource == "users":
            # Since the mocked admin user is also in the DB
            assert response.json["_meta"]["total"] == 2
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
def test_item_patch(resource, config, cidc_api, clean_db, monkeypatch):
    mock_gcloud_client(monkeypatch)
    setup_db_records(cidc_api)
    mock_admin_user(cidc_api, monkeypatch)
    setup_mocks(config, monkeypatch)

    client = cidc_api.test_client()

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
        with cidc_api.app_context():
            item = config["model"].find_by_id(response.json["id"])
            assert_dict_contains(item, config["PATCH_json"])
    else:
        assert response.status_code in (404, 405)


@pytest.mark.parametrize("resource, config", resource_requests.items())
def test_item_put(resource, config, cidc_api, clean_db, monkeypatch):
    mock_gcloud_client(monkeypatch)
    setup_db_records(cidc_api)
    mock_admin_user(cidc_api, monkeypatch)
    setup_mocks(config, monkeypatch)
    client = cidc_api.test_client()

    # Try to PUT the resource - this is disallowed for all resources.
    lookup = get_lookup_value(config)
    response = client.put(f"{resource}/{lookup}", json=config["json"])
    if "PUT" in config["allowed_methods"]:
        assert response.status_code == 200
        assert response.json == config["json"]
    else:
        assert response.status_code in (404, 405)


@pytest.mark.parametrize("resource, config", resource_requests.items())
def test_item_delete(resource, config, cidc_api, clean_db, monkeypatch):
    mock_gcloud_client(monkeypatch)
    setup_db_records(cidc_api)
    mock_admin_user(cidc_api, monkeypatch)
    setup_mocks(config, monkeypatch)
    client = cidc_api.test_client()

    # Try to DELETE the resource - this is disallowed for all resources.
    lookup = get_lookup_value(config)
    response = client.delete(f"{resource}/{lookup}", headers={"if-match": ETAG})
    if "DELETE" in config["allowed_methods"]:
        assert response.status_code == 204
    else:
        assert response.status_code in (404, 405)


@pytest.mark.parametrize("resource, config", resource_requests_with_key("filters"))
def test_resource_filters(resource, config, cidc_api, clean_db, monkeypatch):
    mock_gcloud_client(monkeypatch)
    setup_db_records(cidc_api)
    mock_admin_user(cidc_api, monkeypatch)
    setup_mocks(config, monkeypatch)
    client = cidc_api.test_client()

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
def test_resource_pagination(resource, config, cidc_api, clean_db, monkeypatch):
    mock_gcloud_client(monkeypatch)
    setup_db_records(cidc_api)
    mock_admin_user(cidc_api, monkeypatch)
    setup_mocks(config, monkeypatch)

    # Insert additional records for pagination testing
    with cidc_api.app_context():
        for record in config["additional_records"]:
            config["model"](**record).insert()

    client = cidc_api.test_client()

    # Check that max_results = 1 returns only one result
    response = client.get(resource, query_string={"page_size": 1})
    assert response.status_code == 200
    assert len(response.json["_items"]) == 1

    # Check that changing the sorting seems to work
    response = client.get(
        resource,
        query_string={"page_size": 1, "sort_field": "id", "sort_direction": "desc"},
    )
    assert response.status_code == 200
    assert response.json["_items"][0]["id"] > TEST_RECORD_ID

    # Check that pagination seems to work
    page_1_response = client.get(resource, query_string={"page_size": 2, "page_num": 0})
    assert page_1_response.status_code == 200
    assert len(page_1_response.json["_items"]) == 2
    page_2_response = client.get(resource, query_string={"page_size": 2, "page_num": 1})
    assert page_2_response.status_code == 200
    assert len(page_2_response.json["_items"]) == (2 if resource == "users" else 1)


def test_endpoint_urls(cidc_api):
    """
    Ensure that the API has exactly the endpoints we expect.
    """
    expected_endpoints = {
        "/",
        "/downloadable_files/",
        "/downloadable_files/filelist",
        "/downloadable_files/download_url",
        "/downloadable_files/filter_facets",
        "/downloadable_files/<int:downloadable_file>",
        "/downloadable_files/<int:downloadable_file>/related_files",
        "/info/assays",
        "/info/analyses",
        "/info/manifests",
        "/info/extra_data_types",
        "/info/data_overview",
        "/info/templates/<template_family>/<template_type>",
        "/ingestion/validate",
        "/ingestion/upload_manifest",
        "/ingestion/upload_assay",
        "/ingestion/upload_analysis",
        "/ingestion/extra-assay-metadata",
        "/ingestion/poll_upload_merge_status/<int:upload_job>",
        "/ingestion/intake_gcs_uri",
        "/ingestion/intake_metadata",
        "/permissions/",
        "/permissions/<int:permission>",
        "/trial_metadata/",
        "/trial_metadata/<string:trial>",
        "/upload_jobs/",
        "/upload_jobs/<int:upload_job>",
        "/users/",
        "/users/self",
        "/users/<int:user>",
    }

    # Check that every endpoint included in the API is expected.
    endpoints = set([rule.rule for rule in cidc_api.url_map._rules])
    assert endpoints == expected_endpoints


def test_exception_handler(clean_cidc_api):
    """
    Ensure that the API handles HTTPExceptions in its routes as expected.
    """
    message = "uh oh!"

    @clean_cidc_api.route("/bad_request")
    def raise_bad_request():
        raise BadRequest(message)

    @clean_cidc_api.route("/key_error")
    def raise_key_error():
        raise KeyError(message)

    client = clean_cidc_api.test_client()

    res = client.get("/bad_request")
    assert res.status_code == 400
    assert res.json == {"_status": "ERR", "_error": {"message": message}}

    res = client.get("/key_error")
    assert res.status_code == 500
    assert res.json["_status"] == "ERR"
    assert "internal error" in res.json["_error"]["message"]
