import io
from unittest.mock import MagicMock

import pytest
from werkzeug.exceptions import (
    HTTPException,
    InternalServerError,
    BadRequest,
    NotImplemented,
)

from settings import GOOGLE_UPLOAD_BUCKET
from services.ingestion import extract_schema_and_xlsx

from . import open_data_file
from ..util import assert_same_elements


@pytest.fixture
def valid_xlsx():
    with open_data_file("pbmc_valid.xlsx") as xlsx:
        yield xlsx


@pytest.fixture
def invalid_xlsx():
    yield open_data_file("pbmc_invalid.xlsx")


@pytest.fixture
def wes_xlsx():
    yield open_data_file("wes_data.xlsx")


def form_data(filename=None, fp=None, schema=None):
    """
    If no filename is provided, return some text form data.
    If a filename is provided but no opened file (`fp`) is provided,
    return form data with a mock file included.
    If a filename and an opened file is provided, return
    form data with the provided file included.
    """
    data = {"foo": "bar"}
    if schema:
        data["schema"] = schema
    if filename:
        fp = fp or io.BytesIO(b"blah blah")
        data["template"] = (fp, filename)
    return data


VALIDATE = "/ingestion/validate"
UPLOAD = "/ingestion/upload"


def test_validate_valid_template(app_no_auth, valid_xlsx):
    """Ensure that the validation endpoint returns no errors for a known-valid .xlsx file"""
    client = app_no_auth.test_client()
    data = form_data("pbmc.xlsx", valid_xlsx, "templates/pbmc_template.json")
    res = client.post(VALIDATE, data=data)
    assert res.status_code == 200
    assert res.json["errors"] == []


def test_validate_invalid_template(app_no_auth, invalid_xlsx):
    """Ensure that the validation endpoint returns errors for a known-invalid .xlsx file"""
    client = app_no_auth.test_client()
    data = form_data("pbmc.xlsx", invalid_xlsx, "templates/pbmc_template.json")
    res = client.post(VALIDATE, data=data)
    assert res.status_code == 200
    assert len(res.json["errors"]) > 0


@pytest.mark.parametrize(
    "url,data,error,message",
    [
        # Missing form content
        [VALIDATE, None, BadRequest, "form content"],
        # Form missing template file
        [VALIDATE, form_data(), BadRequest, "template file"],
        # Template file is non-.xlsx
        [VALIDATE, form_data("text.txt"), BadRequest, ".xlsx file"],
        # URL is missing "schema" query param
        [VALIDATE, form_data("text.xlsx"), BadRequest, "form entry for 'schema'"],
        # "schema" query param references non-existent schema
        [
            VALIDATE,
            form_data("test.xlsx", schema="foo/bar"),
            BadRequest,
            "schema with id foo/bar",
        ],
    ],
)
def test_extract_schema_and_xlsx_failures(app, url, data, error, message):
    """
    Test that we get the expected errors when trying to extract 
    schema/template from a malformed request.
    """
    with app.test_request_context(url, data=data):
        with pytest.raises(error, match=message):
            extract_schema_and_xlsx()


def test_upload(app_no_auth, wes_xlsx, test_user, monkeypatch):
    """Ensure the upload endpoint follows the expected execution flow"""
    client = app_no_auth.test_client()

    grant_write = MagicMock()
    monkeypatch.setattr("gcs_iam.grant_upload_access", grant_write)

    res = client.post(UPLOAD, data=form_data("wes.xlsx", wes_xlsx, "wes"))
    assert res.json
    assert "url_mapping" in res.json

    url_mapping = res.json["url_mapping"]

    # We expect local_path to map to a gcs object name with gcs_prefix
    # based on the contents of wes_xlsx.
    local_path = "read_group_map.txt"
    gcs_prefix = "10021/Patient_1/sample_1/aliquot_2/wes_read_group.txt/"
    gcs_object_name = url_mapping[local_path]
    assert local_path in url_mapping
    assert gcs_object_name.startswith(gcs_prefix)

    # Check that we tried to grant IAM upload access to gcs_object_name
    iam_args = (GOOGLE_UPLOAD_BUCKET, test_user.email)
    grant_write.assert_called_with(*iam_args)

    # Check that we tried to revoke IAM upload access after updating the
    revoke_write = MagicMock()
    monkeypatch.setattr("gcs_iam.revoke_upload_access", revoke_write)

    job_id = res.json["job_id"]
    update_url = f"/upload_jobs/{job_id}"
    res = client.patch(
        update_url,
        json={"status": "completed"},
        headers={"If-Match": res.json["job_etag"]},
    )
    assert res.status_code == 200
    revoke_write.assert_called_with(*iam_args)


def test_signed_upload_urls(app_no_auth, monkeypatch):
    """
    Ensure the signed upload urls endpoint responds with the expected structure
    
    TODO: an integration test that actually calls out to GCS
    """
    client = app_no_auth.test_client()
    data = {
        "directory_name": "my-assay-run-id",
        "object_names": ["my-fastq-1.fastq.gz", "my-fastq-2.fastq.gz"],
    }

    monkeypatch.setattr("google.cloud.storage.Client", MagicMock)
    res = client.post("/ingestion/signed-upload-urls", json=data)

    assert_same_elements(res.json.keys(), data["object_names"])
