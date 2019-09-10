import io
from unittest.mock import MagicMock

import pytest
from werkzeug.exceptions import (
    HTTPException,
    InternalServerError,
    BadRequest,
    NotImplemented,
)

from cidc_api.config.settings import GOOGLE_UPLOAD_BUCKET
from cidc_api.services.ingestion import extract_schema_and_xlsx
from cidc_api.models import TrialMetadata, Users, TRIAL_ID_FIELD

from . import open_data_file
from ..test_models import db_test
from ..util import assert_same_elements
from ..conftest import TEST_EMAIL


@pytest.fixture
def pbmc_valid_xlsx():
    with open_data_file("pbmc_valid.xlsx") as xlsx:
        yield xlsx


@pytest.fixture
def pbmc_non_existing_trial():
    with open_data_file("pbmc_non_existing_trial.xlsx") as xlsx:
        yield xlsx


@pytest.fixture
def pbmc_invalid_xlsx():
    yield open_data_file("pbmc_invalid.xlsx")


@pytest.fixture
def wes_xlsx():
    yield open_data_file("wes_data.xlsx")


@pytest.fixture
def olink_xlsx():
    yield open_data_file("olink_data.xlsx")


@pytest.fixture
@db_test
def db_with_trial_and_user(db, test_user):
    # Create the target trial and the uploader
    TrialMetadata.create(
        "test_trial", {TRIAL_ID_FIELD: "test_trial", "participants": []}
    )
    Users.create(profile={"email": test_user.email})


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
ASSAY_UPLOAD = "/ingestion/upload_assay"
MANIFEST_UPLOAD = "/ingestion/upload_manifest"


def test_validate_valid_template(app_no_auth, pbmc_valid_xlsx):
    """Ensure that the validation endpoint returns no errors for a known-valid .xlsx file"""
    client = app_no_auth.test_client()
    data = form_data("pbmc.xlsx", pbmc_valid_xlsx, "pbmc")
    res = client.post(VALIDATE, data=data)
    assert res.status_code == 200
    assert res.json["errors"] == []


def test_validate_invalid_template(app_no_auth, pbmc_invalid_xlsx):
    """Ensure that the validation endpoint returns errors for a known-invalid .xlsx file"""
    client = app_no_auth.test_client()
    data = form_data("pbmc.xlsx", pbmc_invalid_xlsx, "pbmc")
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
            "Unknown template type foo/bar",
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


def test_upload_manifest_non_existing_trial_id(
    app_no_auth, pbmc_non_existing_trial, test_user, db_with_trial_and_user, monkeypatch
):
    """Ensure the upload_manifest endpoint follows the expected execution flow"""

    mocks = UploadMocks(monkeypatch)

    client = app_no_auth.test_client()

    res = client.post(
        MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", pbmc_non_existing_trial, "pbmc")
    )
    assert res.status_code == 400
    assert "test-non-existing-trial-id" in res.json["_error"]["message"]

    # Check that we tried to upload the excel file
    mocks.upload_xlsx.assert_called_once()


def test_upload_manifest(
    app_no_auth, pbmc_valid_xlsx, test_user, db_with_trial_and_user, monkeypatch
):
    """Ensure the upload_manifest endpoint follows the expected execution flow"""

    mocks = UploadMocks(monkeypatch)

    client = app_no_auth.test_client()

    res = client.post(
        MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", pbmc_valid_xlsx, "pbmc")
    )
    assert res.status_code == 200

    # Check that we tried to upload the excel file
    mocks.upload_xlsx.assert_called_once()


class UploadMocks:
    def __init__(self, monkeypatch):
        self.grant_write = MagicMock()
        monkeypatch.setattr("gcloud_client.grant_upload_access", self.grant_write)

        self.upload_xlsx = MagicMock()
        self.upload_xlsx.return_value = MagicMock()
        self.upload_xlsx.return_value.name = "trial_id/xlsx/assays/wes/12345"
        self.upload_xlsx.return_value.size = 100
        self.upload_xlsx.return_value.md5_hash = "md5_hash"

        monkeypatch.setattr("gcloud_client.upload_xlsx_to_gcs", self.upload_xlsx)

        self.revoke_write = MagicMock()
        monkeypatch.setattr("gcloud_client.revoke_upload_access", self.revoke_write)

        self.publish_success = MagicMock()
        monkeypatch.setattr(
            "gcloud_client.publish_upload_success", self.publish_success
        )


def test_upload_wes(
    app_no_auth, wes_xlsx, test_user, db_with_trial_and_user, monkeypatch
):
    """Ensure the upload endpoint follows the expected execution flow"""
    client = app_no_auth.test_client()

    mocks = UploadMocks(monkeypatch)

    res = client.post(ASSAY_UPLOAD, data=form_data("wes.xlsx", wes_xlsx, "wes"))
    assert res.json
    assert "url_mapping" in res.json

    url_mapping = res.json["url_mapping"]

    # We expect local_path to map to a gcs object name with gcs_prefix
    # based on the contents of wes_xlsx.
    local_path = "/local/path/to/rgm.1.1.1.txt"
    gcs_prefix = "test_trial/wes example PA 1/wes example SA 1.1/wes example aliquot 1.1.1/wes/rgm.txt"
    gcs_object_name = url_mapping[local_path]
    assert local_path in url_mapping
    assert gcs_object_name.startswith(gcs_prefix)
    assert not gcs_object_name.endswith(
        local_path
    ), "PHI from local_path shouldn't end up in gcs urls"

    # Check that we tried to grant IAM upload access to gcs_object_name
    mocks.grant_write.assert_called_with(GOOGLE_UPLOAD_BUCKET, test_user.email)

    # Check that we tried to upload the assay metadata excel file
    mocks.upload_xlsx.assert_called_once()

    job_id = res.json["job_id"]
    update_url = f"/assay_uploads/{job_id}"

    # Report an upload failure
    res = client.patch(
        update_url,
        json={"status": "errored"},
        headers={"If-Match": res.json["job_etag"]},
    )
    assert res.status_code == 200
    mocks.revoke_write.assert_called_with(GOOGLE_UPLOAD_BUCKET, test_user.email)
    # This was an upload failure, so success shouldn't have been published
    mocks.publish_success.assert_not_called()

    # Report an upload success
    res = client.patch(
        update_url,
        json={"status": "completed"},
        headers={"If-Match": res.json["_etag"]},
    )
    mocks.publish_success.assert_called_with(job_id)


OLINK_TESTDATA = [
    ("/local/path/combined.xlsx", "test_trial/olink/study_npx.xlsx"),
    (
        "assay1_npx.xlsx",
        "test_trial/olink/chip_111/assay_npx.xlsx",
    ),  # 111 is a chip barcode in .xlsx
    (
        "ct2.xlsx",
        "test_trial/olink/chip_112/assay_raw_ct.xlsx",
    ),  # 112 is a chip barcode in .xlsx
]


def test_upload_olink(
    app_no_auth, olink_xlsx, test_user, db_with_trial_and_user, monkeypatch
):
    """Ensure the upload endpoint follows the expected execution flow"""
    client = app_no_auth.test_client()

    mocks = UploadMocks(monkeypatch)

    res = client.post(ASSAY_UPLOAD, data=form_data("olink.xlsx", olink_xlsx, "olink"))
    assert res.json
    assert "url_mapping" in res.json

    url_mapping = res.json["url_mapping"]

    # We expect local_path to map to a gcs object name with gcs_prefix
    # based on the contents of olink_xlsx.
    for local_path, gcs_prefix in OLINK_TESTDATA:
        gcs_object_name = url_mapping[local_path]
        assert local_path in url_mapping
        assert gcs_object_name.startswith(gcs_prefix)
        assert (
            local_path not in gcs_object_name
        ), "PHI from local_path shouldn't end up in gcs urls"

    # Check that we tried to grant IAM upload access to gcs_object_name
    mocks.grant_write.assert_called_with(GOOGLE_UPLOAD_BUCKET, test_user.email)

    # Check that we tried to upload the assay metadata excel file
    mocks.upload_xlsx.assert_called_once()

    job_id = res.json["job_id"]
    update_url = f"/assay_uploads/{job_id}"

    # Report an upload failure
    res = client.patch(
        update_url,
        json={"status": "errored"},
        headers={"If-Match": res.json["job_etag"]},
    )
    assert res.status_code == 200
    mocks.revoke_write.assert_called_with(GOOGLE_UPLOAD_BUCKET, test_user.email)
    # This was an upload failure, so success shouldn't have been published
    mocks.publish_success.assert_not_called()

    # Report an upload success
    res = client.patch(
        update_url,
        json={"status": "completed"},
        headers={"If-Match": res.json["_etag"]},
    )
    mocks.publish_success.assert_called_with(job_id)


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
