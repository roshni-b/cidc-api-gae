import io
import logging
from datetime import datetime
from contextlib import contextmanager
from collections import namedtuple
from unittest.mock import MagicMock
from typing import Tuple

import pytest
from werkzeug.exceptions import (
    NotFound,
    Unauthorized,
    UnprocessableEntity,
    HTTPException,
    InternalServerError,
    BadRequest,
    NotImplemented,
)
from cidc_schemas import prism
from cidc_schemas.prism import (
    merge_artifact_extra_metadata,
    PROTOCOL_ID_FIELD_NAME,
    LocalFileUploadEntry,
)

from cidc_api.config.settings import GOOGLE_UPLOAD_BUCKET
from cidc_api.resources.upload_jobs import (
    extract_schema_and_xlsx,
    requires_upload_token_auth,
)
from cidc_api.models import (
    TrialMetadata,
    Users,
    UploadJobs,
    UploadJobStatus,
    Permissions,
    DownloadableFiles,
    CIDCRole,
)

from ..utils import mock_current_user, make_admin, mock_gcloud_client

trial_id = "test_trial"
user_email = "test@email.com"


def setup_trial_and_user(cidc_api, monkeypatch) -> int:
    """
    Insert a trial and a cimac-user into the database, and set the user
    as the current user.
    """
    # this is necessary for adding/removing permissions from this user
    # without trying to contact GCP
    mock_gcloud_client(monkeypatch)

    user = Users(
        email=user_email, role=CIDCRole.CIMAC_USER.value, approval_date=datetime.now()
    )
    mock_current_user(user, monkeypatch)

    with cidc_api.app_context():
        TrialMetadata(
            trial_id="test_trial",
            metadata_json={
                prism.PROTOCOL_ID_FIELD_NAME: trial_id,
                "participants": [],
                "allowed_cohort_names": ["Arm_Z"],
                "allowed_collection_event_names": [],
            },
        ).insert()

        user.insert()
        return user.id


def setup_upload_jobs(cidc_api) -> Tuple[int, int]:
    """
    Insert two uploads into the database created by different users
    and return their IDs.
    """
    with cidc_api.app_context():
        other_user = Users(email="other@email.org")
        other_user.insert()

        job1 = UploadJobs(
            uploader_email=user_email,
            trial_id=trial_id,
            status=UploadJobStatus.STARTED.value,
            metadata_patch={},
            upload_type="",
            gcs_xlsx_uri="",
            multifile=False,
        )
        job2 = UploadJobs(
            uploader_email=other_user.email,
            trial_id=trial_id,
            status=UploadJobStatus.STARTED.value,
            metadata_patch={},
            upload_type="",
            gcs_xlsx_uri="",
            multifile=False,
        )

        job1.insert()
        job2.insert()

        return job1.id, job2.id


def make_nci_biobank_user(user_id, cidc_api):
    with cidc_api.app_context():
        user = Users.find_by_id(user_id)
        user.role = CIDCRole.NCI_BIOBANK_USER.value
        user.update()


def make_cimac_biofx_user(user_id, cidc_api):
    with cidc_api.app_context():
        user = Users.find_by_id(user_id)
        user.role = CIDCRole.CIMAC_BIOFX_USER.value
        user.update()


### UploadJobs REST endpoints ###


def test_list_upload_jobs(cidc_api, clean_db, monkeypatch):
    """Check that listing upload jobs works as expected."""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    user_job, other_job = setup_upload_jobs(cidc_api)

    client = cidc_api.test_client()

    # Regular CIMAC users aren't allowed to list upload jobs
    res = client.get("upload_jobs")
    assert res.status_code == 401

    # Biofx users can only view their own upload jobs by default
    make_cimac_biofx_user(user_id, cidc_api)
    res = client.get("upload_jobs")
    assert res.status_code == 200
    assert res.json["_meta"]["total"] == 1
    assert res.json["_items"][0]["id"] == user_job

    # Admin users can view all upload jobs
    make_admin(user_id, cidc_api)
    res = client.get("upload_jobs")
    assert res.status_code == 200
    assert res.json["_meta"]["total"] == 2
    assert set(i["id"] for i in res.json["_items"]) == set([user_job, other_job])


def test_get_upload_job(cidc_api, clean_db, monkeypatch):
    """Check that getting a single upload job by ID works as expected."""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    user_job, other_job = setup_upload_jobs(cidc_api)

    client = cidc_api.test_client()

    # Regular CIMAC users aren't allowed to get upload jobs
    res = client.get(f"upload_jobs/{user_job}")
    assert res.status_code == 401

    make_cimac_biofx_user(user_id, cidc_api)

    # 404 for non-existent upload
    res = client.get(f"upload_jobs/123123")
    assert res.status_code == 404

    # 404 for another user's upload if non-admin
    res = client.get(f"upload_jobs/{other_job}")
    assert res.status_code == 404

    # 200 for user's upload
    res = client.get(f"upload_jobs/{user_job}")
    assert res.status_code == 200
    assert res.json["id"] == user_job

    # 200 for another user's upload if admin
    make_admin(user_id, cidc_api)
    res = client.get(f"upload_jobs/{other_job}")
    assert res.status_code == 200
    assert res.json["id"] == other_job


def test_requires_upload_token_auth(cidc_api, clean_db, monkeypatch):
    """Check that the requires_upload_token_auth decorator works as expected"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    job_id = setup_upload_jobs(cidc_api)[0]
    with cidc_api.app_context():
        job = UploadJobs.find_by_id(job_id)

    test_route = "/foobarfoo"

    @requires_upload_token_auth
    def endpoint(*args, **kwargs):
        assert "upload_job" in kwargs
        return "ok", 200

    query_route = f"{test_route}/{job_id}"
    nonexistent_job_id = "9999999"

    # User must provide `token` query param
    with cidc_api.test_request_context(query_route):
        with pytest.raises(UnprocessableEntity) as e:
            endpoint(upload_job=job_id)
        assert e._excinfo[1].data["messages"]["query"]["token"] == [
            "Missing data for required field."
        ]

    # User must provide correct `token` query param
    with cidc_api.test_request_context(f"{query_route}?token={'bad token'}"):
        with pytest.raises(
            Unauthorized, match="upload_job token authentication failed"
        ):
            endpoint(upload_job=job_id)

    with cidc_api.test_request_context(f"{query_route}?token={job.token}"):
        assert endpoint(upload_job=job_id) == ("ok", 200)

    # User whose id token authentication succeeds gets a 404 if the relevant job doesn't exist
    with cidc_api.test_request_context(
        f"{test_route}/{nonexistent_job_id}?token={job.token}"
    ):
        with pytest.raises(NotFound):
            endpoint(upload_job=nonexistent_job_id)

    monkeypatch.setattr(
        "cidc_api.resources.upload_jobs.authenticate_and_get_user",
        lambda *args, **kwargs: None,
    )

    # User whose id token authentication fails can still successfully authenticate
    # using an upload token.
    with cidc_api.test_request_context(f"{query_route}?token={job.token}"):
        assert endpoint(upload_job=job_id) == ("ok", 200)

    # User whose id token authentication fails gets a 401 if the relevant job doesn't exist
    with cidc_api.test_request_context(
        f"{test_route}/{nonexistent_job_id}?token={job.token}"
    ):
        with pytest.raises(
            Unauthorized, match="upload_job token authentication failed"
        ):
            endpoint(upload_job=nonexistent_job_id)


def test_update_upload_job(cidc_api, clean_db, monkeypatch):
    """Check that getting a updating an upload job by ID works as expected."""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    user_job, other_job = setup_upload_jobs(cidc_api)
    with cidc_api.app_context():
        user_job_record = UploadJobs.find_by_id(user_job)
        other_job_record = UploadJobs.find_by_id(other_job)

    publish_success = MagicMock()
    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client.publish_upload_success", publish_success
    )
    revoke_upload_access = MagicMock()
    monkeypatch.setattr(
        "cidc_api.shared.gcloud_client.revoke_upload_access", revoke_upload_access
    )

    client = cidc_api.test_client()

    # Possible patches
    upload_success = {"status": UploadJobStatus.UPLOAD_COMPLETED.value}
    upload_failure = {"status": UploadJobStatus.UPLOAD_FAILED.value}
    invalid_update = {"status": UploadJobStatus.MERGE_COMPLETED.value}

    # A user gets error if they fail to provide an upload token
    res = client.patch(f"/upload_jobs/{other_job}", json=upload_success)
    assert res.status_code == 422
    publish_success.assert_not_called()
    revoke_upload_access.assert_not_called()

    # A user gets an authentication error if they provide an incorrect upload token
    res = client.patch(
        f"/upload_jobs/{other_job}?token=nope",
        headers={"if-match": other_job_record._etag},
        json=upload_success,
    )
    assert res.status_code == 401
    assert res.json["_error"]["message"] == "upload_job token authentication failed"
    publish_success.assert_not_called()
    revoke_upload_access.assert_not_called()

    # A user gets an error if they try to update something besides the job's status
    res = client.patch(
        f"/upload_jobs/{other_job}?token={other_job_record.token}",
        headers={"if-match": other_job_record._etag},
        json={"uploader_email": "foo@bar.com", "status": ""},
    )
    assert res.status_code == 422
    assert res.json["_error"]["message"]["uploader_email"][0] == "Unknown field."

    # A user providing a correct token can update their job's status to be a failure
    res = client.patch(
        f"/upload_jobs/{other_job}?token={other_job_record.token}",
        headers={"if-match": other_job_record._etag},
        json=upload_failure,
    )
    assert res.status_code == 200
    publish_success.assert_not_called()
    revoke_upload_access.assert_called_once()
    revoke_upload_access.reset_mock()

    with cidc_api.app_context():
        user_job_record._set_status_no_validation(UploadJobStatus.STARTED.value)
        user_job_record.update()

    # A user can update a job to be a success
    res = client.patch(
        f"/upload_jobs/{user_job}?token={user_job_record.token}",
        headers={"if-match": user_job_record._etag},
        json=upload_success,
    )
    assert res.status_code == 200
    publish_success.assert_called_once_with(user_job)
    revoke_upload_access.assert_called_once()
    publish_success.reset_mock()
    revoke_upload_access.reset_mock()

    with cidc_api.app_context():
        user_job_record._set_status_no_validation(UploadJobStatus.STARTED.value)
        user_job_record.update()

    # Users can't make an illegal state transition
    res = client.patch(
        f"/upload_jobs/{user_job}?token={user_job_record.token}",
        headers={"if-match": user_job_record._etag},
        json=invalid_update,
    )
    assert res.status_code == 400


### Ingestion tests ###


@pytest.fixture
def some_file():
    return io.BytesIO(b"foobar")


def grant_upload_permission(user_id, upload_type, cidc_api):
    with cidc_api.app_context():
        Permissions(
            granted_by_user=user_id,
            granted_to_user=user_id,
            trial_id=trial_id,
            upload_type=upload_type,
        ).insert()


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
ANALYSIS_UPLOAD = "/ingestion/upload_analysis"
MANIFEST_UPLOAD = "/ingestion/upload_manifest"


class UploadMocks:
    def __init__(
        self,
        monkeypatch,
        prismify_trial_id="test_trial",
        prismify_file_entries=None,
        prismify_extra=None,
        prismify_errors=None,
    ):
        self.grant_write = MagicMock()
        monkeypatch.setattr(
            "cidc_api.shared.gcloud_client.grant_upload_access", self.grant_write
        )

        self.upload_xlsx = MagicMock(name="upload_xlsx")
        self.upload_xlsx.return_value = MagicMock(name="upload_xlsx.return_value")
        self.upload_xlsx.return_value.name = "trial_id/xlsx/assays/wes/12345"
        self.upload_xlsx.return_value.size = 100
        self.upload_xlsx.return_value.md5_hash = "md5_hash"
        self.upload_xlsx.return_value.crc32c = "crc32c_hash"
        self.upload_xlsx.return_value.time_created = datetime.now()

        monkeypatch.setattr(
            "cidc_api.shared.gcloud_client.upload_xlsx_to_gcs", self.upload_xlsx
        )

        self.revoke_write = MagicMock(name="revoke_write")
        monkeypatch.setattr(
            "cidc_api.shared.gcloud_client.revoke_upload_access", self.revoke_write
        )

        self.publish_success = MagicMock(name="publish_success")
        monkeypatch.setattr(
            "cidc_api.shared.gcloud_client.publish_upload_success", self.publish_success
        )

        self.publish_patient_sample_update = MagicMock()
        monkeypatch.setattr(
            "cidc_api.shared.gcloud_client.publish_patient_sample_update",
            self.publish_patient_sample_update,
        )

        self.open_xlsx = MagicMock(name="open_xlsx")
        self.open_xlsx.return_value = MagicMock(name="open_xlsx.return_value")
        monkeypatch.setattr("openpyxl.load_workbook", self.open_xlsx)

        self.iter_errors = MagicMock(name="iter_errors")
        self.iter_errors.return_value = (_ for _ in range(0))
        monkeypatch.setattr(
            "cidc_schemas.template_reader.XlTemplateReader.iter_errors",
            self.iter_errors,
        )

        self.prismify = MagicMock(name="prismify")
        monkeypatch.setattr("cidc_schemas.prism.prismify", self.prismify)
        self.prismify.return_value = (
            dict(
                **{PROTOCOL_ID_FIELD_NAME: prismify_trial_id}, **(prismify_extra or {})
            ),
            prismify_file_entries or [],
            prismify_errors or [],
        )

    def make_all_assertions(self):
        self.prismify.assert_called_once()
        self.open_xlsx.assert_called_once()
        self.iter_errors.assert_called_once()

    def clear_all(self):
        for attr in self.__dict__.values():
            if isinstance(attr, MagicMock):
                attr.reset_mock()


def test_validate_valid_template(cidc_api, some_file, clean_db, monkeypatch):
    """Ensure that the validation endpoint returns no errors for a known-valid .xlsx file"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    make_admin(user_id, cidc_api)

    client = cidc_api.test_client()
    data = form_data("pbmc.xlsx", some_file, "pbmc")

    mocks = UploadMocks(monkeypatch)

    grant_upload_permission(user_id, "pbmc", cidc_api)

    res = client.post(VALIDATE, data=data)
    assert res.status_code == 200
    assert res.json["errors"] == []
    mocks.iter_errors.assert_called_once()


def test_validate_invalid_template(cidc_api, some_file, clean_db, monkeypatch):
    """Ensure that the validation endpoint returns errors for a known-invalid .xlsx file"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    make_admin(user_id, cidc_api)

    mocks = UploadMocks(monkeypatch)
    mocks.iter_errors.return_value = ["test error"]

    grant_upload_permission(user_id, "pbmc", cidc_api)

    client = cidc_api.test_client()
    data = form_data("pbmc.xlsx", some_file, "pbmc")
    res = client.post(VALIDATE, data=data)
    assert res.status_code == 400
    assert len(res.json["_error"]["message"]) > 0


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
            "not supported",
        ],
    ],
)
def test_extract_schema_and_xlsx_failures(
    cidc_api, url, data, error, message, clean_db, monkeypatch
):
    """
    Test that we get the expected errors when trying to extract
    schema/template from a malformed request.
    """
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    with cidc_api.test_request_context(url, data=data):
        with pytest.raises(error, match=message):
            extract_schema_and_xlsx([])


def test_upload_manifest_non_existing_trial_id(
    cidc_api, some_file, clean_db, monkeypatch
):
    """Ensure the upload_manifest endpoint follows the expected execution flow"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    make_admin(user_id, cidc_api)

    mocks = UploadMocks(monkeypatch, prismify_trial_id="test-non-existing-trial-id")

    client = cidc_api.test_client()

    res = client.post(MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", some_file, "pbmc"))
    assert res.status_code == 400
    assert "test-non-existing-trial-id" in str(res.json["_error"]["message"])

    # Check that we tried to upload the excel file
    mocks.upload_xlsx.assert_not_called()
    mocks.iter_errors.assert_called_once()
    mocks.prismify.assert_called_once()


def test_upload_invalid_manifest(cidc_api, some_file, clean_db, monkeypatch):
    """Ensure the upload_manifest endpoint follows the expected execution flow"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    make_admin(user_id, cidc_api)

    mocks = UploadMocks(monkeypatch)

    mocks.iter_errors.return_value = ["bad, bad error"]

    grant_upload_permission(user_id, "pbmc", cidc_api)

    client = cidc_api.test_client()

    res = client.post(MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", some_file, "pbmc"))
    assert res.status_code == 400

    assert len(res.json["_error"]["message"]["errors"]) > 0

    # Check that we tried to upload the excel file
    mocks.upload_xlsx.assert_not_called()


def test_upload_unsupported_manifest(cidc_api, some_file, clean_db, monkeypatch):
    """Ensure the upload_manifest endpoint follows the expected execution flow"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    make_admin(user_id, cidc_api)

    mocks = UploadMocks(monkeypatch)

    client = cidc_api.test_client()

    res = client.post(
        MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", some_file, "UNSUPPORTED_")
    )
    assert res.status_code == 400

    assert (
        "'unsupported_' is not supported for this endpoint."
        in res.json["_error"]["message"]
    )
    assert "UNSUPPORTED_".lower() in res.json["_error"]["message"]

    # Check that we tried to upload the excel file
    mocks.upload_xlsx.assert_not_called()


def test_admin_upload(cidc_api, clean_db, monkeypatch):
    """Ensure an admin can upload assays and manifests without specific permissions."""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    make_admin(user_id, cidc_api)
    mocks = UploadMocks(monkeypatch)

    client = cidc_api.test_client()

    res = client.post(
        MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", io.BytesIO(b"a"), "pbmc")
    )
    assert res.status_code == 200

    res = client.post(
        ASSAY_UPLOAD, data=form_data("wes.xlsx", io.BytesIO(b"1234"), "wes_fastq")
    )
    assert res.status_code == 200


def test_upload_manifest(cidc_api, clean_db, monkeypatch, caplog):
    """Ensure the upload_manifest endpoint follows the expected execution flow"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    mocks = UploadMocks(monkeypatch)

    client = cidc_api.test_client()

    # NCI users can upload manifests without explicit permission
    make_nci_biobank_user(user_id, cidc_api)
    with caplog.at_level(logging.DEBUG):
        res = client.post(
            MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", io.BytesIO(b"a"), "pbmc")
        )
    assert res.status_code == 200

    # Check that upload alert email was "sent"
    assert "Would send email with subject '[UPLOAD SUCCESS]" in caplog.text

    # Check that we tried to publish a patient/sample update
    mocks.publish_patient_sample_update.assert_called_once()

    # Check that we tried to upload the excel file
    mocks.make_all_assertions()


def test_upload_manifest_twice(cidc_api, clean_db, monkeypatch):
    """Ensure that doing upload_manifest twice will produce only one DownloadableFiles"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    mocks = UploadMocks(monkeypatch)

    client = cidc_api.test_client()

    grant_upload_permission(user_id, "pbmc", cidc_api)
    make_nci_biobank_user(user_id, cidc_api)

    res = client.post(
        MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", io.BytesIO(b"a"), "pbmc")
    )
    assert res.status_code == 200

    # Check that we tried to publish a patient/sample update
    mocks.publish_patient_sample_update.assert_called_once()

    with cidc_api.app_context():
        assert not DownloadableFiles.list()  # manifest is not stored

    # uploading second time
    res = client.post(
        MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", io.BytesIO(b"b"), "pbmc")
    )
    assert res.status_code == 200

    assert mocks.upload_xlsx.call_count == 0  # manifest is not stored

    with cidc_api.app_context():
        assert not DownloadableFiles.list()  # manifest is not stored


finfo = LocalFileUploadEntry


def test_upload_endpoint_blocking(cidc_api, clean_db, monkeypatch):
    """Ensure you can't upload an analysis to the upload assay endpoint or vice versa"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    make_admin(user_id, cidc_api)
    mocks = UploadMocks(monkeypatch)

    client = cidc_api.test_client()

    assay_form = lambda: form_data("cytof.xlsx", io.BytesIO(b"1234"), "cytof")
    analysis_form = lambda: form_data(
        "cytof_analysis.xlsx", io.BytesIO(b"1234"), "cytof_analysis"
    )

    res = client.post(ASSAY_UPLOAD, data=assay_form())
    assert res.status_code == 200
    res = client.post(ASSAY_UPLOAD, data=analysis_form())
    assert "not supported" in res.json["_error"]["message"]
    assert res.status_code == 400

    res = client.post(ANALYSIS_UPLOAD, data=analysis_form())
    assert res.status_code == 200
    res = client.post(ANALYSIS_UPLOAD, data=assay_form())
    assert "not supported" in res.json["_error"]["message"]
    assert res.status_code == 400


def test_upload_wes(cidc_api, clean_db, monkeypatch):
    """Ensure the upload endpoint follows the expected execution flow"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    make_cimac_biofx_user(user_id, cidc_api)
    with cidc_api.app_context():
        user = Users.find_by_id(user_id)

    client = cidc_api.test_client()

    mocks = UploadMocks(
        monkeypatch,
        prismify_file_entries=[
            finfo("localfile.ext", "test_trial/url/file.ext", "uuid-1", None)
        ],
    )

    # No permission to upload yet
    res = client.post(
        ASSAY_UPLOAD, data=form_data("wes.xlsx", io.BytesIO(b"1234"), "wes_fastq")
    )
    assert res.status_code == 401
    assert "not authorized to upload wes_fastq data" in str(
        res.json["_error"]["message"]
    )

    mocks.clear_all()

    # Give permission and retry
    grant_upload_permission(user_id, "wes_fastq", cidc_api)

    res = client.post(
        ASSAY_UPLOAD, data=form_data("wes.xlsx", io.BytesIO(b"1234"), "wes_fastq")
    )
    assert res.status_code == 200
    assert "url_mapping" in res.json
    url_mapping = res.json["url_mapping"]

    # WES assay does not have any extra_metadata files, but its (and every assay's) response
    # should have an extra_metadata field.
    assert "extra_metadata" in res.json
    extra_metadata = res.json["extra_metadata"]
    assert extra_metadata is None

    # We expect local_path to map to a gcs object name with gcs_prefix
    local_path = "localfile.ext"
    gcs_prefix = "test_trial/url/file.ext"
    gcs_object_name = url_mapping[local_path]
    assert local_path in url_mapping
    assert gcs_object_name.startswith(gcs_prefix)
    assert not gcs_object_name.endswith(
        local_path
    ), "PHI from local_path shouldn't end up in gcs urls"

    # Check that we tried to grant IAM upload access to gcs_object_name
    mocks.grant_write.assert_called_with(user.email)

    # Check that we tried to upload the assay metadata excel file
    mocks.upload_xlsx.assert_called_once()

    job_id = res.json["job_id"]
    update_url = f"/upload_jobs/{job_id}"

    # Report an upload failure
    res = client.patch(
        f"{update_url}?token={res.json['token']}",
        json={"status": UploadJobStatus.UPLOAD_FAILED.value},
        headers={"If-Match": res.json["job_etag"]},
    )
    assert res.status_code == 200
    mocks.revoke_write.assert_called_with(user.email)
    # This was an upload failure, so success shouldn't have been published
    mocks.publish_success.assert_not_called()

    # Reset the upload status and try the request again
    with cidc_api.app_context():
        job = UploadJobs.find_by_id_and_email(job_id, user.email)
        job._set_status_no_validation(UploadJobStatus.STARTED.value)
        job.update()
        _etag = job._etag

    # Report an upload success
    res = client.patch(
        f"{update_url}?token={res.json['token']}",
        json={"status": UploadJobStatus.UPLOAD_COMPLETED.value},
        headers={"If-Match": _etag},
    )
    assert res.status_code == 200
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


def test_upload_olink(cidc_api, clean_db, monkeypatch):
    """Ensure the upload endpoint follows the expected execution flow"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    with cidc_api.app_context():
        user = Users.find_by_id(user_id)

    make_cimac_biofx_user(user_id, cidc_api)

    client = cidc_api.test_client()

    mocks = UploadMocks(
        monkeypatch,
        prismify_file_entries=[
            finfo(lp, url, "uuid" + str(i), "npx" in url)
            for i, (lp, url) in enumerate(OLINK_TESTDATA)
        ],
    )

    # No permission to upload yet
    res = client.post(
        ASSAY_UPLOAD, data=form_data("olink.xlsx", io.BytesIO(b"1234"), "olink")
    )
    assert res.status_code == 401
    assert "not authorized to upload olink data" in str(res.json["_error"]["message"])

    mocks.clear_all()

    # Give permission and retry
    grant_upload_permission(user_id, "olink", cidc_api)

    res = client.post(
        ASSAY_UPLOAD, data=form_data("olink.xlsx", io.BytesIO(b"1234"), "olink")
    )
    assert res.status_code == 200

    assert "url_mapping" in res.json
    url_mapping = res.json["url_mapping"]

    # Olink assay has extra_metadata files
    assert "extra_metadata" in res.json
    extra_metadata = res.json["extra_metadata"]
    assert type(extra_metadata) == dict

    # We expect local_path to map to a gcs object name with gcs_prefix.
    for local_path, gcs_prefix in OLINK_TESTDATA:
        gcs_object_name = url_mapping[local_path]
        assert local_path in url_mapping
        assert gcs_object_name.startswith(gcs_prefix)
        assert (
            local_path not in gcs_object_name
        ), "PHI from local_path shouldn't end up in gcs urls"

    # Check that we tried to grant IAM upload access to gcs_object_name
    mocks.grant_write.assert_called_with(user.email)

    # Check that we tried to upload the assay metadata excel file
    mocks.upload_xlsx.assert_called_once()

    job_id = res.json["job_id"]
    update_url = f"/upload_jobs/{job_id}"

    # Report an upload failure
    res = client.patch(
        f"{update_url}?token={res.json['token']}",
        json={"status": UploadJobStatus.UPLOAD_FAILED.value},
        headers={"If-Match": res.json["job_etag"]},
    )
    assert res.status_code == 200
    mocks.revoke_write.assert_called_with(user.email)
    # This was an upload failure, so success shouldn't have been published
    mocks.publish_success.assert_not_called()

    # Test upload status validation - since the upload job's current status
    # is UPLOAD_FAILED, the API shouldn't permit this status to be updated to
    # UPLOAD_COMPLETED.
    bad_res = client.patch(
        f"{update_url}?token={res.json['token']}",
        json={"status": UploadJobStatus.UPLOAD_COMPLETED.value},
        headers={"If-Match": res.json["_etag"]},
    )
    assert bad_res.status_code == 400
    assert (
        "status upload-failed can't transition to status upload-completed"
        in bad_res.json["_error"]["message"]
    )

    # Reset the upload status and try the request again
    with cidc_api.app_context():
        job = UploadJobs.find_by_id_and_email(job_id, user.email)
        job._set_status_no_validation(UploadJobStatus.STARTED.value)
        job.update()
        _etag = job._etag

    res = client.patch(
        f"{update_url}?token={res.json['token']}",
        json={"status": UploadJobStatus.UPLOAD_COMPLETED.value},
        headers={"If-Match": _etag},
    )
    assert res.status_code == 200
    mocks.publish_success.assert_called_with(job_id)


def test_poll_upload_merge_status(cidc_api, clean_db, monkeypatch):
    """
    Check pull_upload_merge_status endpoint behavior
    """
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    with cidc_api.app_context():
        user = Users.find_by_id(user_id)
    make_cimac_biofx_user(user_id, cidc_api)

    metadata = {PROTOCOL_ID_FIELD_NAME: trial_id}

    with cidc_api.app_context():
        other_user = Users(email="other@email.com")
        other_user.insert()
        upload_job = UploadJobs.create(
            upload_type="wes",
            uploader_email=user.email,
            gcs_file_map={},
            metadata=metadata,
            gcs_xlsx_uri="",
        )
        upload_job.insert()
        upload_job_id = upload_job.id

    client = cidc_api.test_client()

    # Upload not found
    res = client.get(
        f"/ingestion/poll_upload_merge_status/12345?token={upload_job.token}"
    )
    assert res.status_code == 404

    upload_job_url = (
        f"/ingestion/poll_upload_merge_status/{upload_job_id}?token={upload_job.token}"
    )

    # Upload not-yet-ready
    res = client.get(upload_job_url)
    assert res.status_code == 200
    assert "retry_in" in res.json and res.json["retry_in"] == 5
    assert "status" not in res.json

    test_details = "A human-friendly reason for this "
    for status in [
        UploadJobStatus.MERGE_COMPLETED.value,
        UploadJobStatus.MERGE_FAILED.value,
    ]:
        # Simulate cloud function merge status update
        with cidc_api.app_context():
            upload_job._set_status_no_validation(status)
            upload_job.status_details = test_details
            upload_job.update()

        # Upload ready
        res = client.get(upload_job_url)
        assert res.status_code == 200
        assert "retry_in" not in res.json
        assert "status" in res.json and res.json["status"] == status
        assert (
            "status_details" in res.json and res.json["status_details"] == test_details
        )


def test_extra_assay_metadata(cidc_api, clean_db, monkeypatch):
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    make_cimac_biofx_user(user_id, cidc_api)
    job_id, _ = setup_upload_jobs(cidc_api)

    client = cidc_api.test_client()

    res = client.post("/ingestion/extra-assay-metadata")
    assert res.status_code == 400
    assert "Expected form" in res.json["_error"]["message"]

    res = client.post("/ingestion/extra-assay-metadata", data={"foo": "bar"})
    assert res.status_code == 400
    assert "job_id" in res.json["_error"]["message"]

    res = client.post("/ingestion/extra-assay-metadata", data={"job_id": 123})
    assert res.status_code == 400
    assert "files" in res.json["_error"]["message"]

    with monkeypatch.context() as m:
        res = client.post(
            "/ingestion/extra-assay-metadata",
            data={"job_id": 987, "uuid-1": (io.BytesIO(b"fake file"), "fname1")},
        )
        assert res.status_code == 400
        assert "987 doesn't exist" in res.json["_error"]["message"]

    with monkeypatch.context() as m:
        merge_extra_metadata = MagicMock()
        merge_extra_metadata.return_value = MagicMock()  # not caught
        m.setattr(
            "cidc_api.models.UploadJobs.merge_extra_metadata", merge_extra_metadata
        )
        res = client.post(
            "/ingestion/extra-assay-metadata",
            data={"job_id": job_id, "uuid-1": (io.BytesIO(b"fake file"), "fname1")},
        )
        assert res.status_code == 200
        merge_extra_metadata.assert_called_once()

    with monkeypatch.context() as m:
        merge_artifact_extra_metadata = MagicMock()
        merge_artifact_extra_metadata.return_value = ("md patch", {}, "nothing")
        m.setattr(
            "cidc_schemas.prism.merge_artifact_extra_metadata",
            merge_artifact_extra_metadata,
        )
        res = client.post(
            "/ingestion/extra-assay-metadata",
            data={"job_id": job_id, "uuid-1": (io.BytesIO(b"fake file"), "fname1")},
        )
        assert res.status_code == 200
        merge_artifact_extra_metadata.assert_called_once()

    with monkeypatch.context() as m:
        merge_artifact_extra_metadata = MagicMock()
        merge_artifact_extra_metadata.side_effect = ValueError("testing")
        m.setattr(
            "cidc_schemas.prism.merge_artifact_extra_metadata",
            merge_artifact_extra_metadata,
        )
        res = client.post(
            "/ingestion/extra-assay-metadata",
            data={"job_id": job_id, "uuid-1": (io.BytesIO(b"fake file"), "fname1")},
        )
        assert res.status_code == 400  # ValueError should get translated to BadRequest
        assert "testing" in res.json["_error"]["message"]

    with monkeypatch.context():
        merge_artifact_extra_metadata = MagicMock()
        merge_artifact_extra_metadata.side_effect = TypeError("testing")
        monkeypatch.setattr(
            "cidc_schemas.prism.merge_artifact_extra_metadata",
            merge_artifact_extra_metadata,
        )
        res = client.post(
            "/ingestion/extra-assay-metadata",
            data={"job_id": job_id, "uuid-1": (io.BytesIO(b"fake file"), "fname1")},
        )
        assert res.status_code == 500  # TypeError should be a server error


def test_merge_extra_metadata(cidc_api, clean_db, monkeypatch):
    """Ensure merging of extra metadata follows the expected execution flow"""
    user_id = setup_trial_and_user(cidc_api, monkeypatch)
    with cidc_api.app_context():
        user = Users.find_by_id(user_id)
    make_cimac_biofx_user(user_id, cidc_api)

    with cidc_api.app_context():
        assay_upload = UploadJobs.create(
            upload_type="assay_with_extra_md",
            uploader_email=user.email,
            gcs_file_map={},
            metadata={
                PROTOCOL_ID_FIELD_NAME: trial_id,
                "whatever": {
                    "hierarchy": [
                        {"we just need a": "uuid-1", "to be able": "to merge"},
                        {"and": "uuid-2"},
                    ]
                },
            },
            gcs_xlsx_uri="",
            commit=False,
        )
        assay_upload.id = 137
        assay_upload.insert()

        custom_extra_md_parse = MagicMock()
        custom_extra_md_parse.side_effect = lambda f: {"extra_md": f.read().decode()}
        monkeypatch.setattr(
            "cidc_schemas.prism.merger.EXTRA_METADATA_PARSERS",
            {"assay_with_extra_md": custom_extra_md_parse},
        )

        form_data = {
            "job_id": 137,
            "uuid-1": (io.BytesIO(b"fake file 1"), "fname1"),
            "uuid-2": (io.BytesIO(b"fake file 2"), "fname2"),
        }

        client = cidc_api.test_client()
        res = client.post("/ingestion/extra-assay-metadata", data=form_data)
        assert res.status_code == 200
        assert custom_extra_md_parse.call_count == 2

        fetched_jobs = UploadJobs.list()
        assert 1 == len(fetched_jobs)
        au = fetched_jobs[0]
        assert "extra_md" in au.metadata_patch["whatever"]["hierarchy"][0]
        assert "extra_md" in au.metadata_patch["whatever"]["hierarchy"][1]
