import io
from datetime import datetime
from contextlib import contextmanager
from collections import namedtuple
from unittest.mock import MagicMock

import pytest
from flask import _request_ctx_stack
from werkzeug.exceptions import (
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
from cidc_api.services.ingestion import extract_schema_and_xlsx
from cidc_api.models import (
    TrialMetadata,
    Users,
    AssayUploadStatus,
    Permissions,
    DownloadableFiles,
)

from . import open_data_file
from ..test_models import db_test
from ..util import assert_same_elements
from ..conftest import TEST_EMAIL

from cidc_api.models import (
    Users,
    AssayUploads,
    AssayUploadStatus,
    TrialMetadata,
    CIDCRole,
)

from . import open_data_file
from ..test_models import db_test
from ..util import assert_same_elements
from ..conftest import TEST_EMAIL


@pytest.fixture
def some_file():
    with open_data_file("some_file") as f:
        yield f


TEST_TRIAL = "test_trial"


@pytest.fixture
@db_test
def db_with_trial_and_user(db, test_user):
    # Create the target trial and the uploader
    TrialMetadata.create(
        "test_trial", {
            prism.PROTOCOL_ID_FIELD_NAME: TEST_TRIAL, 
            "participants": [],
            "allowed_cohort_names": ["Arm_Z"],
            "allowed_collection_event_names": [],
            }
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


def test_validate_valid_template(
    app_no_auth, some_file, monkeypatch, test_user, db, db_with_trial_and_user
):
    """Ensure that the validation endpoint returns no errors for a known-valid .xlsx file"""
    client = app_no_auth.test_client()
    data = form_data("pbmc.xlsx", some_file, "pbmc")

    mocks = UploadMocks(monkeypatch)

    give_upload_permission(test_user, TEST_TRIAL, "pbmc", db)

    res = client.post(VALIDATE, data=data)
    assert res.status_code == 200
    assert res.json["errors"] == []
    mocks.iter_errors.assert_called_once()


def test_validate_invalid_template(app_no_auth, some_file, monkeypatch):
    """Ensure that the validation endpoint returns errors for a known-invalid .xlsx file"""
    mocks = UploadMocks(monkeypatch)
    mocks.iter_errors.return_value = ["test error"]
    client = app_no_auth.test_client()
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
            "Unknown template type.*foo/bar",
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
    app_no_auth, some_file, test_user, db_with_trial_and_user, monkeypatch
):
    """Ensure the upload_manifest endpoint follows the expected execution flow"""

    mocks = UploadMocks(monkeypatch, prismify_trial_id="test-non-existing-trial-id")

    client = app_no_auth.test_client()

    res = client.post(MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", some_file, "pbmc"))
    assert res.status_code == 400
    assert "test-non-existing-trial-id" in str(res.json["_error"]["message"])

    # Check that we tried to upload the excel file
    mocks.upload_xlsx.assert_not_called()
    mocks.iter_errors.assert_called_once()
    mocks.prismify.assert_called_once()


def test_upload_invalid_manifest(
    app_no_auth, some_file, test_user, db_with_trial_and_user, db, monkeypatch
):
    """Ensure the upload_manifest endpoint follows the expected execution flow"""

    mocks = UploadMocks(monkeypatch)

    mocks.iter_errors.return_value = ["bad, bad error"]

    give_upload_permission(test_user, TEST_TRIAL, "pbmc", db)

    client = app_no_auth.test_client()

    res = client.post(MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", some_file, "pbmc"))
    assert res.status_code == 400

    assert len(res.json["_error"]["message"]["errors"]) > 0

    # Check that we tried to upload the excel file
    mocks.upload_xlsx.assert_not_called()


def test_upload_unsupported_manifest(
    app_no_auth, some_file, test_user, db_with_trial_and_user, db, monkeypatch
):
    """Ensure the upload_manifest endpoint follows the expected execution flow"""

    mocks = UploadMocks(monkeypatch)

    client = app_no_auth.test_client()

    res = client.post(
        MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", some_file, "UNSUPPORTED_")
    )
    assert res.status_code == 400

    assert "Unknown template type" in res.json["_error"]["message"]
    assert "UNSUPPORTED_".lower() in res.json["_error"]["message"]

    # Check that we tried to upload the excel file
    mocks.upload_xlsx.assert_not_called()


def give_upload_permission(user, trial, type_, db):
    db.add(
        Permissions(
            granted_by_user=user.id,
            granted_to_user=user.id,
            trial_id=TEST_TRIAL,
            assay_type=type_,
        )
    )
    db.commit()


def test_admin_upload(app, test_user, db_with_trial_and_user, monkeypatch):
    """Ensure an admin can upload assays and manifests without specific permissions."""
    mocks = UploadMocks(monkeypatch)

    # Mock an admin user
    test_user.role = CIDCRole.ADMIN.value

    def fake_auth(*args):
        _request_ctx_stack.top.current_user = test_user
        return True

    monkeypatch.setattr(app.auth, "authorized", fake_auth)

    client = app.test_client()

    res = client.post(
        MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", io.BytesIO(b"a"), "pbmc")
    )
    assert res.status_code == 200

    res = client.post(
        ASSAY_UPLOAD, data=form_data("wes.xlsx", io.BytesIO(b"1234"), "wes")
    )
    assert res.status_code == 200


def test_upload_manifest(
    app_no_auth, test_user, db_with_trial_and_user, db, monkeypatch, capsys
):
    """Ensure the upload_manifest endpoint follows the expected execution flow"""

    mocks = UploadMocks(monkeypatch)

    client = app_no_auth.test_client()

    # Try to upload manifest without permission
    res = client.post(
        MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", io.BytesIO(b"a"), "pbmc")
    )
    assert res.status_code == 401
    assert "not authorized to upload pbmc data" in str(res.json["_error"]["message"])

    # Add permission and retry the upload
    give_upload_permission(test_user, TEST_TRIAL, "pbmc", db)

    mocks.clear_all()

    res = client.post(
        MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", io.BytesIO(b"a"), "pbmc")
    )
    assert res.status_code == 200

    # Check that upload alert email was "sent"
    assert "Would send email with subject '[UPLOAD SUCCESS]" in capsys.readouterr()[0]

    # Check that we tried to publish a patient/sample update
    mocks.publish_patient_sample_update.assert_called_once()

    # Check that we tried to upload the excel file
    mocks.make_all_assertions()


def test_upload_manifest_twice(
    app_no_auth, some_file, test_user, db_with_trial_and_user, db, monkeypatch
):
    """Ensure that doing upload_manifest twice will produce only one DownloadableFiles"""

    mocks = UploadMocks(monkeypatch)

    client = app_no_auth.test_client()

    give_upload_permission(test_user, TEST_TRIAL, "pbmc", db)

    res = client.post(MANIFEST_UPLOAD, data=form_data("pbmc.xlsx", some_file, "pbmc"))
    assert res.status_code == 200

    # Check that we tried to publish a patient/sample update
    mocks.publish_patient_sample_update.assert_called_once()

    # Check that we tried to upload the excel file
    mocks.upload_xlsx.assert_called_once()

    assert 1 == db.query(DownloadableFiles).count()

    # uploading second time
    res = client.post(
        MANIFEST_UPLOAD,
        data=form_data("pbmc.xlsx", open_data_file(some_file.name), "pbmc"),
    )
    assert res.status_code == 200

    assert mocks.upload_xlsx.call_count == 2

    assert 1 == db.query(DownloadableFiles).count()


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
        monkeypatch.setattr("gcloud_client.grant_upload_access", self.grant_write)

        self.upload_xlsx = MagicMock(name="upload_xlsx")
        self.upload_xlsx.return_value = MagicMock(name="upload_xlsx.return_value")
        self.upload_xlsx.return_value.name = "trial_id/xlsx/assays/wes/12345"
        self.upload_xlsx.return_value.size = 100
        self.upload_xlsx.return_value.md5_hash = "md5_hash"
        self.upload_xlsx.return_value.time_created = datetime.now()

        monkeypatch.setattr("gcloud_client.upload_xlsx_to_gcs", self.upload_xlsx)

        self.revoke_write = MagicMock(name="revoke_write")
        monkeypatch.setattr("gcloud_client.revoke_upload_access", self.revoke_write)

        self.publish_success = MagicMock(name="publish_success")
        monkeypatch.setattr(
            "gcloud_client.publish_upload_success", self.publish_success
        )

        self.publish_patient_sample_update = MagicMock()
        monkeypatch.setattr(
            "gcloud_client.publish_patient_sample_update",
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
        self.upload_xlsx.assert_called_once()
        self.prismify.assert_called_once()
        self.open_xlsx.assert_called_once()
        self.iter_errors.assert_called_once()

    def clear_all(self):
        for attr in self.__dict__.values():
            if isinstance(attr, MagicMock):
                attr.reset_mock()


finfo = LocalFileUploadEntry


def test_upload_wes(app_no_auth, test_user, db_with_trial_and_user, db, monkeypatch):
    """Ensure the upload endpoint follows the expected execution flow"""
    client = app_no_auth.test_client()

    mocks = UploadMocks(
        monkeypatch,
        prismify_file_entries=[
            finfo("localfile.ext", "test_trial/url/file.ext", "uuid-1", None)
        ],
    )

    # No permission to upload yet
    res = client.post(
        ASSAY_UPLOAD, data=form_data("wes.xlsx", io.BytesIO(b"1234"), "wes")
    )
    assert res.status_code == 401
    assert "not authorized to upload wes data" in str(res.json["_error"]["message"])

    mocks.clear_all()

    # Give permission and retry
    give_upload_permission(test_user, TEST_TRIAL, "wes", db)

    res = client.post(
        ASSAY_UPLOAD, data=form_data("wes.xlsx", io.BytesIO(b"1234"), "wes")
    )
    assert res.json
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
    mocks.grant_write.assert_called_with(GOOGLE_UPLOAD_BUCKET, test_user.email)

    # Check that we tried to upload the assay metadata excel file
    mocks.upload_xlsx.assert_called_once()

    job_id = res.json["job_id"]
    update_url = f"/assay_uploads/{job_id}"

    # Report an upload failure
    res = client.patch(
        update_url,
        json={"status": AssayUploadStatus.UPLOAD_FAILED.value},
        headers={"If-Match": res.json["job_etag"]},
    )
    assert res.status_code == 200
    mocks.revoke_write.assert_called_with(GOOGLE_UPLOAD_BUCKET, test_user.email)
    # This was an upload failure, so success shouldn't have been published
    mocks.publish_success.assert_not_called()

    # Reset the upload status and try the request again
    with app_no_auth.app_context():
        job = AssayUploads.find_by_id_and_email(job_id, test_user.email)
        job.status = AssayUploadStatus.STARTED.value
        db.commit()

    # Report an upload success
    res = client.patch(
        update_url,
        json={"status": AssayUploadStatus.UPLOAD_COMPLETED.value},
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


def test_upload_olink(app_no_auth, test_user, db_with_trial_and_user, db, monkeypatch):
    """Ensure the upload endpoint follows the expected execution flow"""
    client = app_no_auth.test_client()

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
    give_upload_permission(test_user, TEST_TRIAL, "olink", db)

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
    mocks.grant_write.assert_called_with(GOOGLE_UPLOAD_BUCKET, test_user.email)

    # Check that we tried to upload the assay metadata excel file
    mocks.upload_xlsx.assert_called_once()

    job_id = res.json["job_id"]
    update_url = f"/assay_uploads/{job_id}"

    # Report an upload failure
    res = client.patch(
        update_url,
        json={"status": AssayUploadStatus.UPLOAD_FAILED.value},
        headers={"If-Match": res.json["job_etag"]},
    )
    assert res.status_code == 200
    mocks.revoke_write.assert_called_with(GOOGLE_UPLOAD_BUCKET, test_user.email)
    # This was an upload failure, so success shouldn't have been published
    mocks.publish_success.assert_not_called()

    # Test upload status validation - since the upload job's current status
    # is UPLOAD_FAILED, the API shouldn't permit this status to be updated to
    # UPLOAD_COMPLETED.
    bad_res = client.patch(
        update_url,
        json={"status": AssayUploadStatus.UPLOAD_COMPLETED.value},
        headers={"If-Match": res.json["_etag"]},
    )
    assert bad_res.status_code == 400
    assert "Cannot set assay upload status" in bad_res.json["_error"]["message"]

    # Reset the upload status and try the request again
    with app_no_auth.app_context():
        job = AssayUploads.find_by_id_and_email(job_id, test_user.email)
        job.status = AssayUploadStatus.STARTED.value
        db.commit()

    res = client.patch(
        update_url,
        json={"status": AssayUploadStatus.UPLOAD_COMPLETED.value},
        headers={"If-Match": res.json["_etag"]},
    )
    mocks.publish_success.assert_called_with(job_id)


def test_poll_upload_merge_status(app, db, test_user, monkeypatch):
    """
    Check pull_upload_merge_status endpoint behavior
    """
    trial_id = "test-12345"
    metadata = {PROTOCOL_ID_FIELD_NAME: trial_id}

    with app.app_context():
        user = Users.create({"email": test_user.email})
        user.role = CIDCRole.CIMAC_BIOFX_USER.value
        user.approval_date = datetime.now()

        Users.create({"email": "other@email.com"})
        db.add(TrialMetadata(trial_id=trial_id, metadata_json={}))
        upload_1 = AssayUploads.create(
            assay_type="wes",
            uploader_email=test_user.email,
            gcs_file_map={},
            metadata=metadata,
            gcs_xlsx_uri="",
        )

        user_created = upload_1.id
        upload_2 = AssayUploads.create(
            assay_type="wes",
            uploader_email="other@email.com",
            gcs_file_map={},
            metadata=metadata,
            gcs_xlsx_uri="",
        )

        not_user_created = upload_2.id

        db.commit()

    monkeypatch.setattr(
        app.auth, "token_auth", lambda *args: {"email": test_user.email}
    )

    client = app.test_client()

    HEADER = {"Authorization": "Bearer foo"}

    # Upload not found
    res = client.get("/ingestion/poll_upload_merge_status?id=12345", headers=HEADER)
    assert res.status_code == 404

    # Upload not created by user
    res = client.get(
        f"/ingestion/poll_upload_merge_status?id={not_user_created}", headers=HEADER
    )
    assert res.status_code == 404

    user_created_url = f"/ingestion/poll_upload_merge_status?id={user_created}"

    # Upload not-yet-ready
    res = client.get(user_created_url, headers=HEADER)
    assert res.status_code == 200
    assert "retry_in" in res.json and res.json["retry_in"] == 5
    assert "status" not in res.json

    test_details = "A human-friendly reason for this "
    for status in [
        AssayUploadStatus.MERGE_COMPLETED.value,
        AssayUploadStatus.MERGE_FAILED.value,
    ]:
        # Simulate cloud function merge status update
        with app.app_context():
            upload = AssayUploads.find_by_id_and_email(user_created, test_user.email)
            upload.status = status
            upload.status_details = test_details
            db.commit()

        # Upload ready
        res = client.get(user_created_url, headers=HEADER)
        assert res.status_code == 200
        assert "retry_in" not in res.json
        assert "status" in res.json and res.json["status"] == status
        assert (
            "status_details" in res.json and res.json["status_details"] == test_details
        )


def test_extra_metadata(app_no_auth, monkeypatch):
    """Ensure the extra assay metadata endpoint follows the expected execution flow"""

    client = app_no_auth.test_client()
    res = client.post("/ingestion/extra-assay-metadata")
    assert res.status_code == 400
    assert "Expected form" in res.json["_error"]["message"]

    res = client.post("/ingestion/extra-assay-metadata", data={"foo": "bar"})
    assert res.status_code == 400
    assert "job_id" in res.json["_error"]["message"]

    res = client.post("/ingestion/extra-assay-metadata", data={"job_id": 123})
    assert res.status_code == 400
    assert "files" in res.json["_error"]["message"]

    from cidc_api.services.ingestion import AssayUploads as _AssayUploads

    merge_extra_metadata = MagicMock()
    monkeypatch.setattr(_AssayUploads, "merge_extra_metadata", merge_extra_metadata)

    res = client.post(
        "/ingestion/extra-assay-metadata",
        data={
            "job_id": 123,
            "uuid-1": (io.BytesIO(b"fake file 1"), "fname1"),
            "uuid-2": (io.BytesIO(b"fake file 2"), "fname2"),
        },
    )
    assert res.status_code == 200
    merge_extra_metadata.assert_called()


def test_merge_extra_metadata(
    app_no_auth, monkeypatch, db, test_user, db_with_trial_and_user
):
    """Ensure merging of extra metadata follows the expected execution flow"""
    with app_no_auth.app_context():
        assay_upload = AssayUploads.create(
            assay_type="assay_with_extra_md",
            uploader_email=test_user.email,
            gcs_file_map={},
            metadata={
                PROTOCOL_ID_FIELD_NAME: TEST_TRIAL,
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
        db.commit()

        custom_extra_md_parse = MagicMock()
        custom_extra_md_parse.side_effect = lambda f: {"extra_md": f.read().decode()}
        monkeypatch.setattr(
            prism,
            "_EXTRA_METADATA_PARSERS",
            {"assay_with_extra_md": custom_extra_md_parse},
        )

        form_data = {
            "job_id": 137,
            "uuid-1": (io.BytesIO(b"fake file 1"), "fname1"),
            "uuid-2": (io.BytesIO(b"fake file 2"), "fname2"),
        }

        client = app_no_auth.test_client()
        res = client.post("/ingestion/extra-assay-metadata", data=form_data)
        assert res.status_code == 200
        assert custom_extra_md_parse.call_count == 2

        assert 1 == db.query(AssayUploads).count()
        au = db.query(AssayUploads).first()
        assert "extra_md" in au.assay_patch["whatever"]["hierarchy"][0]
        assert "extra_md" in au.assay_patch["whatever"]["hierarchy"][1]
