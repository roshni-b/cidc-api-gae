from functools import wraps
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from cidc_api.app import app
from cidc_api.models import (
    Users,
    TrialMetadata,
    AssayUploads,
    Permissions,
    DownloadableFiles,
    with_default_session,
    AssayUploadStatus,
)
from cidc_schemas.prism import PROTOCOL_ID_FIELD_NAME

from .util import assert_same_elements


def db_test(test):
    """
    Wrap a test function in an application context.
    """

    @wraps(test)
    def wrapped(*args, **kwargs):
        with app.app_context():
            test(*args, **kwargs)

    return wrapped


EMAIL = "test@email.com"
PROFILE = {"email": EMAIL}


@db_test
def test_create_user(db):
    """Try to create a user that doesn't exist"""
    Users.create(PROFILE)
    user = Users.find_by_email(EMAIL)
    assert user
    assert user.email == EMAIL


@db_test
def test_duplicate_user(db):
    """Ensure that a user won't be created twice"""
    Users.create(PROFILE)
    Users.create(PROFILE)
    assert db.query(Users).count() == 1


TRIAL_ID = "cimac-12345"
METADATA = {PROTOCOL_ID_FIELD_NAME: TRIAL_ID, "participants": []}


@db_test
def test_create_trial_metadata(db):
    """Insert a trial metadata record if one doesn't exist"""
    TrialMetadata.create(TRIAL_ID, METADATA)
    trial = TrialMetadata.find_by_trial_id(TRIAL_ID)
    assert trial
    assert trial.metadata_json == METADATA


@db_test
def test_trial_metadata_patch_manifest(db):
    """Update manifest data in a trial_metadata record"""
    # Add a participant to the trial
    metadata_with_participant = METADATA.copy()
    metadata_with_participant["participants"] = [
        {
            "samples": [],
            "cimac_participant_id": "CM-TEST-1234",
            "participant_id": "trial a",
            "cohort_name": "Arm_Z",
        }
    ]

    with pytest.raises(NoResultFound, match=f"No trial found with id {TRIAL_ID}"):
        TrialMetadata.patch_manifest(TRIAL_ID, metadata_with_participant)

    # Create trial
    TrialMetadata.create(TRIAL_ID, METADATA)

    # Try again
    TrialMetadata.patch_manifest(TRIAL_ID, metadata_with_participant)

    # Look the trial up and check that it has the participant in it
    trial = TrialMetadata.find_by_trial_id(TRIAL_ID)
    assert (
        trial.metadata_json["participants"] == metadata_with_participant["participants"]
    )


@db_test
def test_trial_metadata_patch_assay(db):
    """Update assay data in a trial_metadata record"""
    # Add an assay to the trial
    metadata_with_assay = METADATA.copy()
    metadata_with_assay["assays"] = {"wes": []}

    with pytest.raises(NoResultFound, match=f"No trial found with id {TRIAL_ID}"):
        TrialMetadata.patch_manifest(TRIAL_ID, metadata_with_assay)

    # Create trial
    TrialMetadata.create(TRIAL_ID, METADATA)

    # Try again
    TrialMetadata.patch_manifest(TRIAL_ID, metadata_with_assay)

    # Look the trial up and check that it has the assay in it
    trial = TrialMetadata.find_by_trial_id(TRIAL_ID)
    assert trial.metadata_json["assays"] == metadata_with_assay["assays"]


@db_test
def test_partial_patch_trial_metadata(db):
    """Update an existing trial_metadata_record"""
    # Create the initial trial

    db.add(TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA))
    db.commit()

    # Create patch without all required fields (no "participants")
    metadata_patch = {PROTOCOL_ID_FIELD_NAME: TRIAL_ID, "assays": {}}

    # patch it - should be no error/exception
    TrialMetadata._patch_trial_metadata(TRIAL_ID, metadata_patch)


@db_test
def test_create_assay_upload(db):
    """Try to create an assay upload"""
    new_user = Users.create(PROFILE)

    gcs_file_map = {
        "my/first/wes/blob1/2019-08-30T15:51:38.450978": "test-uuid-1",
        "my/first/wes/blob2/2019-08-30T15:51:38.450978": "test-uuid-2",
    }
    metadata_patch = {PROTOCOL_ID_FIELD_NAME: TRIAL_ID}
    gcs_xlsx_uri = "xlsx/assays/wes/12:0:1.5123095"

    # Should fail, since trial doesn't exist yet
    with pytest.raises(IntegrityError):
        AssayUploads.create("wes", EMAIL, gcs_file_map, metadata_patch, gcs_xlsx_uri)
    db.rollback()

    TrialMetadata.create(TRIAL_ID, METADATA)

    new_job = AssayUploads.create(
        "wes", EMAIL, gcs_file_map, metadata_patch, gcs_xlsx_uri
    )
    job = AssayUploads.find_by_id_and_email(new_job.id, PROFILE["email"])
    assert_same_elements(new_job.gcs_file_map, job.gcs_file_map)
    assert job.status == "started"

    assert list(job.upload_uris_with_data_uris_with_uuids()) == [
        (
            "my/first/wes/blob1/2019-08-30T15:51:38.450978",
            "my/first/wes/blob1",
            "test-uuid-1",
        ),
        (
            "my/first/wes/blob2/2019-08-30T15:51:38.450978",
            "my/first/wes/blob2",
            "test-uuid-2",
        ),
    ]


@db_test
def test_create_downloadable_file_from_metadata(db, monkeypatch):
    """Try to create a downloadable file from artifact_core metadata"""
    # fake file metadata
    file_metadata = {
        "object_url": "10021/Patient 1/sample 1/aliquot 1/wes_forward.fastq",
        "file_name": "wes_forward.fastq",
        "file_size_bytes": 1,
        "md5_hash": "hash1234",
        "uploaded_timestamp": datetime.now(),
        "foo": "bar",  # unsupported column - should be filtered
        "data_format": "FASTQ",
    }
    additional_metadata = {"more": "info"}

    # Create the trial (to avoid violating foreign-key constraint)
    TrialMetadata.create(TRIAL_ID, METADATA)
    # Create the file
    DownloadableFiles.create_from_metadata(
        TRIAL_ID, "wes", file_metadata, additional_metadata=additional_metadata
    )

    # Check that we created the file
    new_file = (
        db.query(DownloadableFiles)
        .filter_by(file_name=file_metadata["file_name"])
        .first()
    )
    assert new_file
    del file_metadata["foo"]
    for k in file_metadata.keys():
        assert getattr(new_file, k) == file_metadata[k]
    assert new_file.additional_metadata == additional_metadata


@db_test
def test_create_downloadable_file_from_blob(db, monkeypatch):
    """Try to create a downloadable file from a GCS blob"""
    fake_blob = MagicMock()
    fake_blob.name = "name"
    fake_blob.md5_hash = "12345"
    fake_blob.size = 5
    fake_blob.time_created = datetime.now()

    db.add(TrialMetadata(trial_id="id", metadata_json={}))
    df = DownloadableFiles.create_from_blob(
        "id", "pbmc", "Shipping Manifest", fake_blob
    )

    # Check that the file was created
    assert 1 == db.query(DownloadableFiles).count()
    df_lookup = DownloadableFiles.find_by_id(df.id)
    assert df_lookup.object_url == fake_blob.name
    assert df_lookup.data_format == "Shipping Manifest"
    assert df_lookup.file_size_bytes == 5
    assert df_lookup.md5_hash == "12345"

    # uploading second time to check non duplicating entries
    fake_blob.size = 6
    fake_blob.md5_hash = "6"
    df = DownloadableFiles.create_from_blob(
        "id", "pbmc", "Shipping Manifest", fake_blob
    )

    # Check that the file was created
    assert 1 == db.query(DownloadableFiles).count()
    df_lookup = DownloadableFiles.find_by_id(df.id)
    assert df_lookup.file_size_bytes == 6
    assert df_lookup.md5_hash == "6"


def test_with_default_session(app_no_auth):
    """Test that the with_default_session decorator provides defaults as expected"""

    @with_default_session
    def check_default_session(expected_session_value, session=None):
        assert session == expected_session_value

    with app_no_auth.app_context():
        check_default_session(app_no_auth.data.driver.session)
        fake_session = "some other db session"
        check_default_session(fake_session, session=fake_session)


def test_assay_upload_status():
    """Test AssayUploadStatus transition validation logic"""
    upload_statuses = [
        AssayUploadStatus.UPLOAD_COMPLETED.value,
        AssayUploadStatus.UPLOAD_FAILED.value,
    ]
    merge_statuses = [
        AssayUploadStatus.MERGE_COMPLETED.value,
        AssayUploadStatus.MERGE_FAILED.value,
    ]
    for upload in upload_statuses:
        for merge in merge_statuses:
            for status in [upload, merge]:
                assert AssayUploadStatus.is_valid_transition(
                    AssayUploadStatus.STARTED, status
                )
                assert not AssayUploadStatus.is_valid_transition(
                    status, AssayUploadStatus.STARTED
                )
            assert AssayUploadStatus.is_valid_transition(upload, merge)
            assert not AssayUploadStatus.is_valid_transition(merge, upload)
