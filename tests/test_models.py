from functools import wraps
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from cidc_api.app import app
from cidc_api.models import (
    Users,
    TrialMetadata,
    AssayUploads,
    Permissions,
    DownloadableFiles,
    with_default_session,
)

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
METADATA = {"lead_organization_study_id": TRIAL_ID, "participants": []}


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
            "cimac_participant_id": "b",
            "trial_participant_id": "trial a",
            "cohort_id": "cohort_id",
            "arm_id": "arm_id",
        }
    ]

    with pytest.raises(AssertionError, match=f"No trial found with id {TRIAL_ID}"):
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

    with pytest.raises(AssertionError, match=f"No trial found with id {TRIAL_ID}"):
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
    metadata_patch = {"lead_organization_study_id": TRIAL_ID, "assays": {}}

    # patch it - should be no error/exception
    TrialMetadata.patch_trial_metadata(TRIAL_ID, metadata_patch)


@db_test
def test_create_assay_upload(db):
    """Try to create an assay upload"""
    new_user = Users.create(PROFILE)

    gcs_file_map = {
        "my/first/wes/blob1/2019-08-30T15:51:38.450978": "test-uuid-1",
        "my/first/wes/blob2/2019-08-30T15:51:38.450978": "test-uuid-2",
    }
    metadata_patch = {"lead_organization_study_id": TRIAL_ID}
    gcs_xlsx_uri = "xlsx/assays/wes/12:0:1.5123095"

    # Should fail, since trial doesn't exist yet
    with pytest.raises(IntegrityError):
        AssayUploads.create("wes", EMAIL, gcs_file_map, metadata_patch, gcs_xlsx_uri)
    db.rollback()

    TrialMetadata.create(TRIAL_ID, METADATA)

    new_job = AssayUploads.create(
        "wes", EMAIL, gcs_file_map, metadata_patch, gcs_xlsx_uri
    )
    job = AssayUploads.find_by_id(new_job.id)
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
        "artifact_category": "Assay Artifact from CIMAC",
        "object_url": "10021/Patient 1/sample 1/aliquot 1/wes_forward.fastq",
        "file_name": "wes_forward.fastq",
        "file_size_bytes": 1,
        "md5_hash": "hash1234",
        "uploaded_timestamp": datetime.now(),
        "foo": "bar",  # unsupported column - should be filtered
        "data_format": "FASTQ",
    }

    # Create the trial (to avoid violating foreign-key constraint)
    TrialMetadata.create(TRIAL_ID, METADATA)
    # Create the file
    DownloadableFiles.create_from_metadata(TRIAL_ID, "wes", file_metadata)

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


def test_with_default_session(app_no_auth):
    """Test that the with_default_session decorator provides defaults as expected"""

    @with_default_session
    def check_default_session(expected_session_value, session=None):
        assert session == expected_session_value

    with app_no_auth.app_context():
        check_default_session(app_no_auth.data.driver.session)
        fake_session = "some other db session"
        check_default_session(fake_session, session=fake_session)
