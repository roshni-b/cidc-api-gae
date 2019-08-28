from functools import wraps
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from cidc_api.app import app
from cidc_api.models import (
    Users,
    TrialMetadata,
    UploadJobs,
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
METADATA = {
    "lead_organization_study_id": "1234",
    "participants": [{
        "samples": [], 
        "cimac_participant_id": "a",
        "trial_participant_id": "trial a",
        "cohort_id": "cohort_id",
        "arm_id": "arm_id"
        }],
}


@db_test
def test_create_trial_metadata(db):
    """Insert a trial metadata record if one doesn't exist"""
    TrialMetadata.patch_trial_metadata(TRIAL_ID, METADATA)
    trial = TrialMetadata.find_by_trial_id(TRIAL_ID)
    assert trial
    assert trial.metadata_json == METADATA


@db_test
def test_update_trial_metadata(db):
    """Update an existing trial_metadata_record"""
    # Create the initial trial
    TrialMetadata.patch_trial_metadata(TRIAL_ID, METADATA)

    # Add metadata to the trial
    metadata_patch = METADATA.copy()
    metadata_patch["participants"] = [{
        "samples": [], 
        "cimac_participant_id": "b",
        "trial_participant_id": "trial a",
        "cohort_id": "cohort_id",
        "arm_id": "arm_id"
    }]
    TrialMetadata.patch_trial_metadata(TRIAL_ID, metadata_patch)

    # Look the trial up and check that it was merged as expected
    trial = TrialMetadata.find_by_trial_id(TRIAL_ID)
    sort = lambda participant_list: sorted(
        participant_list, key=lambda d: d["cimac_participant_id"]
    )
    expected_participants = METADATA["participants"] + metadata_patch["participants"]
    actual_participants = trial.metadata_json["participants"]
    assert sort(actual_participants) == sort(expected_participants)


@db_test
def test_create_upload_job(db):
    """Try to create an upload job"""
    new_user = Users.create(PROFILE)

    gcs_file_uris = ["my/first/wes/blob1", "my/first/wes/blob2"]
    metadata_json_patch = {"foo": "bar"}

    # Create a fresh upload job
    new_job = UploadJobs.create(EMAIL, gcs_file_uris, metadata_json_patch)
    job = UploadJobs.find_by_id(new_job.id)
    assert_same_elements(new_job.gcs_file_uris, job.gcs_file_uris)
    assert job.status == "started"


@db_test
def test_create_downloadable_file_from_metadata(db, monkeypatch):
    """Try to create a downloadable file from artifact_core metadata"""
    # fake file metadata
    file_metadata = {
        "artifact_category": "Assay Artifact from CIMAC",
        "assay_category": "Whole Exome Sequencing (WES)",
        "object_url": "10021/Patient 1/sample 1/aliquot 1/wes_forward.fastq",
        "file_name": "wes_forward.fastq",
        "file_size_bytes": 1,
        "md5_hash": "hash1234",
        "uploaded_timestamp": datetime.now(),
        "file_type": "FASTQ",
        "foo": "bar",  # unsupported column - should be filtered
    }

    # Create the trial (to avoid violating foreign-key constraint)
    TrialMetadata.patch_trial_metadata(TRIAL_ID, METADATA)
    # Create the file
    DownloadableFiles.create_from_metadata(TRIAL_ID, file_metadata)

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
