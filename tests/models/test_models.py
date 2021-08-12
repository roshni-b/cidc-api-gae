from jsonschema.validators import validate
from cidc_api.models.schemas import TrialMetadataListSchema
import io
import logging
from copy import deepcopy
from functools import wraps
from datetime import datetime, timedelta
from unittest.mock import MagicMock, call

import pytest
from sqlalchemy.exc import IntegrityError, InvalidRequestError
from sqlalchemy.orm.exc import NoResultFound

from cidc_api.app import app
from cidc_api.models import (
    CommonColumns,
    Users,
    TrialMetadata,
    UploadJobs,
    Permissions,
    DownloadableFiles,
    with_default_session,
    UploadJobStatus,
    NoResultFound,
    ValidationMultiError,
    CIDCRole,
)
from cidc_api.config.settings import (
    PAGINATION_PAGE_SIZE,
    MAX_PAGINATION_PAGE_SIZE,
    INACTIVE_USER_DAYS,
)
from cidc_schemas.prism import PROTOCOL_ID_FIELD_NAME
from cidc_schemas import prism

from ..utils import make_admin, make_role, mock_gcloud_client


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


def test_common_compute_etag():
    """Check that compute_etag excludes private fields"""

    u = Users()

    # Updates to private fields shouldn't change the etag
    etag = u.compute_etag()
    u._updated = datetime.now()
    assert u.compute_etag() == etag

    # Updates to public fields should change the etag
    u.first_n = "foo"
    new_etag = u.compute_etag()
    assert new_etag != etag
    u.first_n = "buzz"
    assert u.compute_etag() != new_etag

    # Compute etag returns the same result if `u` doesn't change
    assert u.compute_etag() == u.compute_etag()


@db_test
def test_common_insert(clean_db):
    """Test insert, inherited from CommonColumns"""
    # Check disabling committing
    u1 = Users(email="a")
    u1.insert(commit=False)
    assert not u1.id

    # Insert a new record without disabling committing
    u2 = Users(email="b")
    u2.insert()
    assert u1.id and u1._etag
    assert u2.id and u2._etag
    assert u1._etag != u2._etag

    assert Users.find_by_id(u1.id)
    assert Users.find_by_id(u2.id)


@db_test
def test_common_update(clean_db):
    """Test update, inherited from CommonColumns"""
    email = "foo"
    user = Users(id=1, email=email)

    # Record not found
    with pytest.raises(NoResultFound):
        user.update()

    user.insert()

    _updated = user._updated

    # Update via setattr and changes
    first_n = "hello"
    last_n = "goodbye"
    user.last_n = last_n
    user.update(changes={"first_n": first_n})
    user = Users.find_by_id(user.id)
    assert user._updated > _updated
    assert user.first_n == first_n
    assert user.last_n == last_n

    _updated = user._updated
    _etag = user._etag

    # Make sure you can clear a field to null
    user.update(changes={"first_n": None})
    user = Users.find_by_id(user.id)
    assert user._updated > _updated
    assert _etag != user._etag
    assert user.first_n is None

    _updated = user._updated
    _etag = user._etag

    # Make sure etags don't change if public fields don't change
    user.update()
    user = Users.find_by_id(user.id)
    assert user._updated > _updated
    assert _etag == user._etag


@db_test
def test_common_delete(clean_db):
    """Test delete, inherited from CommonColumns"""
    user1 = Users(email="foo")
    user2 = Users(email="bar")

    # Try to delete an uninserted record
    with pytest.raises(InvalidRequestError):
        user1.delete()

    user1.insert()
    user2.insert()

    # Defer a deletion with commit=False
    user1.delete(commit=False)
    assert Users.find_by_id(user1.id)

    # Delete with auto-commit
    user2.delete()
    assert not Users.find_by_id(user1.id)
    assert not Users.find_by_id(user2.id)


@db_test
def test_common_list(clean_db):
    """Test listing behavior, inherited from CommonColumns"""
    for i in range(105):
        name = f"user_{i}"
        Users(email=f"{name}@example.com", first_n=name).insert()

    # List with defaults
    user_list = Users.list()
    assert len(user_list) == PAGINATION_PAGE_SIZE

    # List with different pagination size
    short_list = Users.list(page_size=5)
    assert len(short_list) == 5

    # List with sorting
    sorted_list = Users.list(sort_field="id")
    assert sorted_list[0].first_n == "user_104"
    first_page = Users.list(sort_field="id", sort_direction="asc")
    assert first_page[0].first_n == "user_0"
    sorted_list = Users.list(sort_field="first_n", sort_direction="asc")
    assert sorted_list[0].first_n == "user_0"

    # Get the second page
    second_page = Users.list(page_num=1, sort_field="id", sort_direction="asc")
    assert second_page[0].first_n == "user_25"
    assert second_page[-1].first_n == "user_49"

    # Get the last page
    last_page = Users.list(page_num=4, sort_field="id", sort_direction="asc")
    assert len(last_page) == 5

    # Get a negative page
    negative_page = Users.list(page_num=-1, sort_field="id", sort_direction="asc")
    assert set(n.id for n in negative_page) == set(f.id for f in first_page)

    # Get a too-high page
    too_high_page = Users.list(page_num=100, sort_field="id", sort_direction="asc")
    assert len(too_high_page) == 0

    # Add a filter
    def f(q):
        return q.filter(Users.first_n.like("%9%"))

    all_expected_values = set(f"user_{i}" for i in range(100) if "9" in str(i))
    filtered_page = Users.list(
        filter_=f, page_num=0, sort_field="id", sort_direction="asc"
    )
    assert all_expected_values == set(f.first_n for f in filtered_page)

    # Get a too-large page
    for i in range(106, 300):
        name = f"user_{i}"
        Users(email=f"{name}@example.com", first_n=name).insert()
    big_page = Users.list(page_size=1e10)
    assert len(big_page) == MAX_PAGINATION_PAGE_SIZE


@db_test
def test_common_count(clean_db):
    """Test counting behavior, inherited from CommonColumns"""
    num = 105
    for i in range(num):
        name = f"user_{i}"
        Users(email=f"{name}@example.com", first_n=name).insert()

    # Count without filter
    assert Users.count() == num

    # Count with filter
    def f(q):
        return q.filter(Users.first_n.like("%9%"))

    num_expected = len(list(f"user_{i}" for i in range(100) if "9" in str(i)))
    assert Users.count(filter_=f) == num_expected


@db_test
def test_create_user(clean_db):
    """Try to create a user that doesn't exist"""
    Users.create(PROFILE)
    user = Users.find_by_email(EMAIL)
    assert user
    assert user.email == EMAIL


@db_test
def test_duplicate_user(clean_db):
    """Ensure that a user won't be created twice"""
    Users.create(PROFILE)
    Users.create(PROFILE)
    assert clean_db.query(Users).count() == 1


@db_test
def test_disable_inactive_users(clean_db):
    """Check that the disable_inactive_users method disables users appropriately"""
    # Create two users who should be disabled, and one who should not
    now = datetime.now()
    Users(email="1@", _accessed=now - timedelta(days=INACTIVE_USER_DAYS)).insert()
    Users(email="2@", _accessed=now - timedelta(days=INACTIVE_USER_DAYS + 5)).insert()
    Users(email="3@", _accessed=now - timedelta(days=INACTIVE_USER_DAYS - 1)).insert()

    # All users start off enabled
    for user in Users.list():
        assert user.disabled == False

    disabled = Users.disable_inactive_users()
    disabled = [u for u in disabled]

    assert len(disabled)

    users = Users.list()
    assert len([u for u in users if u.disabled]) == len(disabled)
    assert [u for u in users if not u.disabled][0].email == "3@"


TRIAL_ID = "cimac-12345"
METADATA = {
    PROTOCOL_ID_FIELD_NAME: TRIAL_ID,
    "participants": [],
    "allowed_cohort_names": ["Arm_Z"],
    "allowed_collection_event_names": [],
}


@db_test
def test_create_trial_metadata(clean_db):
    """Insert a trial metadata record if one doesn't exist"""
    TrialMetadata.create(TRIAL_ID, METADATA)
    trial = TrialMetadata.find_by_trial_id(TRIAL_ID)
    assert trial
    assert trial.metadata_json == METADATA

    # Check that you can't insert a trial with invalid metadata
    with pytest.raises(ValidationMultiError, match="'buzz' was unexpected"):
        TrialMetadata.create("foo", {"buzz": "bazz"})

    with pytest.raises(ValidationMultiError, match="'buzz' was unexpected"):
        TrialMetadata(trial_id="foo", metadata_json={"buzz": "bazz"}).insert()


@db_test
def test_trial_metadata_insert(clean_db):
    """Test that metadata validation on insert works as expected"""
    # No error with valid metadata
    trial = TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA)
    trial.insert()

    # Error with invalid metadata
    trial.metadata_json = {"foo": "bar"}
    with pytest.raises(ValidationMultiError):
        trial.insert()

    # No error if validate_metadata=False
    trial.insert(validate_metadata=False)


@db_test
def test_trial_metadata_update(clean_db):
    """Test that metadata validation on update works as expected"""
    trial = TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA)
    trial.insert()

    # No error on valid `changes` update
    trial.update(changes={"metadata_json": {**METADATA, "nct_id": "foo"}})

    # No error on valid attribute update
    trial.metadata_json = {**METADATA, "nct_id": "bar"}
    trial.update()

    bad_json = {"metadata_json": {"foo": "bar"}}

    # Error on invalid `changes` update
    with pytest.raises(ValidationMultiError):
        trial.update(changes=bad_json)

    # No error on invalid `changes` update if validate_metadata=False
    trial.update(changes=bad_json, validate_metadata=False)

    # Error on invalid attribute update
    trial.metadata_json = bad_json
    with pytest.raises(ValidationMultiError):
        trial.update()

    # No error on invalid attribute update if validate_metadata=False
    trial.update(validate_metadata=False)


@db_test
def test_trial_metadata_patch_manifest(clean_db):
    """Update manifest data in a trial_metadata record"""
    # Add a participant to the trial
    metadata_with_participant = METADATA.copy()
    metadata_with_participant["participants"] = [
        {
            "samples": [],
            "cimac_participant_id": "CTSTP01",
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
def test_trial_metadata_patch_assay(clean_db):
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
def test_partial_patch_trial_metadata(clean_db):
    """Update an existing trial_metadata_record"""
    # Create the initial trial

    clean_db.add(TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA))
    clean_db.commit()

    # Create patch without all required fields (no "participants")
    metadata_patch = {PROTOCOL_ID_FIELD_NAME: TRIAL_ID, "assays": {}}

    # patch it - should be no error/exception
    TrialMetadata._patch_trial_metadata(TRIAL_ID, metadata_patch)


@db_test
def test_trial_metadata_get_summaries(clean_db, monkeypatch):
    """Check that trial data summaries are computed as expected"""
    # Add some trials
    records = [{"fake": "record"}]
    cytof_record_with_output = [{"output_files": {"foo": "bar"}}]
    tm1 = {
        **METADATA,
        # deliberately override METADATA['protocol_identifier']
        "protocol_identifier": "tm1",
        "participants": [{"samples": [1, 2]}, {"samples": [3]}],
        "expected_assays": ["ihc", "olink"],
        "assays": {
            "wes": [
                {"records": records * 6},
                {"records": records * 5},
            ],  # 6 + 5 11 = 7 for wes + 4 for wes_tumor_only
            "rna": [{"records": records * 2}],
            "mif": [
                {"records": records * 3},
                {"records": records},
                {"records": records},
            ],
            "elisa": [{"assay_xlsx": {"number_of_samples": 7}}],
            "nanostring": [
                {"runs": [{"samples": records * 2}]},
                {"runs": [{"samples": records * 1}]},
            ],
            "hande": [{"records": records * 5}],
        },
        "analysis": {
            "wes_analysis": {
                "pair_runs": [
                    # 7 here for wes_assay: t0/1/2, n0/1/2/3
                    {
                        "tumor": {"cimac_id": "t0"},
                        "normal": {"cimac_id": "n0"},
                    },  # no analysis data
                    {
                        "tumor": {"cimac_id": "t1"},
                        "normal": {"cimac_id": "n1"},
                        "report": {"report": "foo"},
                    },
                    {
                        "tumor": {"cimac_id": "t1"},
                        "normal": {"cimac_id": "n2"},
                        "report": {"report": "foo"},
                    },
                    {
                        "tumor": {"cimac_id": "t2"},
                        "normal": {"cimac_id": "n3"},
                        "report": {"report": "foo"},
                    },
                ],
                # these are excluded, so not adding fake assay data
                "excluded_samples": records * 2,
            },
            "wes_tumor_only_analysis": {
                "runs": records * 4,  # need 4
                # these are excluded, so not adding fake assay data
                "excluded_samples": records * 3,
            },
        },
        "clinical_data": {
            "records": [
                {"clinical_file": {"participants": ["a", "b", "c"]}},
                {"clinical_file": {"participants": ["a", "b", "d"]}},
                {"clinical_file": {"participants": ["e", "f", "g"]}},
            ]
        },
    }
    tm2 = {
        **METADATA,
        # deliberately override METADATA['protocol_identifier']
        "protocol_identifier": "tm1",
        "participants": [{"samples": []}],
        "assays": {
            "cytof_10021_9204": [
                {
                    "records": cytof_record_with_output * 2,
                    "excluded_samples": records * 2,
                },
                {"records": records * 2},
                {"records": records},
            ],
            "cytof_e4412": [
                {
                    "participants": [
                        {"samples": records},
                        {"samples": cytof_record_with_output * 5},
                        {"samples": records * 2},
                    ],
                    "excluded_samples": records,
                }
            ],
            "olink": {
                "batches": [
                    {
                        "records": [
                            {"files": {"assay_npx": {"number_of_samples": 2}}},
                            {"files": {"assay_npx": {"number_of_samples": 3}}},
                        ]
                    },
                    {"records": [{"files": {"assay_npx": {"number_of_samples": 3}}}]},
                ]
            },
        },
        "analysis": {
            "rna_analysis": {"level_1": records * 10, "excluded_samples": records * 2},
            "tcr_analysis": {
                "batches": [
                    {"records": records * 4, "excluded_samples": records * 3},
                    {"records": records * 2, "excluded_samples": records * 1},
                ]
            },
        },
    }
    TrialMetadata(trial_id="tm1", metadata_json=tm1).insert(validate_metadata=False)
    TrialMetadata(trial_id="tm2", metadata_json=tm2).insert(validate_metadata=False)

    # Add some files
    for i, (tid, fs) in enumerate([("tm1", 3), ("tm1", 2), ("tm2", 4), ("tm2", 6)]):
        DownloadableFiles(
            trial_id=tid,
            file_size_bytes=fs,
            object_url=str(i),
            facet_group="",
            uploaded_timestamp=datetime.now(),
            upload_type="",
        ).insert()

    sorter = lambda s: s["trial_id"]
    received = sorted(TrialMetadata.get_summaries(), key=sorter)
    expected = sorted(
        [
            {
                "expected_assays": [],
                "cytof": 13.0,
                "olink": 8.0,
                "trial_id": "tm2",
                "file_size_bytes": 10,
                "total_participants": 1,
                "total_samples": 0,
                "clinical_participants": 0.0,
                "rna": 0.0,
                "nanostring": 0.0,
                "elisa": 0.0,
                "h&e": 0.0,
                "mif": 0.0,
                "cytof_analysis": 7.0,
                "rna_level1_analysis": 10.0,
                "tcr_analysis": 6.0,
                "wes_analysis": 0.0,
                "wes_tumor_only_analysis": 0.0,
                "wes": 0.0,
                "wes_tumor_only": 0.0,
                "excluded_samples": {
                    "tcr_analysis": records * 4,
                    "rna_level1_analysis": records * 2,
                    "cytof_analysis": records * 3,
                },
            },
            {
                "expected_assays": ["ihc", "olink"],
                "elisa": 7.0,
                "cytof": 0.0,
                "olink": 0.0,
                "trial_id": "tm1",
                "file_size_bytes": 5,
                "total_participants": 2,
                "total_samples": 3,
                "clinical_participants": 7.0,
                "rna": 2.0,
                "nanostring": 3.0,
                "h&e": 5.0,
                "mif": 5.0,
                "cytof_analysis": 0.0,
                "rna_level1_analysis": 0.0,
                "tcr_analysis": 0.0,
                "wes_analysis": 5.0,
                "wes_tumor_only_analysis": 4.0,
                "wes": 7.0,
                "wes_tumor_only": 4.0,
                "excluded_samples": {
                    "wes_analysis": records * 2,
                    "wes_tumor_only_analysis": records * 3,
                },
            },
        ],
        key=sorter,
    )
    assert received == expected
    assert all("misc_data" not in entry for entry in received)


@db_test
def test_create_assay_upload(clean_db):
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
        UploadJobs.create("wes_bam", EMAIL, gcs_file_map, metadata_patch, gcs_xlsx_uri)
    clean_db.rollback()

    TrialMetadata.create(TRIAL_ID, METADATA)

    new_job = UploadJobs.create(
        "wes_bam", EMAIL, gcs_file_map, metadata_patch, gcs_xlsx_uri
    )
    job = UploadJobs.find_by_id_and_email(new_job.id, PROFILE["email"])
    assert len(new_job.gcs_file_map) == len(job.gcs_file_map)
    assert set(new_job.gcs_file_map) == set(job.gcs_file_map)
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
def test_upload_job_no_file_map(clean_db):
    """Try to create an assay upload"""
    new_user = Users.create(PROFILE)

    metadata_patch = {PROTOCOL_ID_FIELD_NAME: TRIAL_ID}
    gcs_xlsx_uri = "xlsx/assays/wes/12:0:1.5123095"

    TrialMetadata.create(TRIAL_ID, METADATA)

    new_job = UploadJobs.create(
        prism.SUPPORTED_MANIFESTS[0], EMAIL, None, metadata_patch, gcs_xlsx_uri
    )
    assert list(new_job.upload_uris_with_data_uris_with_uuids()) == []

    job = UploadJobs.find_by_id_and_email(new_job.id, PROFILE["email"])
    assert list(job.upload_uris_with_data_uris_with_uuids()) == []


@db_test
def test_assay_upload_merge_extra_metadata(clean_db, monkeypatch):
    """Try to create an assay upload"""
    new_user = Users.create(PROFILE)

    TrialMetadata.create(TRIAL_ID, METADATA)

    assay_upload = UploadJobs.create(
        upload_type="assay_with_extra_md",
        uploader_email=EMAIL,
        gcs_file_map={},
        metadata={
            PROTOCOL_ID_FIELD_NAME: TRIAL_ID,
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
    assay_upload.id = 111
    clean_db.commit()

    custom_extra_md_parse = MagicMock()
    custom_extra_md_parse.side_effect = lambda f: {"extra": f.read().decode()}
    monkeypatch.setattr(
        "cidc_schemas.prism.merger.EXTRA_METADATA_PARSERS",
        {"assay_with_extra_md": custom_extra_md_parse},
    )

    UploadJobs.merge_extra_metadata(
        111,
        {
            "uuid-1": io.BytesIO(b"within extra md file 1"),
            "uuid-2": io.BytesIO(b"within extra md file 2"),
        },
        session=clean_db,
    )

    assert 1 == clean_db.query(UploadJobs).count()
    au = clean_db.query(UploadJobs).first()
    assert "extra" in au.metadata_patch["whatever"]["hierarchy"][0]
    assert "extra" in au.metadata_patch["whatever"]["hierarchy"][1]


@db_test
def test_assay_upload_ingestion_success(clean_db, monkeypatch, caplog):
    """Check that the ingestion success method works as expected"""
    caplog.set_level(logging.DEBUG)

    new_user = Users.create(PROFILE)
    trial = TrialMetadata.create(TRIAL_ID, METADATA)
    assay_upload = UploadJobs.create(
        upload_type="ihc",
        uploader_email=EMAIL,
        gcs_file_map={},
        metadata={PROTOCOL_ID_FIELD_NAME: TRIAL_ID},
        gcs_xlsx_uri="",
        commit=False,
    )

    clean_db.commit()

    # Ensure that success can't be declared from a starting state
    with pytest.raises(Exception, match="current status"):
        assay_upload.ingestion_success(trial)

    # Update assay_upload status to simulate a completed but not ingested upload
    assay_upload.status = UploadJobStatus.UPLOAD_COMPLETED.value
    assay_upload.ingestion_success(trial)

    # Check that status was updated and email wasn't sent by default
    db_record = UploadJobs.find_by_id(assay_upload.id)
    assert db_record.status == UploadJobStatus.MERGE_COMPLETED.value
    assert "Would send email with subject '[UPLOAD SUCCESS]" not in caplog.text

    # Check that email gets sent when specified
    assay_upload.ingestion_success(trial, send_email=True)
    assert "Would send email with subject '[UPLOAD SUCCESS]" in caplog.text


@db_test
def test_create_downloadable_file_from_metadata(clean_db, monkeypatch):
    """Try to create a downloadable file from artifact_core metadata"""
    # fake file metadata
    file_metadata = {
        "object_url": "10021/Patient 1/sample 1/aliquot 1/wes_forward.fastq",
        "file_size_bytes": 1,
        "md5_hash": "hash1234",
        "uploaded_timestamp": datetime.now(),
        "foo": "bar",  # unsupported column - should be filtered
    }
    additional_metadata = {"more": "info"}

    # Mock artifact upload publishing
    publisher = MagicMock()
    monkeypatch.setattr("cidc_api.models.models.publish_artifact_upload", publisher)

    # Create the trial (to avoid violating foreign-key constraint)
    TrialMetadata.create(TRIAL_ID, METADATA)

    # Create files with empty or "null" additional metadata
    for nullish_value in ["null", None, {}]:
        df = DownloadableFiles.create_from_metadata(
            TRIAL_ID, "wes_bam", file_metadata, additional_metadata=nullish_value
        )
        clean_db.refresh(df)
        assert df.additional_metadata == {}

    # Create the file
    DownloadableFiles.create_from_metadata(
        TRIAL_ID, "wes_bam", file_metadata, additional_metadata=additional_metadata
    )

    # Check that we created the file
    new_file = (
        clean_db.query(DownloadableFiles)
        .filter_by(object_url=file_metadata["object_url"])
        .first()
    )
    assert new_file
    del file_metadata["foo"]
    for k in file_metadata.keys():
        assert getattr(new_file, k) == file_metadata[k]
    assert new_file.additional_metadata == additional_metadata

    # Check that no artifact upload event was published
    publisher.assert_not_called()

    # Check that artifact upload publishes
    DownloadableFiles.create_from_metadata(
        TRIAL_ID,
        "wes_bam",
        file_metadata,
        additional_metadata=additional_metadata,
        alert_artifact_upload=True,
    )
    publisher.assert_called_once_with(file_metadata["object_url"])


@db_test
def test_downloadable_files_additional_metadata_default(clean_db):
    TrialMetadata.create(TRIAL_ID, METADATA)
    df = DownloadableFiles(
        trial_id=TRIAL_ID,
        upload_type="wes_bam",
        object_url="10021/Patient 1/sample 1/aliquot 1/wes_forward.fastq",
        file_size_bytes=1,
        md5_hash="hash1234",
        uploaded_timestamp=datetime.now(),
    )

    # Check no value passed
    df.insert()
    assert df.additional_metadata == {}

    for nullish_value in [None, "null", {}]:
        df.additional_metadata = nullish_value
        df.update()
        assert df.additional_metadata == {}

    # Non-nullish value doesn't get overridden
    non_nullish_value = {"foo": "bar"}
    df.additional_metadata = non_nullish_value
    df.update()
    assert df.additional_metadata == non_nullish_value


@db_test
def test_create_downloadable_file_from_blob(clean_db, monkeypatch):
    """Try to create a downloadable file from a GCS blob"""
    fake_blob = MagicMock()
    fake_blob.name = "name"
    fake_blob.md5_hash = "12345"
    fake_blob.crc32c = "54321"
    fake_blob.size = 5
    fake_blob.time_created = datetime.now()

    clean_db.add(
        TrialMetadata(
            trial_id="id",
            metadata_json={
                "protocol_identifier": "id",
                "allowed_collection_event_names": [],
                "allowed_cohort_names": [],
                "participants": [],
            },
        )
    )
    df = DownloadableFiles.create_from_blob(
        "id", "pbmc", "Shipping Manifest", "pbmc/shipping", fake_blob
    )

    # Mock artifact upload publishing
    publisher = MagicMock()
    monkeypatch.setattr("cidc_api.models.models.publish_artifact_upload", publisher)

    # Check that the file was created
    assert 1 == clean_db.query(DownloadableFiles).count()
    df_lookup = DownloadableFiles.find_by_id(df.id)
    assert df_lookup.object_url == fake_blob.name
    assert df_lookup.data_format == "Shipping Manifest"
    assert df_lookup.file_size_bytes == fake_blob.size
    assert df_lookup.md5_hash == fake_blob.md5_hash
    assert df_lookup.crc32c_hash == fake_blob.crc32c

    # uploading second time to check non duplicating entries
    fake_blob.size = 6
    fake_blob.md5_hash = "6"
    df = DownloadableFiles.create_from_blob(
        "id", "pbmc", "Shipping Manifest", "pbmc/shipping", fake_blob
    )

    # Check that the file was created
    assert 1 == clean_db.query(DownloadableFiles).count()
    df_lookup = DownloadableFiles.find_by_id(df.id)
    assert df_lookup.file_size_bytes == 6
    assert df_lookup.md5_hash == "6"

    # Check that no artifact upload event was published
    publisher.assert_not_called()

    # Check that artifact upload publishes
    DownloadableFiles.create_from_blob(
        "id",
        "pbmc",
        "Shipping Manifest",
        "pbmc/shipping",
        fake_blob,
        alert_artifact_upload=True,
    )
    publisher.assert_called_once_with(fake_blob.name)


def test_downloadable_files_data_category_prefix():
    """Check that data_category_prefix's are derived as expected"""
    file_w_category = DownloadableFiles(facet_group="/wes/r1_L.fastq.gz")
    assert file_w_category.data_category_prefix == "WES"

    file_no_category = DownloadableFiles()
    assert file_no_category.data_category_prefix == None


@db_test
def test_downloadable_files_get_related_files(clean_db):
    # Create a trial to avoid constraint errors
    TrialMetadata.create(trial_id=TRIAL_ID, metadata_json=METADATA)

    # Convenience function for building file records
    def create_df(facet_group, additional_metadata={}) -> DownloadableFiles:
        df = DownloadableFiles(
            facet_group=facet_group,
            additional_metadata=additional_metadata,
            trial_id=TRIAL_ID,
            uploaded_timestamp=datetime.now(),
            file_size_bytes=0,
            object_url=facet_group,  # just filler, not relevant to the test
            upload_type="",
        )
        df.insert()
        clean_db.refresh(df)
        return df

    # Set up test data
    cimac_id_1 = "CTTTPPP01.01"
    cimac_id_2 = "CTTTPPP02.01"
    files = [
        create_df(
            "/cytof/normalized_and_debarcoded.fcs", {"some.path.cimac_id": cimac_id_1}
        ),
        create_df(
            "/cytof_analysis/assignment.csv",
            # NOTE: this isn't realistic - assignment files aren't sample-specific - but
            # it serves the purpose of the test.
            {"path.cimac_id": cimac_id_1, "another.path.cimac_id": cimac_id_1},
        ),
        create_df("/cytof_analysis/source.fcs", {"path.to.cimac_id": cimac_id_2}),
        create_df("/cytof_analysis/reports.zip"),
        create_df("/cytof_analysis/analysis.zip"),
        create_df("/wes/r1_L.fastq.gz"),
    ]

    # Based on setup, we expect the following disjoint sets of related files:
    related_file_groups = [
        [files[0], files[1]],
        [files[2]],
        [files[3], files[4]],
        [files[5]],
    ]

    # Check that get_related_files returns what we expect
    for file_group in related_file_groups:
        for file_record in file_group:
            other_ids = [f.id for f in file_group if f.id != file_record.id]
            related_files = file_record.get_related_files()
            assert set([f.id for f in related_files]) == set(other_ids)
            assert len(related_files) == len(other_ids)


def test_with_default_session(cidc_api, clean_db):
    """Test that the with_default_session decorator provides defaults as expected"""

    @with_default_session
    def check_default_session(expected_session_value, session=None):
        assert session == expected_session_value

    with cidc_api.app_context():
        check_default_session(clean_db)
        fake_session = "some other db session"
        check_default_session(fake_session, session=fake_session)


def test_assay_upload_status():
    """Test UploadJobStatus transition validation logic"""
    upload_statuses = [
        UploadJobStatus.UPLOAD_COMPLETED.value,
        UploadJobStatus.UPLOAD_FAILED.value,
    ]
    merge_statuses = [
        UploadJobStatus.MERGE_COMPLETED.value,
        UploadJobStatus.MERGE_FAILED.value,
    ]
    for upload in upload_statuses:
        assert UploadJobStatus.is_valid_transition(UploadJobStatus.STARTED, upload)
        for merge in merge_statuses:
            assert not UploadJobStatus.is_valid_transition(
                UploadJobStatus.STARTED, merge
            )
            for status in [upload, merge]:
                assert not UploadJobStatus.is_valid_transition(
                    status, UploadJobStatus.STARTED
                )
            assert UploadJobStatus.is_valid_transition(upload, merge)
            assert not UploadJobStatus.is_valid_transition(merge, upload)


@db_test
def test_permissions_insert(clean_db, monkeypatch, caplog):
    gcloud_client = mock_gcloud_client(monkeypatch)
    user = Users(email="test@user.com")
    user.insert()
    trial = TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA)
    trial.insert()

    _insert = MagicMock()
    monkeypatch.setattr(CommonColumns, "insert", _insert)

    # if upload_type is invalid
    with pytest.raises(ValueError, match="invalid upload type"):
        Permissions(upload_type="foo", granted_to_user=user.id, trial_id=trial.trial_id)

    # if don't give granted_by_user
    perm = Permissions(
        granted_to_user=user.id, trial_id=trial.trial_id, upload_type="wes_bam"
    )
    with pytest.raises(IntegrityError, match="`granted_by_user` user must be given"):
        perm.insert()
    _insert.assert_not_called()

    # if give bad granted_by_user
    _insert.reset_mock()
    perm = Permissions(
        granted_to_user=user.id,
        trial_id=trial.trial_id,
        upload_type="wes_bam",
        granted_by_user=999999,
    )
    with pytest.raises(IntegrityError, match="`granted_by_user` user must exist"):
        perm.insert()
    _insert.assert_not_called()

    # if give bad granted_to_user
    _insert.reset_mock()
    perm = Permissions(
        granted_to_user=999999,
        trial_id=trial.trial_id,
        upload_type="wes_bam",
        granted_by_user=user.id,
    )
    with pytest.raises(IntegrityError, match="`granted_to_user` user must exist"):
        perm.insert()
    _insert.assert_not_called()

    # This one will work
    _insert.reset_mock()
    perm = Permissions(
        granted_to_user=user.id,
        trial_id=trial.trial_id,
        upload_type="wes_bam",
        granted_by_user=user.id,
    )
    with caplog.at_level(logging.DEBUG):
        perm.insert()
    _insert.assert_called_once()
    assert any(
        log_record.message.strip()
        == f"admin-action: {user.email} gave {user.email} the permission wes_bam on {trial.trial_id}"
        for log_record in caplog.records
    )
    gcloud_client.grant_download_access.assert_called_once()

    # If granting a permission to a "network-viewer", no GCS IAM actions are taken
    _insert.reset_mock()
    gcloud_client.grant_download_access.reset_mock()
    user.role = CIDCRole.NETWORK_VIEWER.value
    user.update()
    perm = Permissions(
        granted_to_user=user.id,
        trial_id=trial.trial_id,
        upload_type="ihc",
        granted_by_user=user.id,
    )
    perm.insert()
    _insert.assert_called_once()
    gcloud_client.grant_download_access.assert_not_called()


@db_test
def test_permissions_broad_perms(clean_db, monkeypatch):
    gcloud_client = mock_gcloud_client(monkeypatch)
    user = Users(email="test@user.com")
    user.insert()
    trial = TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA)
    trial.insert()
    other_trial = TrialMetadata(
        trial_id="other-trial",
        metadata_json={**METADATA, "protocol_identifier": "other-trial"},
    )
    other_trial.insert()
    for ut in ["wes_fastq", "olink"]:
        for tid in [trial.trial_id, other_trial.trial_id]:
            Permissions(
                granted_to_user=user.id,
                trial_id=tid,
                upload_type=ut,
                granted_by_user=user.id,
            ).insert()

    # Can't insert a permission for access to all trials and assays
    with pytest.raises(ValueError, match="must have a trial id or upload type"):
        Permissions(granted_to_user=user.id, granted_by_user=user.id).insert()

    # Inserting a trial-level permission should delete other more specific related perms.
    trial_query = clean_db.query(Permissions).filter(
        Permissions.trial_id == trial.trial_id
    )
    assert trial_query.count() == 2
    Permissions(
        trial_id=trial.trial_id, granted_to_user=user.id, granted_by_user=user.id
    ).insert()
    assert trial_query.count() == 1
    perm = trial_query.one()
    assert perm.trial_id == trial.trial_id
    assert perm.upload_type is None

    # Inserting an upload-level permission should delete other more specific related perms.
    olink_query = clean_db.query(Permissions).filter(Permissions.upload_type == "olink")
    assert olink_query.count() == 1
    assert olink_query.one().trial_id == other_trial.trial_id
    Permissions(
        upload_type="olink", granted_to_user=user.id, granted_by_user=user.id
    ).insert()
    assert olink_query.count() == 1
    perm = olink_query.one()
    assert perm.trial_id is None
    assert perm.upload_type == "olink"

    # Getting perms for a particular user-trial-type returns broader perms
    perm = Permissions.find_for_user_trial_type(user.id, trial.trial_id, "ihc")
    assert perm is not None and perm.upload_type is None
    perm = Permissions.find_for_user_trial_type(user.id, "some random trial", "olink")
    assert perm is not None and perm.trial_id is None


@db_test
def test_permissions_delete(clean_db, monkeypatch, caplog):
    gcloud_client = mock_gcloud_client(monkeypatch)
    user = Users(email="test@user.com")
    user.insert()
    trial = TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA)
    trial.insert()
    perm = Permissions(
        granted_to_user=user.id,
        trial_id=trial.trial_id,
        upload_type="wes_bam",
        granted_by_user=user.id,
    )
    perm.insert()

    # Deleting a record by a user doesn't exist leads to an error
    gcloud_client.reset_mocks()
    with pytest.raises(NoResultFound, match="no user with id"):
        perm.delete(deleted_by=999999)

    # Deletion of an existing permission leads to no error
    gcloud_client.reset_mocks()
    with caplog.at_level(logging.DEBUG):
        perm.delete(deleted_by=user.id)
    gcloud_client.revoke_download_access.assert_called_once()
    gcloud_client.grant_download_access.assert_not_called()
    assert any(
        log_record.message.strip()
        == f"admin-action: {user.email} removed from {user.email} the permission wes_bam on {trial.trial_id}"
        for log_record in caplog.records
    )

    # Deleting an already-deleted record is idempotent
    gcloud_client.reset_mocks()
    perm.delete(deleted_by=user)
    gcloud_client.revoke_download_access.assert_called_once()
    gcloud_client.grant_download_access.assert_not_called()

    # Deleting a record whose user doesn't exist leads to an error
    gcloud_client.reset_mocks()
    with pytest.raises(NoResultFound, match="no user with id"):
        Permissions(granted_to_user=999999).delete(deleted_by=user)

    gcloud_client.revoke_download_access.assert_not_called()
    gcloud_client.grant_download_access.assert_not_called()

    # If revoking a permission from a "network-viewer", no GCS IAM actions are taken
    gcloud_client.revoke_download_access.reset_mock()
    user.role = CIDCRole.NETWORK_VIEWER.value
    user.update()
    perm = Permissions(
        granted_to_user=user.id,
        trial_id=trial.trial_id,
        upload_type="ihc",
        granted_by_user=user.id,
    )
    perm.insert()
    perm.delete(deleted_by=user)
    gcloud_client.revoke_download_access.assert_not_called()


@db_test
def test_permissions_grant_iam_permissions(clean_db, monkeypatch):
    """
    Smoke test that Permissions.grant_iam_permissions calls grant_download_access with the right arguments.
    """
    refresh_intake_access = MagicMock()
    monkeypatch.setattr(
        "cidc_api.models.models.refresh_intake_access", refresh_intake_access
    )

    gcloud_client = mock_gcloud_client(monkeypatch)
    user = Users(email="test@user.com", role=CIDCRole.NETWORK_VIEWER.value)
    user.insert()
    trial = TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA)
    trial.insert()

    upload_types = ["wes_bam", "ihc", "rna_fastq", "plasma"]
    for upload_type in upload_types:
        Permissions(
            granted_to_user=user.id,
            trial_id=trial.trial_id,
            upload_type=upload_type,
            granted_by_user=user.id,
        ).insert()

    # IAM permissions not granted to network viewers
    Permissions.grant_iam_permissions(user=user)
    gcloud_client.grant_download_access.assert_not_called()

    # IAM permissions should be granted for any other role
    user.role = CIDCRole.CIMAC_USER.value
    Permissions.grant_iam_permissions(user=user)
    for upload_type in upload_types:
        assert (
            call(user.email, trial.trial_id, upload_type)
            in gcloud_client.grant_download_access.call_args_list
        )

    refresh_intake_access.assert_called_once_with(user.email)


@db_test
def test_permissions_grant_all_iam_permissions(clean_db, monkeypatch):
    """
    Smoke test that Permissions.grant_all_iam_permissions calls grant_download_access with the right arguments.
    """
    gcloud_client = mock_gcloud_client(monkeypatch)
    user = Users(email="test@user.com")
    user.insert()
    trial = TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA)
    trial.insert()

    upload_types = ["wes_bam", "ihc", "rna_fastq", "plasma"]
    for upload_type in upload_types:
        Permissions(
            granted_to_user=user.id,
            trial_id=trial.trial_id,
            upload_type=upload_type,
            granted_by_user=user.id,
        ).insert()

    Permissions.grant_all_iam_permissions()
    gcloud_client.grant_download_access.assert_has_calls(
        [call(user.email, trial.trial_id, upload_type) for upload_type in upload_types]
    )

    # not called on admins or nci biobank users
    gcloud_client.grant_download_access.reset_mock()
    for role in [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]:
        user.role = role
        user.update()
        Permissions.grant_all_iam_permissions()
        gcloud_client.grant_download_access.assert_not_called()


@db_test
def test_permissions_revoke_all_iam_permissions(clean_db, monkeypatch):
    """
    Smoke test that Permissions.revoke_all_iam_permissions calls revoke_download_access the right arguments.
    """
    gcloud_client = mock_gcloud_client(monkeypatch)
    user = Users(email="test@user.com")
    user.insert()
    trial = TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA)
    trial.insert()

    upload_types = ["wes_bam", "ihc", "rna_fastq", "plasma"]
    for upload_type in upload_types:
        Permissions(
            granted_to_user=user.id,
            trial_id=trial.trial_id,
            upload_type=upload_type,
            granted_by_user=user.id,
        ).insert()

    Permissions.revoke_all_iam_permissions()
    gcloud_client.revoke_download_access.assert_has_calls(
        [call(user.email, trial.trial_id, upload_type) for upload_type in upload_types]
    )

    # not called on admins or nci biobank users
    gcloud_client.revoke_download_access.reset_mock()
    for role in [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]:
        user.role = role
        user.update()
        Permissions.revoke_all_iam_permissions()
        gcloud_client.revoke_download_access.assert_not_called()


@db_test
def test_user_confirm_approval(clean_db, monkeypatch):
    """Ensure that users are notified when their account goes from pending to approved."""
    confirm_account_approval = MagicMock()
    monkeypatch.setattr(
        "cidc_api.shared.emails.confirm_account_approval", confirm_account_approval
    )

    user = Users(email="test@user.com")
    user.insert()

    # The confirmation email shouldn't be sent for updates unrelated to account approval
    user.update(changes={"first_n": "foo"})
    confirm_account_approval.assert_not_called()

    # The confirmation email should be sent for updates related to account approval
    user.update(changes={"approval_date": datetime.now()})
    confirm_account_approval.assert_called_once_with(user, send_email=True)


@db_test
def test_user_get_data_access_report(clean_db, monkeypatch):
    """Test that user data access info is collected as expected"""
    mock_gcloud_client(monkeypatch)

    admin_user = Users(
        email="admin@email.com",
        organization="CIDC",
        approval_date=datetime.now(),
        role=CIDCRole.ADMIN.value,
    )
    admin_user.insert()

    cimac_user = Users(
        email="cimac@email.com",
        organization="DFCI",
        approval_date=datetime.now(),
        role=CIDCRole.CIMAC_USER.value,
    )
    cimac_user.insert()

    trial = TrialMetadata(trial_id=TRIAL_ID, metadata_json=METADATA)
    trial.insert()

    trial2 = TrialMetadata(trial_id=TRIAL_ID + "2", metadata_json=METADATA)
    trial2.insert()

    upload_types = ["wes_bam", "ihc"]

    # Note that admins don't need permissions to view data,
    # so we're deliberately issuing unnecessary permissions here.
    for user in [admin_user, cimac_user]:
        for t in upload_types:
            Permissions(
                granted_to_user=user.id,
                granted_by_user=admin_user.id,
                trial_id=trial.trial_id,
                upload_type=t,
            ).insert()

    # Add a cross-assay permission
    Permissions(
        granted_to_user=cimac_user.id,
        granted_by_user=admin_user.id,
        trial_id=trial2.trial_id,
        upload_type=None,
    ).insert()

    # Add a cross-trial permission as well
    Permissions(
        granted_to_user=cimac_user.id,
        granted_by_user=admin_user.id,
        trial_id=None,
        upload_type="olink",
    ).insert()

    bio = io.BytesIO()
    result_df = Users.get_data_access_report(bio)
    bio.seek(0)

    # Make sure bytes were written to the BytesIO instance
    assert bio.getbuffer().nbytes > 0

    # Make sure report data has expected info
    assert set(result_df.columns) == set(
        ["email", "role", "organization", "trial_id", "permissions"]
    )
    for t in [trial, trial2]:
        for user in [admin_user, cimac_user]:
            user_df = result_df[result_df.trial_id == t.trial_id][
                result_df.email == user.email
            ]
            assert set([user.role]) == set(user_df.role)
            assert set([user.organization]) == set(user_df.organization)
            if user == admin_user:
                assert set(["*"]) == set(user_df.permissions)
            else:
                if t == trial:
                    assert set(user_df.permissions).issubset(
                        ["wes_bam,ihc", "ihc,wes_bam", "olink"]
                    )
                else:
                    assert set(["*"]) == set(user_df.permissions)
