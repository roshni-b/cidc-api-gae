from datetime import datetime
from typing import Tuple

from cidc_api.resources.trial_metadata import trial_modifier_roles
from cidc_api.models import Users, TrialMetadata, TrialMetadataSchema, CIDCRole

from ..utils import mock_current_user, make_role


def setup_user(cidc_api, monkeypatch) -> int:
    current_user = Users(
        email="test@email.com",
        role=CIDCRole.CIMAC_USER.value,
        approval_date=datetime.now(),
    )
    mock_current_user(current_user, monkeypatch)

    with cidc_api.app_context():
        current_user.insert()
        return current_user.id


def setup_trial_metadata(cidc_api) -> Tuple[int, int]:
    """Insert two trials into the database and return their IDs."""

    def create_trial(n):
        trial_id = f"test-trial-{n}"
        metadata_json = {
            "protocol_identifier": trial_id,
            "participants": [],
            "allowed_collection_event_names": [],
            "allowed_cohort_names": [],
        }
        trial = TrialMetadata(trial_id=trial_id, metadata_json=metadata_json)
        trial.insert()
        return trial.id

    with cidc_api.app_context():
        return create_trial(1), create_trial(2)


def test_list_trials(cidc_api, clean_db, monkeypatch):
    """Check that listing trials works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    trial_ids = set(setup_trial_metadata(cidc_api))

    client = cidc_api.test_client()

    # Non-admins can't list trials
    res = client.get("/trial_metadata")
    assert res.status_code == 401

    # Allowed users can get all trials
    for role in trial_modifier_roles:
        make_role(user_id, role, cidc_api)

        res = client.get("/trial_metadata")
        assert res.status_code == 200
        assert len(res.json["_items"]) == 2
        assert res.json["_meta"]["total"] == 2
        assert set([t["id"] for t in res.json["_items"]]) == trial_ids


def test_get_trial(cidc_api, clean_db, monkeypatch):
    """Check that getting a single trial works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    trial_record_id, _ = set(setup_trial_metadata(cidc_api))
    with cidc_api.app_context():
        trial = TrialMetadata.find_by_id(trial_record_id)

    client = cidc_api.test_client()

    # Non-admins can't get single trials
    res = client.get(f"/trial_metadata/{trial.trial_id}")
    assert res.status_code == 401

    # Allowed users can get single trials
    for role in trial_modifier_roles:
        make_role(user_id, role, cidc_api)
        res = client.get(f"/trial_metadata/{trial.trial_id}")
        assert res.status_code == 200
        assert res.json == TrialMetadataSchema().dump(trial)

        # Getting non-existent trials yields 404
        res = client.get(f"/trial_metadata/123212321")
        assert res.status_code == 404


def test_get_trial_by_trial_id(cidc_api, clean_db, monkeypatch):
    """Check that getting a single trial by trial id works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    trial_id, _ = set(setup_trial_metadata(cidc_api))
    with cidc_api.app_context():
        trial = TrialMetadata.find_by_id(trial_id)

    client = cidc_api.test_client()

    # Non-admins can't get single trials
    res = client.get(f"/trial_metadata/{trial.trial_id}")
    assert res.status_code == 401

    # Allowed users can get single trials
    for role in trial_modifier_roles:
        make_role(user_id, role, cidc_api)
        res = client.get(f"/trial_metadata/{trial.trial_id}")
        assert res.status_code == 200
        assert res.json == TrialMetadataSchema().dump(trial)

        # Getting non-existent trials yields 404
        res = client.get(f"/trial_metadata/foobar")
        assert res.status_code == 404


bad_trial_json = {"trial_id": "foo", "metadata_json": {"foo": "bar"}}
bad_trial_error_message = {
    "errors": [
        "'metadata_json': Additional properties are not allowed ('foo' was unexpected)",
        "'metadata_json': 'protocol_identifier' is a required property",
        "'metadata_json': 'participants' is a required property",
        "'metadata_json': 'allowed_cohort_names' is a required property",
        "'metadata_json': 'allowed_collection_event_names' is a required property",
    ]
}


def test_create_trial(cidc_api, clean_db, monkeypatch):
    """Check that creating a new trial works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    trial_id = "test-trial"
    trial_json = {
        "trial_id": trial_id,
        "metadata_json": {
            "protocol_identifier": trial_id,
            "participants": [],
            "allowed_collection_event_names": [],
            "allowed_cohort_names": [],
        },
    }

    client = cidc_api.test_client()

    # Non-admins can't create trials
    res = client.post("/trial_metadata", json=trial_json)
    assert res.status_code == 401

    # Allowed users can create trials
    for role in trial_modifier_roles:
        make_role(user_id, role, cidc_api)
        res = client.post("/trial_metadata", json=trial_json)
        assert res.status_code == 201
        assert {**res.json, **trial_json} == res.json

        # No two trials can have the same trial_id
        res = client.post("/trial_metadata", json=trial_json)
        assert res.status_code == 400

        # No trial can be created with invalid metadata
        bad_trial_json = {"trial_id": "foo", "metadata_json": {"foo": "bar"}}
        res = client.post("/trial_metadata", json=bad_trial_json)
        assert res.status_code == 422
        assert res.json["_error"]["message"] == bad_trial_error_message

        # Clear created trial
        with cidc_api.app_context():
            trial = TrialMetadata.find_by_trial_id(trial_id)
            trial.delete()


def test_update_trial(cidc_api, clean_db, monkeypatch):
    """Check that updating a trial works as expected"""
    user_id = setup_user(cidc_api, monkeypatch)
    trial_record_id, _ = set(setup_trial_metadata(cidc_api))
    with cidc_api.app_context():
        trial = TrialMetadata.find_by_id(trial_record_id)

    client = cidc_api.test_client()

    # Non-admins can't update single trials
    res = client.patch(f"/trial_metadata/{trial.trial_id}")
    assert res.status_code == 401

    for role in trial_modifier_roles:
        make_role(user_id, role, cidc_api)

        # A missing ETag blocks an update
        res = client.patch(f"/trial_metadata/{trial.trial_id}")
        assert res.status_code == 428

        # An incorrect ETag blocks an update
        res = client.patch(
            f"/trial_metadata/{trial.trial_id}", headers={"If-Match": "foo"}
        )
        assert res.status_code == 412

        # No trial can be updated to have invalid metadata
        res = client.patch(
            f"/trial_metadata/{trial.trial_id}",
            headers={"If-Match": trial._etag},
            json={"metadata_json": bad_trial_json["metadata_json"]},
        )
        assert res.status_code == 422
        assert res.json["_error"]["message"] == bad_trial_error_message

        # An admin can successfully update a trial
        new_metadata_json = {
            **trial.metadata_json,
            "allowed_collection_event_names": ["bazz"],
            "allowed_cohort_names": ["buzz"],
        }
        res = client.patch(
            f"/trial_metadata/{trial.trial_id}",
            headers={"If-Match": trial._etag},
            json={"metadata_json": new_metadata_json},
        )
        assert res.status_code == 200
        assert res.json["id"] == trial.id
        assert res.json["trial_id"] == trial.trial_id
        assert res.json["metadata_json"] == new_metadata_json

        with cidc_api.app_context():
            trial = TrialMetadata.find_by_id(trial.id)
