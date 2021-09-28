import pytest
from unittest.mock import MagicMock

from cidc_api.models import (
    ClinicalTrial,
    Cohort,
    CollectionEvent,
    insert_record_batch,
    TrialMetadata,
)
from cidc_api.models.templates.csms_api import (
    insert_manifest_from_json,
    insert_manifest_into_blob,
)
from cidc_api.shared import auth

from tests.csms.data import manifests
from tests.csms.utils import validate_json_blob, validate_relational


from ...resources.test_trial_metadata import setup_user


def mock_get_current_user(cidc_api, monkeypatch):
    setup_user(cidc_api, monkeypatch)

    get_current_user = MagicMock()
    get_current_user.return_value = MagicMock()
    get_current_user.return_value.email = "test@email.com"
    monkeypatch.setattr(auth, "get_current_user", get_current_user)


def test_insert_manifest_into_blob(cidc_api, clean_db, monkeypatch):
    """test that insertion of manifest into blob works as expected"""
    # grab a completed manifest
    manifest = [m for m in manifests if m.get("status") in [None, "qc_complete"]][0]

    with cidc_api.app_context():
        mock_get_current_user(cidc_api, monkeypatch)

        # blank db throws error
        with pytest.raises(Exception, match="No trial found with id"):
            insert_manifest_into_blob(manifest)

        # also checks for trial existence in relational
        errs = insert_record_batch(
            {ClinicalTrial: [ClinicalTrial(protocol_identifier="test_trial",)]}
        )
        assert len(errs) == 0

        metadata_json = {
            "protocol_identifier": "test_trial",
            "participants": [],
            "shipments": [],
            "allowed_cohort_names": [],
            "allowed_collection_event_names": [],
        }
        TrialMetadata(trial_id="test_trial", metadata_json=metadata_json,).insert()

        with pytest.raises(Exception, match="not found within '/allowed_cohort_names/"):
            insert_manifest_into_blob(manifest)

        metadata_json["allowed_cohort_names"] = ["Arm_A", "Arm_Z"]
        TrialMetadata.select_for_update_by_trial_id("test_trial").update(
            changes={"metadata_json": metadata_json}
        )

        with pytest.raises(
            Exception, match="not found within '/allowed_collection_event_names/"
        ):
            insert_manifest_into_blob(manifest)

        metadata_json["allowed_collection_event_names"] = [
            "Baseline",
            "Pre_Day_1_Cycle_2",
        ]
        TrialMetadata.select_for_update_by_trial_id("test_trial").update(
            changes={"metadata_json": metadata_json}
        )

        insert_manifest_into_blob(manifest)

        md_json = TrialMetadata.select_for_update_by_trial_id(
            "test_trial"
        ).metadata_json
        validate_json_blob(md_json)

        for other_manifest in [
            m
            for m in manifests
            if m.get("status") in [None, "qc_complete"]
            if m != manifest
        ]:
            insert_manifest_into_blob(other_manifest)

            md_json = TrialMetadata.select_for_update_by_trial_id(
                "test_trial"
            ).metadata_json
            validate_json_blob(md_json)

        with pytest.raises(Exception, match="already exists for trial"):
            insert_manifest_into_blob(manifest)


def test_insert_manifest_from_json(cidc_api, clean_db, monkeypatch):
    """test that insertion of manifest from json works as expected"""
    # grab a completed manifest
    manifest = [m for m in manifests if m.get("status") in [None, "qc_complete"]][0]

    with cidc_api.app_context():
        mock_get_current_user(cidc_api, monkeypatch)

        # blank db throws error
        with pytest.raises(Exception, match="No trial found with id"):
            insert_manifest_from_json(manifest)

        errs = insert_record_batch(
            {ClinicalTrial: [ClinicalTrial(protocol_identifier="test_trial",)]}
        )
        assert len(errs) == 0

        # also checks for trial existence in JSON blobs
        metadata_json = {
            "protocol_identifier": "test_trial",
            "participants": [],
            "shipments": [],
            "allowed_cohort_names": [],
            "allowed_collection_event_names": [],
        }
        TrialMetadata(trial_id="test_trial", metadata_json=metadata_json,).insert()

        with pytest.raises(
            Exception, match="No Collection event with trial_id, event_name"
        ):
            insert_manifest_from_json(manifest)

        errs = insert_record_batch(
            {
                CollectionEvent: [
                    CollectionEvent(trial_id="test_trial", event_name="Baseline"),
                    CollectionEvent(
                        trial_id="test_trial", event_name="Pre_Day_1_Cycle_2"
                    ),
                    CollectionEvent(trial_id="test_trial", event_name="On_Treatment"),
                ]
            }
        )
        assert len(errs) == 0, errs

        with pytest.raises(Exception, match="no Cohort with trial_id, cohort_name"):
            insert_manifest_from_json(manifest)

        errs = insert_record_batch(
            {
                Cohort: [
                    Cohort(trial_id="test_trial", cohort_name="Arm_A"),
                    Cohort(trial_id="test_trial", cohort_name="Arm_Z"),
                ]
            }
        )
        assert len(errs) == 0

        insert_manifest_from_json(manifest)
        validate_relational("test_trial")

        for other_manifest in [
            m
            for m in manifests
            if m.get("status") in [None, "qc_complete"] and m != manifest
        ]:
            insert_manifest_from_json(other_manifest)
            validate_relational("test_trial")

        with pytest.raises(Exception, match="already exists for trial"):
            insert_manifest_from_json(manifest)
