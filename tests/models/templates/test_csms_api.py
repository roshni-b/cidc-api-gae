from re import match
from cidc_api.models.templates.trial_metadata import Sample, Shipment
from collections import OrderedDict
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
    detect_manifest_changes,
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


def test_detect_manifest_changes(cidc_api, clean_db, monkeypatch):
    """test that detecting changes in a manifest work as expected"""
    # grab a completed manifest
    manifest = [m for m in manifests if m.get("status") in [None, "qc_complete"]][0]

    with cidc_api.app_context():
        mock_get_current_user(cidc_api, monkeypatch)

        # prepare relational db
        ordered_records = OrderedDict()
        ordered_records[ClinicalTrial] = [
            ClinicalTrial(protocol_identifier="test_trial"),
            ClinicalTrial(protocol_identifier="foo"),  # need a second valid trial
        ]
        ordered_records[CollectionEvent] = [
            CollectionEvent(trial_id="test_trial", event_name="Baseline"),
            CollectionEvent(trial_id="test_trial", event_name="Pre_Day_1_Cycle_2"),
            CollectionEvent(trial_id="test_trial", event_name="On_Treatment"),
        ]
        ordered_records[Cohort] = [
            Cohort(trial_id="test_trial", cohort_name="Arm_A"),
            Cohort(trial_id="test_trial", cohort_name="Arm_Z"),
        ]
        errs = insert_record_batch(ordered_records)
        assert len(errs) == 0

        # also checks for trial existence in JSON blobs
        metadata_json = {
            "protocol_identifier": "test_trial",
            "participants": [],
            "shipments": [],
            "allowed_cohort_names": [],
            "allowed_collection_event_names": [],
        }
        TrialMetadata(trial_id="test_trial", metadata_json=metadata_json).insert()
        # need a second valid trial
        metadata_json["protocol_identifier"] = "foo"
        TrialMetadata(trial_id="foo", metadata_json=metadata_json).insert()

        # insert manifest before we check for changes
        insert_manifest_from_json(manifest)
        # should check out, but let's make sure
        validate_relational("test_trial")

        # Test critical changes throws Exception on samples
        # Change trial_id or manifest_id is adding a new Shipment
        ## but this means they'll conflict on the sample
        # a bad ID raises a no trial found like insert_manifest_...
        with pytest.raises(Exception, match="No trial found with id"):
            new_manifest = {k: v for k, v in manifest.items() if k != "samples"}
            new_manifest["samples"] = [
                {
                    k: v if k != "protocol_identifier" else "bar"
                    for k, v in sample.items()
                }
                for sample in manifest["samples"]
            ]
            detect_manifest_changes(new_manifest)
        # this is why we needed a second valid trial to test this check
        with pytest.raises(Exception, match="Change in critical field for"):
            # CIDC trial_id = CSMS protocol_identifier
            # stored on samples, not manifest
            new_manifest = {k: v for k, v in manifest.items() if k != "samples"}
            new_manifest["samples"] = [
                {
                    k: v if k != "protocol_identifier" else "foo"
                    for k, v in sample.items()
                }
                for sample in manifest["samples"]
            ]
            detect_manifest_changes(new_manifest)

        # manifest_id has no such complication, but is also on the samples
        with pytest.raises(Exception, match="Change in critical field for"):
            new_manifest = {
                k: v if k != "manifest_id" else "foo" for k, v in manifest.items()
            }
            new_manifest["samples"] = [
                {k: v if k != "manifest_id" else "foo" for k, v in sample.items()}
                for sample in new_manifest["samples"]
            ]
            detect_manifest_changes(new_manifest)

        # Changing a cimac_id is adding/removing a Sample
        ## so this is a different error
        with pytest.raises(Exception, match="Malformatted cimac_id"):
            new_manifest = {k: v for k, v in manifest.items()}
            new_manifest["samples"] = [
                {k: v if k != "cimac_id" else "foo" for k, v in sample.items()}
                if n == 0
                else sample
                for n, sample in enumerate(manifest["samples"])
            ]
            detect_manifest_changes(new_manifest)
        # need to use an actually valid cimac_id
        with pytest.raises(Exception, match="Missing sample"):
            new_manifest = {k: v for k, v in manifest.items() if k != "samples"}
            new_manifest["samples"] = [
                {k: v if k != "cimac_id" else "CXXXP0555.00" for k, v in sample.items()}
                if n == 0
                else sample
                for n, sample in enumerate(manifest["samples"])
            ]
            detect_manifest_changes(new_manifest)

        # Test non-critical changes on the manifest itself
        for key in manifest.keys():
            # don't change these here
            if key in ["manifest_id", "samples", "status", "trial_id"]:
                continue

            new_manifest = {k: v if k != key else "foo" for k, v in manifest.items()}
            print(new_manifest)
            print()
            records, changes = detect_manifest_changes(new_manifest)
            print(records)
            print(changes)
            assert (
                len(records) == 1
                and Shipment in records
                and len(records[Shipment]) == 1
            )
            assert getattr(records[Shipment][0], key) == "foo"

            assert changes == [
                {
                    "manifest_id": manifest["manifest_id"],
                    "trial_id": manifest["trial_id"],
                    key: (manifest[key], "foo"),
                }
            ]

        # Test non-critical changes on the samples
        new_manifest = {k: v for k, v in manifest.items() if k != "samples"}
        for key in manifest["samples"][0].keys():
            # don't change these here
            if key in ["cimac_id", "manifest_id", "status", "trial_id"]:
                continue

            new_manifest["samples"] = [
                {k: v if k != key else "foo" for k, v in sample.items()}
                if n == 0
                else sample
                for n, sample in enumerate(manifest["samples"])
            ]

            records, changes = detect_manifest_changes(new_manifest)
            assert len(records) == 1 and Sample in records and len(records[Sample]) == 1
            assert getattr(records[Sample][0], key) == "foo"

            assert changes == [
                {
                    "cimac_id": manifest["samples"][0]["cimac_id"],
                    "manifest_id": manifest["manifest_id"],
                    "trial_id": manifest["trial_id"],
                    key: (manifest[key], "foo"),
                }
            ]


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
