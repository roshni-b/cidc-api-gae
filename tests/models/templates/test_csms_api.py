from cidc_api.models.templates.file_metadata import Upload
from cidc_api.models.templates.trial_metadata import Participant
from datetime import datetime
from collections import OrderedDict
import pytest
from unittest.mock import MagicMock

from cidc_api.models import (
    ClinicalTrial,
    Cohort,
    CollectionEvent,
    insert_record_batch,
    Sample,
    Shipment,
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

        for manifest in manifests:
            if manifest.get("status") not in (None, "qc_complete"):
                continue

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
                    {
                        k: v if k != "cimac_id" else "CXXXP0555.00"
                        for k, v in sample.items()
                    }
                    if n == 0
                    else sample
                    for n, sample in enumerate(manifest["samples"])
                ]
                detect_manifest_changes(new_manifest)

            # Test non-critical changes on the manifest itself
            for key in manifest.keys():
                # ignore list from calc_diff + criticals
                if key in [
                    "barcode",
                    "biobank_id",
                    "manifest_id",
                    "modified_time",
                    "modified_timestamp",
                    "protocol_identifier",
                    "samples",
                    "status",
                    "submitter",
                ]:
                    continue

                new_manifest = {
                    k: v if k != key else "foo" for k, v in manifest.items()
                }
                records, changes = detect_manifest_changes(new_manifest)
                assert (
                    len(records) == 1
                    and Shipment in records
                    and len(records[Shipment]) == 1
                ), f"{records}\n{changes}"
                assert getattr(records[Shipment][0], key) == "foo", (
                    str(records) + "\n" + str(changes)
                )

                assert changes == [
                    {
                        "manifest_id": manifest["manifest_id"],
                        "trial_id": manifest["protocol_identifier"],
                        key: (
                            datetime.strptime(manifest[key], "%Y-%m-%d %H:%M:%S").date()
                            if key.startswith("date")
                            else manifest[key],
                            "foo",
                        ),
                    }
                ], str(changes)

            # Test non-critical changes for the manifest but stored on the samples
            for key in [
                "assay_priority",
                "assay_type",
                "sample_manifest_type",
            ]:
                # ignore list from calc_diff + criticals
                if key in [
                    "barcode",
                    "biobank_id",
                    "manifest_id",
                    "modified_time",
                    "modified_timestamp",
                    "protocol_identifier",
                    "samples",
                    "status",
                    "submitter",
                ]:
                    continue

                new_manifest = {k: v for k, v in manifest.items() if k != "samples"}

                if key == "sample_manifest_type":
                    new_manifest["samples"] = [
                        {k: v for k, v in sample.items()}
                        for sample in manifest["samples"]
                    ]
                    for n in range(len(new_manifest["samples"])):
                        new_manifest["samples"][n].update(
                            {
                                "processed_sample_type": "foo",
                                "sample_manifest_type": "Tissue Scroll",
                                "processed_sample_derivative": "Germline DNA",
                            }
                        )
                else:
                    new_manifest["samples"] = [
                        {k: v if k != key else "foo" for k, v in sample.items()}
                        for sample in manifest["samples"]
                    ]

                records, changes = detect_manifest_changes(new_manifest)

                if key == "sample_manifest_type":
                    assert (
                        len(records) == 2
                        and Sample in records
                        and Upload in records
                        and len(records[Upload]) == 1
                    ), f"{records}\n{changes}"
                else:
                    assert (
                        len(records) == 1
                        and Shipment in records
                        and len(records[Shipment]) == 1
                    ), f"{records}\n{changes}"
                    assert getattr(records[Shipment][0], key) == "foo", (
                        str(records) + "\n" + str(changes)
                    )
                    assert changes == [
                        {
                            "manifest_id": manifest["manifest_id"],
                            "trial_id": manifest["protocol_identifier"],
                            key: (manifest["samples"][0][key], "foo"),
                        }
                    ], str(changes)

            # Test non-critical changes on the samples
            for key in manifest["samples"][0].keys():
                # ignore list from calc_diff + criticals
                if key in [
                    "assay_priority",
                    "assay_type",
                    "barcode",
                    "biobank_id",
                    "cimac_id",
                    "collection_event_name",
                    "entry_number",
                    "manifest_id",
                    "modified_time",
                    "modified_timestamp",
                    "processed_sample_derivative",
                    "processed_sample_type",
                    "protocol_identifier",
                    "qc_comments",
                    "recorded_collection_event_name",
                    "sample_approved",
                    "sample_key",
                    "sample_manifest_type",
                    "samples",
                    "status",
                    "submitter",
                    "trial_participant_id",
                    "type_of_sample",
                ]:
                    continue
                else:
                    print(key)

                new_manifest = {k: v for k, v in manifest.items() if k != "samples"}

                if key in ["sample_derivative_concentration"]:
                    new_manifest["samples"] = [
                        {k: v if k != key else 10 for k, v in sample.items()}
                        if n == 0
                        else sample
                        for n, sample in enumerate(manifest["samples"])
                    ]
                else:
                    new_manifest["samples"] = [
                        {k: v if k != key else "foo" for k, v in sample.items()}
                        if n == 0
                        else sample
                        for n, sample in enumerate(manifest["samples"])
                    ]

                records, changes = detect_manifest_changes(new_manifest)

                # name change for when we're looking below
                if key == "standardized_collection_event_name":
                    key = "collection_event_name"

                if key not in ["cohort_name", "participant_id"]:
                    assert (
                        len(records) == 1
                        and Sample in records
                        and len(records[Sample]) == 1
                    ), f"{records}\n{changes}"
                    assert (
                        getattr(records[Sample][0], key)
                        == new_manifest["samples"][0][key]
                    ), f"{records}\n{changes}"

                elif key == "cohort_name":
                    assert (
                        len(records) == 1
                        and Participant in records
                        and len(records[Participant]) == 1
                    ), f"{records}\n{changes}"
                    assert (
                        getattr(records[Participant][0], key)
                        == new_manifest["samples"][0][key]
                    ), f"{records}\n{changes}"

                else:  # key == "participant_id":
                    assert (
                        len(records) == 1
                        and Participant in records
                        and len(records[Participant]) == 1
                    ), f"{records}\n{changes}"
                    assert (
                        getattr(records[Participant][0], "trial_participant_id")
                        == new_manifest["samples"][0][key]
                    ), f"{records}\n{changes}"

                assert changes == [
                    {
                        "cimac_id": manifest["samples"][0]["cimac_id"],
                        "shipment_manifest_id": manifest["manifest_id"],
                        "trial_id": manifest["protocol_identifier"],
                        key: (
                            manifest["samples"][0][key],
                            new_manifest["samples"][0][key],
                        ),
                    }
                ], str(changes)


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
