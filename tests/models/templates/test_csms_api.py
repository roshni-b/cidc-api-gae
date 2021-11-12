import os

os.environ["TZ"] = "UTC"
from copy import deepcopy
from datetime import datetime
from collections import OrderedDict
import pytest

from cidc_api.models import (
    ClinicalTrial,
    Cohort,
    CollectionEvent,
    insert_record_batch,
    Sample,
    Shipment,
    TrialMetadata,
)
from cidc_api.models.templates.csms_api import *
from cidc_api.models.templates.file_metadata import Upload
from cidc_api.models.templates.trial_metadata import Participant

from ...csms.data import manifests
from ...csms.utils import validate_json_blob, validate_relational

from ...resources.test_trial_metadata import setup_user


def manifest_change_setup(cidc_api, monkeypatch):
    setup_user(cidc_api, monkeypatch)

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
        "allowed_cohort_names": ["Arm_A", "Arm_Z"],
        "allowed_collection_event_names": [
            "Baseline",
            "Pre_Day_1_Cycle_2",
            "On_Treatment",
        ],
    }
    TrialMetadata(trial_id="test_trial", metadata_json=metadata_json).insert()
    # need a second valid trial
    metadata_json["protocol_identifier"] = "foo"
    TrialMetadata(trial_id="foo", metadata_json=metadata_json).insert()

    for manifest in manifests:
        if manifest.get("status") not in (None, "qc_complete") or manifest.get(
            "excluded"
        ):
            continue

        # insert manifest before we check for changes
        insert_manifest_from_json(deepcopy(manifest), uploader_email="test@email.com")
        insert_manifest_into_blob(deepcopy(manifest), uploader_email="test@email.com")
        # should check out, but let's make sure
        validate_json_blob(
            TrialMetadata.select_for_update_by_trial_id("test_trial").metadata_json
        )
        validate_relational("test_trial")


def test_detect_changes_when_excluded(cidc_api, clean_db, monkeypatch):
    with cidc_api.app_context():
        manifest_change_setup(cidc_api, monkeypatch)
        manifest = [m for m in manifests if m.get("excluded")][0]

        assert detect_manifest_changes(manifest, uploader_email="test@email.com") == (
            {},
            [],
        )


def test_change_protocol_identifier_error(cidc_api, clean_db, monkeypatch):
    with cidc_api.app_context():
        manifest_change_setup(cidc_api, monkeypatch)
        for manifest in manifests:
            if manifest.get("status") not in (None, "qc_complete") or manifest.get(
                "excluded"
            ):
                continue

            # Test critical changes throws Exception on samples
            # Change trial_id or manifest_id is adding a new Shipment
            ## but this means they'll conflict on the sample
            # a bad ID raises a no trial found like insert_manifest_...
            with pytest.raises(Exception, match="No trial found with id"):
                new_manifest = deepcopy(manifest)
                new_manifest["samples"] = [
                    {
                        k: v if k != "protocol_identifier" else "bar"
                        for k, v in sample.items()
                    }
                    for sample in new_manifest["samples"]
                ]
                detect_manifest_changes(new_manifest, uploader_email="test@email.com")
            # this is why we needed a second valid trial to test this check
            with pytest.raises(Exception, match="Change in critical field for"):
                # CIDC trial_id = CSMS protocol_identifier
                # stored on samples, not manifest
                new_manifest = deepcopy(manifest)
                new_manifest["samples"] = [
                    {
                        k: v if k != "protocol_identifier" else "foo"
                        for k, v in sample.items()
                    }
                    for sample in new_manifest["samples"]
                ]
                detect_manifest_changes(new_manifest, uploader_email="test@email.com")


def test_change_manifest_id_error(cidc_api, clean_db, monkeypatch):
    with cidc_api.app_context():
        manifest_change_setup(cidc_api, monkeypatch)
        for n, manifest in enumerate(manifests):
            if manifest.get("status") not in (None, "qc_complete") or manifest.get(
                "excluded"
            ):
                continue

            # manifest_id has no such complication, but is also on the samples
            # changing the manifest_id makes it new
            with pytest.raises(NewManifestError):
                new_manifest = deepcopy(manifest)
                new_manifest["manifest_id"] = "foo"
                new_manifest["samples"] = [
                    {k: v if k != "manifest_id" else "foo" for k, v in sample.items()}
                    for sample in new_manifest["samples"]
                ]
                detect_manifest_changes(new_manifest, uploader_email="test@email.com")

            # make sure that you can then insert this manifest afterwards
            if n == 0:
                insert_manifest_from_json(new_manifest)
                insert_manifest_into_blob(new_manifest)


def test_change_cimac_id_error(cidc_api, clean_db, monkeypatch):
    with cidc_api.app_context():
        manifest_change_setup(cidc_api, monkeypatch)
        for manifest in manifests:
            if manifest.get("status") not in (None, "qc_complete") or manifest.get(
                "excluded"
            ):
                continue

            # Changing a cimac_id is adding/removing a Sample
            ## so this is a different error
            with pytest.raises(Exception, match="Malformatted cimac_id"):
                new_manifest = deepcopy(manifest)
                new_manifest["samples"] = [
                    {k: v if k != "cimac_id" else "foo" for k, v in sample.items()}
                    if n == 0
                    else sample
                    for n, sample in enumerate(new_manifest["samples"])
                ]
                detect_manifest_changes(new_manifest, uploader_email="test@email.com")
            # need to use an actually valid cimac_id
            with pytest.raises(Exception, match="Missing sample"):
                new_manifest = deepcopy(manifest)
                new_manifest["samples"] = [
                    {
                        k: v if k != "cimac_id" else "CXXXP0555.00"
                        for k, v in sample.items()
                    }
                    if n == 0
                    else sample
                    for n, sample in enumerate(new_manifest["samples"])
                ]
                detect_manifest_changes(new_manifest, uploader_email="test@email.com")


def test_manifest_non_critical_changes(cidc_api, clean_db, monkeypatch):
    with cidc_api.app_context():
        manifest_change_setup(cidc_api, monkeypatch)
        # Test non-critical changes on the manifest itself
        keys = {k for manifest in manifests for k in manifest.keys()}
        for key in keys:
            if key in [
                # changing manifest_id would throw NewManifestError
                "manifest_id",
                # ignored by _calc_differences
                "barcode",
                "biobank_id",
                "entry_number",
                "excluded",
                "json_data",
                "modified_time",
                "modified_timestamp",
                "qc_comments",
                "sample_approved",
                "sample_manifest_type",
                "samples",
                "status",
                "submitter",
                # ignore ignored CSMS fields
                "submitter",
                "reason",
                "event",
                "study_encoding",
                "status_log",
            ]:
                continue

            # grab a completed manifest
            for manifest in manifests:
                if (
                    manifest.get("status") not in (None, "qc_complete")
                    or manifest.get("excluded")
                    or key not in manifest
                ):
                    continue

                new_manifest = deepcopy(manifest)
                new_manifest[key] = "foo"
                records, changes = detect_manifest_changes(
                    new_manifest, uploader_email="test@email.com"
                )
                assert (
                    len(records) == 1
                    and Shipment in records
                    and len(records[Shipment]) == 1
                ), f"{key}: {records}\n{changes}"
                assert getattr(records[Shipment][0], key) == "foo", (
                    str(records) + "\n" + str(changes)
                )
                if key not in [
                    "cimac_id",
                    "cimac_participant_id",
                    "cohort_name",
                    "collection_event_name",
                    "manifest_id",
                    "json_data",
                    "trial_id",
                ]:
                    assert records[Shipment][0].json_data[key] == "foo", (
                        str(records) + "\n" + str(changes)
                    )

                assert len(changes) == 1 and changes[0] == Change(
                    entity_type="shipment",
                    manifest_id=manifest["manifest_id"],
                    trial_id=manifest["samples"][0]["protocol_identifier"],
                    changes={
                        key: (
                            datetime.strptime(manifest[key], "%Y-%m-%d %H:%M:%S").date()
                            if key.startswith("date")
                            else manifest[key],
                            "foo",
                        )
                    },
                ), str(changes)


def test_manifest_non_critical_changes_on_samples(cidc_api, clean_db, monkeypatch):
    with cidc_api.app_context():
        manifest_change_setup(cidc_api, monkeypatch)
        # grab a completed manifest
        for manifest in manifests:
            if manifest.get("status") not in (None, "qc_complete") or manifest.get(
                "excluded"
            ):
                continue
            # Test non-critical changes for the manifest but stored on the samples
            for key in ["assay_priority", "assay_type", "sample_manifest_type"]:
                if key not in manifest["samples"][0]:
                    continue

                new_manifest = deepcopy(manifest)

                if key == "sample_manifest_type":
                    new_manifest["samples"] = [
                        {k: v for k, v in sample.items()}
                        for sample in new_manifest["samples"]
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
                        for sample in new_manifest["samples"]
                    ]

                records, changes = detect_manifest_changes(
                    new_manifest, uploader_email="test@email.com"
                )

                if key == "sample_manifest_type":
                    assert (
                        len(records) == 2
                        and Sample in records
                        and Upload in records
                        and len(records[Upload]) == 1
                    ), f"{key}: {records}\n{changes}"
                else:
                    assert (
                        len(records) == 1
                        and Shipment in records
                        and len(records[Shipment]) == 1
                    ), f"{key}: {records}\n{changes}"
                    assert getattr(records[Shipment][0], key) == "foo", (
                        str(records) + "\n" + str(changes)
                    )
                    if key not in [
                        "cimac_id",
                        "cimac_participant_id",
                        "cohort_name",
                        "collection_event_name",
                        "manifest_id",
                        "json_data",
                        "trial_id",
                    ]:
                        assert records[Shipment][0].json_data[key] == "foo", (
                            str(records) + "\n" + str(changes)
                        )
                    assert len(changes) == 1 and changes[0] == Change(
                        entity_type="shipment",
                        manifest_id=manifest["manifest_id"],
                        trial_id=manifest["samples"][0]["protocol_identifier"],
                        changes={key: (manifest["samples"][0][key], "foo")},
                    ), str(changes)


def test_sample_non_critical_changes(cidc_api, clean_db, monkeypatch):
    with cidc_api.app_context():
        manifest_change_setup(cidc_api, monkeypatch)
        # grab a completed manifest
        for manifest in manifests:
            if manifest.get("status") not in (None, "qc_complete") or manifest.get(
                "excluded"
            ):
                continue
            # Test non-critical changes on the samples
            for key in manifest["samples"][0].keys():
                if key in [
                    # ignore critical changes
                    "cimac_id",
                    "collection_event_name",
                    "manifest_id",
                    "protocol_identifier",
                    "recorded_collection_event_name",
                    "sample_key",
                    # ignore non-sample level changes
                    # see test_manifest_non_critical_changes_on_samples
                    "assay_priority",
                    "assay_type",
                    *manifest,
                    "processed_sample_derivative",
                    "processed_sample_type",
                    "receiving_party",
                    "trial_participant_id",
                    "type_of_sample",
                    # ignore list from calc_diff
                    "barcode",
                    "biobank_id",
                    "entry_number",
                    "event",
                    "excluded",
                    "json_data",
                    "modified_time",
                    "modified_timestamp",
                    "qc_comments",
                    "reason",
                    "sample_approved",
                    "sample_manifest_type",
                    "samples",
                    "status",
                    "status_log",
                    "study_encoding",
                    "submitter",
                ]:
                    continue

                new_manifest = deepcopy(manifest)

                if key in ["sample_derivative_concentration"]:
                    new_manifest["samples"] = [
                        {k: v if k != key else 10 for k, v in sample.items()}
                        if n == 0
                        else sample
                        for n, sample in enumerate(new_manifest["samples"])
                    ]
                else:
                    new_manifest["samples"] = [
                        {k: v if k != key else "foo" for k, v in sample.items()}
                        if n == 0
                        else sample
                        for n, sample in enumerate(new_manifest["samples"])
                    ]

                records, changes = detect_manifest_changes(
                    new_manifest, uploader_email="test@email.com"
                )

                # name change for when we're looking below
                if key == "standardized_collection_event_name":
                    key = "collection_event_name"
                elif key == "fixation_or_stabilization_type":
                    key = "fixation_stabilization_type"

                if key not in ["cohort_name", "participant_id"]:
                    if not len(records) == 1:
                        print(key)
                        print(manifest)
                        print(new_manifest)
                    assert (
                        len(records) == 1
                        and Sample in records
                        and len(records[Sample]) == 1
                    ), f"{records}\n{changes}"
                    assert (
                        getattr(records[Sample][0], key)
                        == new_manifest["samples"][0][key]
                    ), f"{records}\n{changes}"

                    if key not in [
                        "cimac_id",
                        "cimac_participant_id",
                        "collection_event_name",
                        "manifest_id",
                        "json_data",
                        "trial_id",
                    ]:
                        assert (
                            key in records[Sample][0].json_data
                            and records[Sample][0].json_data[key]
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
                    assert (
                        "trial_participant_id" in records[Participant][0]
                        and records[Participant][0]["trial_participant_id"]
                        == new_manifest["samples"][0][key]
                    ), f"{records}\n{changes}"

                assert len(changes) == 1 and changes[0] == Change(
                    entity_type="sample",
                    manifest_id=manifest["manifest_id"],
                    cimac_id=manifest["samples"][0]["cimac_id"],
                    trial_id=manifest["samples"][0]["protocol_identifier"],
                    changes={
                        key: (
                            type(changes[0].changes[key][0])(
                                manifest["samples"][0][
                                    "standardized_collection_event_name"
                                    if key == "collection_event_name"
                                    and "standardized_collection_event_name"
                                    in manifest["samples"][0]
                                    else (
                                        "fixation_stabilization_type"
                                        if key == "fixation_stabilization_type"
                                        else key
                                    )
                                ]
                            ),
                            new_manifest["samples"][0][key],
                        )
                    },
                ), str(changes)


def test_insert_manifest_into_blob(cidc_api, clean_db, monkeypatch):
    """test that insertion of manifest into blob works as expected"""
    # grab a completed manifest
    manifest = [
        m
        for m in manifests
        if m.get("status") in (None, "qc_complete") and not m.get("excluded")
    ][0]

    with cidc_api.app_context():
        setup_user(cidc_api, monkeypatch)

        # blank db throws error
        with pytest.raises(Exception, match="No trial found with id"):
            insert_manifest_into_blob(manifest, uploader_email="test@email.com")

        # also checks for trial existence in relational
        errs = insert_record_batch(
            {ClinicalTrial: [ClinicalTrial(protocol_identifier="test_trial")]}
        )
        assert len(errs) == 0

        metadata_json = {
            "protocol_identifier": "test_trial",
            "participants": [],
            "shipments": [],
            "allowed_cohort_names": [],
            "allowed_collection_event_names": [],
        }
        TrialMetadata(trial_id="test_trial", metadata_json=metadata_json).insert()

        with pytest.raises(Exception, match="not found within '/allowed_cohort_names/"):
            insert_manifest_into_blob(manifest, uploader_email="test@email.com")

        metadata_json["allowed_cohort_names"] = ["Arm_A", "Arm_Z"]
        TrialMetadata.select_for_update_by_trial_id("test_trial").update(
            changes={"metadata_json": metadata_json}
        )

        with pytest.raises(
            Exception, match="not found within '/allowed_collection_event_names/"
        ):
            insert_manifest_into_blob(manifest, uploader_email="test@email.com")

        metadata_json["allowed_collection_event_names"] = [
            "Baseline",
            "Pre_Day_1_Cycle_2",
        ]
        TrialMetadata.select_for_update_by_trial_id("test_trial").update(
            changes={"metadata_json": metadata_json}
        )

        insert_manifest_into_blob(manifest, uploader_email="test@email.com")

        md_json = TrialMetadata.select_for_update_by_trial_id(
            "test_trial"
        ).metadata_json
        validate_json_blob(md_json)

        for other_manifest in [
            m
            for m in manifests
            if m.get("status") in [None, "qc_complete"] and not m.get("excluded")
            if m != manifest
        ]:
            insert_manifest_into_blob(other_manifest, uploader_email="test@email.com")

            md_json = TrialMetadata.select_for_update_by_trial_id(
                "test_trial"
            ).metadata_json
            validate_json_blob(md_json)

        with pytest.raises(Exception, match="already exists for trial"):
            insert_manifest_into_blob(manifest, uploader_email="test@email.com")


def test_insert_manifest_from_json(cidc_api, clean_db, monkeypatch):
    """test that insertion of manifest from json works as expected"""
    # grab a completed manifest
    manifest = [m for m in manifests if m.get("status") in [None, "qc_complete"]][0]

    with cidc_api.app_context():
        setup_user(cidc_api, monkeypatch)

        # blank db throws error
        with pytest.raises(Exception, match="No trial found with id"):
            insert_manifest_from_json(
                deepcopy(manifest), uploader_email="test@email.com"
            )

        errs = insert_record_batch(
            {ClinicalTrial: [ClinicalTrial(protocol_identifier="test_trial")]}
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
        TrialMetadata(trial_id="test_trial", metadata_json=metadata_json).insert()

        with pytest.raises(
            Exception, match="No Collection event with trial_id, event_name"
        ):
            insert_manifest_from_json(
                deepcopy(manifest), uploader_email="test@email.com"
            )

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
            insert_manifest_from_json(
                deepcopy(manifest), uploader_email="test@email.com"
            )

        errs = insert_record_batch(
            {
                Cohort: [
                    Cohort(trial_id="test_trial", cohort_name="Arm_A"),
                    Cohort(trial_id="test_trial", cohort_name="Arm_Z"),
                ]
            }
        )
        assert len(errs) == 0

        insert_manifest_from_json(deepcopy(manifest), uploader_email="test@email.com")
        validate_relational("test_trial")

        for other_manifest in [
            m
            for m in manifests
            if m.get("status") in (None, "qc_complete")
            and m != manifest
            and not m.get("excluded")
        ]:
            insert_manifest_from_json(
                deepcopy(other_manifest), uploader_email="test@email.com"
            )
            validate_relational("test_trial")

        with pytest.raises(Exception, match="already exists for trial"):
            insert_manifest_from_json(
                deepcopy(manifest), uploader_email="test@email.com"
            )
