import os

os.environ["TZ"] = "UTC"
import cidc_api
from datetime import datetime
import json
import os
import pytest
from unittest.mock import MagicMock

from cidc_api.models.models import TrialMetadata, UploadJobs, Users
from cidc_api.models.templates import ClinicalTrial
from cidc_api.models.templates.sync_schemas import (
    _make_sample_to_shipment_map,
    syncall_from_blobs,
)

from .utils import set_up_example_trial


@pytest.mark.skip(reason="For local testing of syncall_from_blobs; comment out to run")
def test_run_syncall_from_blobs(cidc_api):
    with cidc_api.app_context():
        err = syncall_from_blobs()
        if err:
            print(len(err))
            raise err[0]
        assert len(err) == 0, "\n".join([str(e) for e in err])


def test_make_sample_to_shipment_map():
    clean_db = MagicMock()
    clean_db.query.return_value = MagicMock()
    mock_return = MagicMock()

    mock_return.metadata_patch = {
        "shipments": [
            {"manifest_id": "test_manifest"},
            {"manifest_id": "test_manifest2"},
        ]
    }
    clean_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
        mock_return
    ]
    with pytest.raises(Exception, match="Multiple shipments in single upload"):
        _make_sample_to_shipment_map("trial_id", session=clean_db)

    mock_return.metadata_patch = {
        "shipments": [{"manifest_id": "test_manifest"}],
        "participants": [
            {"samples": [{"cimac_id": "CTTTPP111.00"}, {"cimac_id": "CTTTPP112.00"}]},
            {"samples": [{"cimac_id": "CTTTPP211.00"}, {"cimac_id": "CTTTPP212.00"}]},
        ],
    }
    clean_db.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
        mock_return
    ]
    assert _make_sample_to_shipment_map("trial_id", session=clean_db) == {
        "CTTTPP111.00": "test_manifest",
        "CTTTPP112.00": "test_manifest",
        "CTTTPP211.00": "test_manifest",
        "CTTTPP212.00": "test_manifest",
    }


def test_syncall_from_blobs(clean_db, cidc_api, monkeypatch):
    with cidc_api.app_context():
        set_up_example_trial(clean_db, cidc_api)

        user = Users(email="foo@email.com")
        user.insert()

        ct_filepath = os.path.join(os.path.dirname(__file__), "data", "CT_1.json")
        md_patch = json.load(open(ct_filepath, "r"))

        trial_md = TrialMetadata(
            metadata_json=md_patch, trial_id=md_patch["protocol_identifier"]
        )
        trial_md.insert()

        upload = UploadJobs(
            _status="merge-completed",
            multifile=False,
            metadata_patch=md_patch,
            token="0123456789ABCDEF0123456789ABCDEF",
            trial_id=md_patch["protocol_identifier"],
            upload_type="test_assay",
            uploader_email=user.email,
        )
        upload.insert()

        err = syncall_from_blobs()
        assert len(err) == 0, err

        trial = clean_db.query(ClinicalTrial).first()
        trial.protocol_identifier == md_patch["protocol_identifier"]
        trial.allowed_cohort_names == md_patch[
            "allowed_cohort_names"
        ]  # names only for these
        trial.allowed_collection_event_names == md_patch[
            "allowed_collection_event_names"
        ]  # names only for these

        shipments = [s for s in trial.shipments]
        assert len(shipments) == len(md_patch["shipments"])  # == 1
        db_ship, json_ship = shipments[0], md_patch["shipments"][0]
        assert db_ship.account_number == json_ship["account_number"]
        assert db_ship.assay_priority == json_ship["assay_priority"]
        assert db_ship.assay_type == json_ship["assay_type"]
        assert db_ship.courier == json_ship["courier"]
        assert (
            db_ship.date_received
            == datetime.strptime(json_ship["date_received"], "%Y-%m-%d").date()
        )  # special type handling
        assert (
            db_ship.date_shipped
            == datetime.strptime(json_ship["date_shipped"], "%Y-%m-%d").date()
        )  # special type handling
        assert db_ship.manifest_id == json_ship["manifest_id"]
        assert db_ship.quality_of_shipment == json_ship["quality_of_shipment"]
        assert db_ship.receiving_party == json_ship["receiving_party"]
        assert db_ship.ship_from == json_ship["ship_from"]
        assert db_ship.ship_to == json_ship["ship_to"]
        assert db_ship.shipping_condition == json_ship["shipping_condition"]
        assert db_ship.tracking_number == json_ship["tracking_number"]

        # Check json_data
        assert db_ship.json_data["account_number"] == json_ship["account_number"]
        assert db_ship.json_data["assay_priority"] == json_ship["assay_priority"]
        assert db_ship.json_data["assay_type"] == json_ship["assay_type"]
        assert db_ship.json_data["courier"] == json_ship["courier"]
        assert db_ship.json_data["date_received"] == json_ship["date_received"]
        assert db_ship.json_data["date_shipped"] == json_ship["date_shipped"]
        assert (
            db_ship.json_data["quality_of_shipment"] == json_ship["quality_of_shipment"]
        )
        assert db_ship.json_data["receiving_party"] == json_ship["receiving_party"]
        assert db_ship.json_data["ship_from"] == json_ship["ship_from"]
        assert db_ship.json_data["ship_to"] == json_ship["ship_to"]
        assert (
            db_ship.json_data["shipping_condition"] == json_ship["shipping_condition"]
        )
        assert db_ship.json_data["tracking_number"] == json_ship["tracking_number"]

        participants = sorted(
            [p for p in trial.participants], key=lambda p: p.cimac_participant_id
        )
        assert len(participants) == len(md_patch["participants"])  # == 2
        for db_partic, json_partic in zip(participants, md_patch["participants"]):
            assert db_partic.cimac_participant_id == json_partic["cimac_participant_id"]
            assert db_partic.cohort_name == json_partic["cohort_name"]
            assert db_partic.trial_id == md_patch["protocol_identifier"]
            assert (
                db_partic.trial_participant_id == json_partic["participant_id"]
            )  # special handling for name change

            samples = sorted([s for s in db_partic.samples], key=lambda s: s.cimac_id)
            assert len(samples) == len(json_partic["samples"])
            for db_sample, json_sample in zip(samples, json_partic["samples"]):
                assert db_sample.cimac_id == json_sample["cimac_id"]
                assert (
                    db_sample.cimac_participant_id
                    == json_partic["cimac_participant_id"]
                )
                assert (
                    db_sample.collection_event_name
                    == json_sample["collection_event_name"]
                )
                assert db_sample.material_used == json_sample["material_used"]
                assert db_sample.material_remaining == json_sample["material_remaining"]
                assert db_sample.parent_sample_id == json_sample["parent_sample_id"]
                assert db_sample.quality_of_sample == json_sample["quality_of_sample"]
                assert db_sample.sample_location == json_sample["sample_location"]
                assert (
                    db_sample.sample_volume_units == json_sample["sample_volume_units"]
                )
                assert db_sample.trial_id == md_patch["protocol_identifier"]
                assert db_sample.type_of_sample == json_sample["type_of_sample"]
                assert (
                    db_sample.type_of_primary_container
                    == json_sample["type_of_primary_container"]
                )

                # Check json_data
                assert (
                    db_sample.json_data["material_used"] == json_sample["material_used"]
                )
                assert (
                    db_sample.json_data["material_remaining"]
                    == json_sample["material_remaining"]
                )
                assert (
                    db_sample.json_data["parent_sample_id"]
                    == json_sample["parent_sample_id"]
                )
                assert (
                    db_sample.json_data["quality_of_sample"]
                    == json_sample["quality_of_sample"]
                )
                assert (
                    db_sample.json_data["sample_location"]
                    == json_sample["sample_location"]
                )
                assert (
                    db_sample.json_data["sample_volume_units"]
                    == json_sample["sample_volume_units"]
                )
                assert (
                    db_sample.json_data["type_of_sample"]
                    == json_sample["type_of_sample"]
                )
                assert (
                    db_sample.json_data["type_of_primary_container"]
                    == json_sample["type_of_primary_container"]
                )
