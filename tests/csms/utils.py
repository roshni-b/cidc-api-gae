from typing import Any, Dict, List
from datetime import date, datetime
from unittest.mock import MagicMock

from cidc_api.csms import auth
from cidc_api.models.templates import MetadataModel, Participant, Sample, Shipment
from cidc_api.models.templates import cimac_id_to_cimac_participant_id

import data


def _convert_to_date(date_str: str) -> date:
    if not date_str.endswith("Z"):
        date_str += "Z"
    return datetime.strptime(date_str.replace("T", " "), "%Y-%m-%d %I:%M:00 %Z")


def samples_history(cimac_id: str) -> Dict[str, Any]:
    sample = [s.get("cimac_id") == cimac_id for s in data.samples]
    assert len(sample) == 1, "Given CIMAC id is not unique"
    sample = sample[0]

    ret = [
        {
            "data": sample,
            "diff": {k: [None, v] for k, v in sample.items()},
            "metadata": {
                "id": "0123456789ABCDEFGHIJKL",
                "version": 0,
                "txTime": "2021-01-01T00:00:00Z",
                "txId": "KJLIHGFEDCBA9876543210",
            },
        }
    ]
    if sample.get("status") in [None, "qc_complete"]:
        ret[0]["data"]["status"] = "cimac_id_generated"
        day = _convert_to_date(sample["modified_timestamp"]).day

        ret.append(
            {
                "data": sample,
                "diff": {"status": ["cimac_id_generated", sample.get("status")]},
                "metadata": {
                    "id": "123456789ABCDEFGHIJKL0",
                    "version": 1,
                    "txTime": f"2021-01-0{day}T00:00:00Z",
                    "txId": "0KJLIHGFEDCBA987654321",
                },
            }
        )


def manifests_history(manifest_id: str) -> Dict[str, Any]:
    manifest = [m.get("manifest_id") == manifest_id for m in data.manifests]
    assert len(manifest) == 1, "Given manifest ID is not unique"
    manifest = manifest[0]

    ret = [
        {
            "data": manifest,
            "diff": {k: [None, v] for k, v in manifest.items()},
            "metadata": {
                "id": "0123456789ABCDEFGHIJKL",
                "version": 0,
                "txTime": "2021-01-01T00:00:00Z",
                "txId": "KJLIHGFEDCBA9876543210",
            },
        }
    ]
    if manifest.get("status") in [None, "qc_complete"]:
        ret[0]["data"]["status"] = "draft"
        ret[0]["data"].pop("account_number", "")

        ret.append(
            {
                "data": manifest,
                "diff": {
                    "status": ["draft", manifest.get("status")],
                    "account_number": [None, manifest.get("account_number")],
                },
                "metadata": {
                    "id": "123456789ABCDEFGHIJKL0",
                    "version": 1,
                    "txTime": f"2021-01-03T00:00:00Z",
                    "txId": "0KJLIHGFEDCBA987654321",
                },
            }
        )


def validate_relational(instances: List[MetadataModel]):
    for inst in instances:
        if isinstance(inst, Shipment):
            manifest = [
                m.get("manifest_id") == inst.manifest_id for m in data.manifests
            ]
            assert len(manifest) == 1, "Given manifest ID is not unique"
            manifest = manifest[0]

            inst_samples = [
                s
                for s in data.samples
                if s.get("manifest_id") == manifest["manifest_id"]
            ]

            if any("assay_priority" in s for s in inst_samples):
                assert (
                    len({s["assay_priority"] for s in inst_samples}) == 1
                ), f"assay_priority not uniquely defined for manifest {inst.manifest_id}"
                assert inst.assay_priority == inst_samples[0]["assay_priority"]
            if any("assay_type" in s for s in inst_samples):
                assert (
                    len({s["assay_type"] for s in inst_samples}) == 1
                ), f"assay_type not uniquely defined for manifest {inst.manifest_id}"
                assert inst.assay_type == inst_samples[0]["assay_type"]

            for k, v in manifest.items():
                if k in ["date_received", "date_shipped"]:
                    v = _convert_to_date(v)

                if hasattr(inst, k):
                    assert getattr(inst, k) == v

        elif isinstance(inst, Participant):
            inst_samples = [
                s.get("participant_id") == inst.cimac_participant_id
                for s in data.samples
            ]

            if any("participant_id" in s for s in inst_samples):
                assert (
                    len({s["participant_id"] for s in inst_samples}) == 1
                ), f"participant_id not uniquely defined for participant {inst.cimac_participant_id}"
                assert inst.trial_participant_id == inst_samples[0]["participant_id"]

            if any("cohort_name" in s for s in inst_samples):
                assert (
                    len({s["cohort_name"] for s in inst_samples}) == 1
                ), f"cohort_name not uniquely defined for participant {inst.cimac_participant_id}"
                assert inst.cohort_name == inst_samples[0]["cohort_name"]

        elif isinstance(inst, Sample):
            inst_samples = [s.get("cimac_id") == inst.cimac_id for s in data.samples]
            assert (
                len(inst_samples) == 1
            ), f"Sample not uniquely defined: {inst.primary_key_map()}"
            sample = inst_samples[0]

            if "standardized_collection_event_name" in sample:
                assert (
                    inst.collection_event_name
                    == sample["standardized_collection_event_name"]
                )
            else:
                assert inst.collection_event_name == sample["collection_event_name"]

            assert inst.cimac_participant_id == cimac_id_to_cimac_participant_id(
                sample["cimac_id"]
            )

            for k, v in sample:
                if k == "participant_id":
                    k = "trial_participant_id"

                if hasattr(inst, k):
                    assert getattr(inst, k) == v


def validate_json_blob(trial_md: dict):
    for shipment in trial_md["shipments"]:
        manifest = [
            m.get("manifest_id") == shipment["manifest_id"] for m in data.manifests
        ]
        assert len(manifest) == 1, "Given manifest ID is not unique"
        inst_samples = [
            s for s in data.samples if s.get("manifest_id") == manifest["manifest_id"]
        ]

        if any("assay_priority" in s for s in inst_samples):
            assert (
                len({s["assay_priority"] for s in inst_samples}) == 1
            ), f"assay_priority not uniquely defined for manifest {shipment['manifest_id']}"
            assert shipment["assay_priority"] == inst_samples[0]["assay_priority"]
        if any("assay_type" in s for s in inst_samples):
            assert (
                len({s["assay_type"] for s in inst_samples}) == 1
            ), f"assay_type not uniquely defined for manifest {shipment['manifest_id']}"
            assert shipment["assay_type"] == inst_samples[0]["assay_type"]

        for k, v in manifest.items():
            if k in ["date_received", "date_shipped"]:
                v = _convert_to_date(v)

            if k in shipment:
                assert shipment[k] == v

    for partic in trial_md["participants"]:
        inst_samples = [
            s.get("participant_id") == partic["cimac_participant_id"]
            for s in data.samples
        ]

        if any("participant_id" in s for s in inst_samples):
            assert (
                len({s["participant_id"] for s in inst_samples}) == 1
            ), f"participant_id not uniquely defined for participant {partic['cimac_participant_id']}"
            assert partic["trial_participant_id"] == inst_samples[0]["participant_id"]

        if any("cohort_name" in s for s in inst_samples):
            assert (
                len({s["cohort_name"] for s in inst_samples}) == 1
            ), f"cohort_name not uniquely defined for participant {partic['cimac_participant_id']}"
            assert partic["cohort_name"] == inst_samples[0]["cohort_name"]

        for sample in partic["samples"]:
            this_sample = [
                s.get("cimac_id") == sample["cimac_id"] for s in inst_samples
            ]
            assert (
                len(inst_samples) == 1
            ), f"Sample not uniquely defined: {sample['cimac_id']}"
            this_sample = inst_samples[0]

            if "standardized_collection_event_name" in sample:
                assert (
                    sample["collection_event_name"]
                    == this_sample["standardized_collection_event_name"]
                )
            else:
                assert (
                    sample["collection_event_name"]
                    == this_sample["collection_event_name"]
                )

            for k, v in this_sample:
                if k in sample:
                    assert sample[k] == v


def mock_get_with_authorization(monkeypatch):
    def mock_get(url: str, **kwargs) -> MagicMock:
        ret = MagicMock()

        # return 500 if limit set too high
        if kwargs.get("limit", 5000) > 5000:
            ret.status_code = 500

        else:
            ret.status_code = 200

            # returning all entries in single page,
            # so return nothing if trying to get page 2
            if kwargs.get("offset", 0) > 0:
                ret.json.return_value = {"data": []}
            elif url == "/samples":
                ret.json.return_value = {"data": data.samples}
            elif url == "/manifests":
                ret.json.return_value = {"data": data.manifests}
            elif url == "/samples/history":
                cimac_id = url.split("/samples/history")[1].split("?")[0]
                ret.json.return_value = samples_history(cimac_id)
            elif url == "/manifests/history":
                manifest_id = url.split("/samples/history")[1].split("?")[0]
                ret.json.return_value = manifests_history(manifest_id)

        return ret

    monkeypatch.setattr(auth, "get_with_authorization", mock_get)
