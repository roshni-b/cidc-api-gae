import os
import shutil
from datetime import datetime

from cidc_schemas import prism
from cidc_api.models import DownloadableFiles, TrialMetadata

INFO_ENDPOINT = "/info"


def test_info_assays(cidc_api):
    """Check that the /info/assays endpoint returns a list of assays"""
    client = cidc_api.test_client()
    res = client.get(f"{INFO_ENDPOINT}/assays")
    assert type(res.json) == list
    assert "olink" in res.json


def test_info_analyses(cidc_api):
    """Check that the /info/analyses endpoint returns a list of assays"""
    client = cidc_api.test_client()
    res = client.get(f"{INFO_ENDPOINT}/analyses")
    assert type(res.json) == list
    assert "cytof_10021_9204_analysis" in res.json


def test_info_manifests(cidc_api):
    """Check that the /info/manifests endpoint returns a list of manifests"""
    client = cidc_api.test_client()
    res = client.get(f"{INFO_ENDPOINT}/manifests")
    assert type(res.json) == list
    assert "pbmc" in res.json


def test_info_extra_types(cidc_api):
    """Check that the /info/manifests endpoint returns a list of manifests"""
    client = cidc_api.test_client()
    res = client.get(f"{INFO_ENDPOINT}/extra_data_types")
    assert type(res.json) == list
    assert "participants info" in res.json


def test_info_data_overview(cidc_api, clean_db):
    """Check that the data overview has expected structure and values"""

    def insert_trial(trial_id, num_participants, num_samples):
        TrialMetadata(
            trial_id=trial_id,
            metadata_json={
                prism.PROTOCOL_ID_FIELD_NAME: trial_id,
                "allowed_cohort_names": [""],
                "allowed_collection_event_names": [""],
                "participants": [
                    {
                        "cimac_participant_id": f"CTTTPP{p}",
                        "participant_id": "x",
                        "samples": [
                            {
                                "cimac_id": f"CTTTPP1SS.0{s}",
                                "sample_location": "",
                                "type_of_primary_container": "Other",
                                "type_of_sample": "Other",
                                "collection_event_name": "",
                                "parent_sample_id": "",
                            }
                            for s in range(num_samples[p])
                        ],
                    }
                    for p in range(num_participants)
                ],
            },
        ).insert()

    # 3 trials
    # 15 participants
    # 40 samples
    # 3 files
    with cidc_api.app_context():
        insert_trial("1", 6, [0] * 6)
        insert_trial("2", 4, [5, 6, 7, 8])
        insert_trial("3", 5, [3, 2, 1, 1, 7])
        for i in range(3):
            DownloadableFiles(
                trial_id="1",
                upload_type="wes",
                object_url=str(i),
                facet_group="/wes/r2_L.fastq.gz",  # this is what makes this file "related"
                uploaded_timestamp=datetime.now(),
                file_size_bytes=2,
            ).insert()

    client = cidc_api.test_client()

    res = client.get("/info/data_overview")
    assert res.status_code == 200
    assert res.json == {
        "num_assays": len(prism.SUPPORTED_ASSAYS),
        "num_trials": 3,
        "num_participants": 15,
        "num_samples": 40,
        "num_files": 3,
        "num_bytes": 6,
    }


def test_templates(cidc_api):
    """Check that the /info/templates endpoint behaves as expected"""
    client = cidc_api.test_client()

    # Invalid URLs
    res = client.get(f"{INFO_ENDPOINT}/templates/../pbmc")
    assert res.status_code == 400
    assert res.json["_error"]["message"] == "Invalid template family: .."

    res = client.get(f"{INFO_ENDPOINT}/templates/manifests/pbmc123")
    assert res.status_code == 404
    assert (
        res.json["_error"]["message"]
        == "No template found for the given template family and template type"
    )

    res = client.get(f"{INFO_ENDPOINT}/templates/manifests/pbmc123!")
    assert res.status_code == 400
    assert res.json["_error"]["message"] == "Invalid template type: pbmc123!"

    # Non-existent template
    res = client.get(f"{INFO_ENDPOINT}/templates/foo/bar")
    assert res.status_code == 404

    # Generate and get a valid manifest
    pbmc_path = os.path.join(
        cidc_api.config["TEMPLATES_DIR"], "manifests", "pbmc_template.xlsx"
    )
    assert not os.path.exists(pbmc_path)
    res = client.get(f"{INFO_ENDPOINT}/templates/manifests/pbmc")
    assert res.status_code == 200
    with open(pbmc_path, "rb") as f:
        assert res.data == f.read()

    # Generate and get a valid assay
    olink_path = os.path.join(
        cidc_api.config["TEMPLATES_DIR"], "metadata", "olink_template.xlsx"
    )
    assert not os.path.exists(olink_path)
    res = client.get(f"{INFO_ENDPOINT}/templates/metadata/olink")
    assert res.status_code == 200
    with open(olink_path, "rb") as f:
        assert res.data == f.read()
