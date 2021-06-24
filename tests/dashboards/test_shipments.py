from datetime import datetime

from dash.testing.composite import DashComposite
from selenium.webdriver.common.by import By
import pytest

from cidc_api.models import (
    Users,
    UploadJobs,
    ROLES,
    UploadJobStatus,
    CIDCRole,
    TrialMetadata,
)
from cidc_api.dashboards.shipments import (
    get_manifest_samples,
    get_trial_shipments,
    shipments_dashboard,
    TRIAL_DROPDOWN,
    SHIPMENTS_TABLE_ID,
    MANIFEST_DROPDOWN,
    SAMPLES_TABLE_ID,
)

from ..utils import make_role, mock_current_user


trial_id = "test-trial"
manifest_id = "test-manifest"
assay_type = "CyTOF"
num_samples = [2, 3, 5]
num_participants = 3


def setup_data(cidc_api, clean_db):
    user = Users(email="test@email.com", approval_date=datetime.now())
    shipment = {
        "courier": "FEDEX",
        "ship_to": "",
        "ship_from": "",
        "assay_type": assay_type,
        "manifest_id": manifest_id,
        "date_shipped": "2020-06-10 00:00:00",
        "date_received": "2020-06-11 00:00:00",
        "account_number": "",
        "assay_priority": "1",
        "receiving_party": "MSSM_Rahman",
        "tracking_number": "",
        "shipping_condition": "Frozen_Dry_Ice",
        "quality_of_shipment": "Specimen shipment received in good condition",
    }
    metadata = {
        "protocol_identifier": trial_id,
        "shipments": [
            # we get duplicate shipment uploads sometimes
            shipment,
            shipment,
        ],
        "participants": [
            {
                "cimac_participant_id": f"CTTTPP{p}",
                "participant_id": "x",
                "cohort_name": "",
                "samples": [
                    {
                        "cimac_id": f"CTTTPP{p}SS.0{s}",
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
        "allowed_cohort_names": [""],
        "allowed_collection_event_names": [""],
    }
    trial = TrialMetadata(trial_id=trial_id, metadata_json=metadata)
    upload_job = UploadJobs(
        uploader_email=user.email,
        trial_id=trial.trial_id,
        upload_type="pbmc",
        gcs_xlsx_uri="",
        metadata_patch=metadata,
        multifile=False,
    )
    upload_job._set_status_no_validation(UploadJobStatus.MERGE_COMPLETED.value)
    with cidc_api.app_context():
        user.insert()
        trial.insert()
        upload_job.insert()

        clean_db.refresh(user)
        clean_db.refresh(upload_job)
        clean_db.refresh(trial)

    return user, upload_job, trial


@pytest.mark.skip(reason="fails locally")
def test_shipments_dashboard(cidc_api, clean_db, monkeypatch, dash_duo: DashComposite):
    """
    Check that the shipments dashboard behaves as expected.
    """
    user, _, _ = setup_data(cidc_api, clean_db)

    for role in ROLES:
        make_role(user.id, role, cidc_api)
        mock_current_user(user, monkeypatch)

        dash_duo.server(shipments_dashboard)
        dash_duo.wait_for_page(f"{dash_duo.server.url}/dashboards/upload_jobs/")

        if CIDCRole(role) == CIDCRole.ADMIN:
            # open trial dropdown
            dash_duo.click_at_coord_fractions(f"#{TRIAL_DROPDOWN}", 0.1, 0.1)
            dash_duo.wait_for_contains_text(f"#{TRIAL_DROPDOWN}", trial_id)
            # select the first trial
            trial_select = dash_duo.find_elements(".VirtualizedSelectOption")[0]
            dash_duo.click_at_coord_fractions(trial_select, 0.1, 0.1)
            # click off dropdown to close it
            dash_duo.click_at_coord_fractions(f"#{SHIPMENTS_TABLE_ID}", 0.1, 0.1)
            # ensure the shipments table loads
            dash_duo.wait_for_contains_text(f"#{SHIPMENTS_TABLE_ID}", manifest_id)
            # open manifest dropdown
            dash_duo.click_at_coord_fractions(f"#{MANIFEST_DROPDOWN}", 0.1, 0.1)
            dash_duo.wait_for_contains_text(f"#{TRIAL_DROPDOWN}", trial_id)
            # select the first manifest
            manifest = dash_duo.find_elements(".VirtualizedSelectOption")[0]
            dash_duo.click_at_coord_fractions(manifest, 0.1, 0.1)
            # ensure the samples table loads
            dash_duo.wait_for_contains_text(f"#{SAMPLES_TABLE_ID}", "CTTTPP1SS.01")
        else:
            dash_duo._wait_for_callbacks()
            assert any(
                ["401 (UNAUTHORIZED)" in log["message"] for log in dash_duo.get_logs()]
            )


def test_get_manifest_samples(cidc_api, clean_db):
    """Test the helper function used for getting a list of samples for a given trial manifest."""
    setup_data(cidc_api, clean_db)

    with cidc_api.app_context():
        samples = get_manifest_samples(trial_id, manifest_id)
        assert samples.cimac_id.nunique() == sum(num_samples)
        assert samples.cimac_participant_id.nunique() == num_participants


def test_get_trial_shipments(cidc_api, clean_db):
    """Test the helper function used for getting a list of shipments for a given trial."""
    _, upload_job, _ = setup_data(cidc_api, clean_db)

    with cidc_api.app_context():
        shipments = get_trial_shipments(trial_id)
        assert len(shipments) == 1
        shipment = shipments[0]
        assert shipment["cidc_received"] == upload_job._created
        assert shipment["participant_count"] == num_participants
        assert shipment["sample_count"] == sum(num_samples)
