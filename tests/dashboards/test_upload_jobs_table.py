from datetime import datetime

from dash.testing.composite import DashComposite

from cidc_api.models import (
    Users,
    UploadJobs,
    ROLES,
    UploadJobStatus,
    CIDCRole,
    TrialMetadata,
)
from cidc_api.dashboards.upload_jobs_table import upload_jobs_table, UPLOAD_JOB_TABLE_ID

from ..utils import make_role, mock_current_user


def test_upload_jobs_table(cidc_api, clean_db, monkeypatch, dash_duo: DashComposite):
    """Check that only CIDC Admins can view data in the upload jobs table dashboard."""
    user = Users(email="test@email.com", approval_date=datetime.now())
    trial = TrialMetadata(
        trial_id="test-trial",
        metadata_json={
            "protocol_identifier": "test-trial",
            "participants": [],
            "allowed_cohort_names": [],
            "allowed_collection_event_names": [],
        },
    )
    upload_job = UploadJobs(
        uploader_email=user.email,
        trial_id=trial.trial_id,
        upload_type="wes",
        gcs_xlsx_uri="",
        metadata_patch={},
        multifile=False,
    )
    upload_job._set_status_no_validation(UploadJobStatus.MERGE_COMPLETED.value)

    with cidc_api.app_context():
        user.insert()
        trial.insert()
        upload_job.insert()

        clean_db.refresh(user)
        clean_db.refresh(upload_job)

    for role in ROLES:
        make_role(user.id, role, cidc_api)
        mock_current_user(user, monkeypatch)

        dash_duo.server(upload_jobs_table)
        dash_duo.wait_for_page(f"{dash_duo.server.url}/dashboards/upload_jobs/")

        if CIDCRole(role) == CIDCRole.ADMIN:
            dash_duo.wait_for_contains_text(
                f"#{UPLOAD_JOB_TABLE_ID}", upload_job.uploader_email
            )
            dash_duo.wait_for_contains_text(
                f"#{UPLOAD_JOB_TABLE_ID}", upload_job.trial_id
            )
            dash_duo.wait_for_contains_text(
                f"#{UPLOAD_JOB_TABLE_ID}", upload_job.upload_type
            )
        else:
            dash_duo._wait_for_callbacks()
            assert any(
                ["401 (UNAUTHORIZED)" in log["message"] for log in dash_duo.get_logs()]
            )
