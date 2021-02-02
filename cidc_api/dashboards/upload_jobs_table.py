"""This dashboard was created as a proof-of-concept for integrating Dash with the API.
We can remove it if we want to - just remember to also remove the corresponding UI 
element from the admin profile page on the portal.
"""

import dash_table as dt
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input

from .dash_utils import create_new_dashboard
from ..models import (
    UploadJobSchema,
    UploadJobStatus,
    UploadJobs,
    CIDCRole,
    TrialMetadata,
)
from ..config.settings import MAX_PAGINATION_PAGE_SIZE
from ..shared.auth import requires_auth

upload_jobs_table = create_new_dashboard("upload_jobs")

UPLOAD_JOB_TABLE_ID = "upload-job-table"
TRIAL_ID_DROPDOWN_ID = "trial-id-dropdown"

upload_jobs_table.layout = html.Div(
    [
        dcc.Dropdown(
            id=TRIAL_ID_DROPDOWN_ID,
            placeholder="Select protocol identifiers",
            multi=True,
        ),
        dt.DataTable(id=UPLOAD_JOB_TABLE_ID),
    ]
)

upload_jobs_schema = UploadJobSchema(exclude=["metadata_patch"])


@upload_jobs_table.callback(
    Output(TRIAL_ID_DROPDOWN_ID, "options"), Input(TRIAL_ID_DROPDOWN_ID, "children")
)
@requires_auth("dash.trial_ids", [CIDCRole.ADMIN.value])
def get_trial_ids(trial_ids):
    """Load available trial_ids into the trial id dropdown."""
    if not trial_ids:
        return [
            {"label": t, "value": t} for t in TrialMetadata.get_distinct("trial_id")
        ]
    return trial_ids


@upload_jobs_table.callback(
    [Output(UPLOAD_JOB_TABLE_ID, "columns"), Output(UPLOAD_JOB_TABLE_ID, "data")],
    [Input(TRIAL_ID_DROPDOWN_ID, "value")],
)
@requires_auth("dash.upload_jobs", [CIDCRole.ADMIN.value])
def get_upload_jobs(selected_trial_ids):
    """Load successful upload jobs for the selected trials."""
    columns = [
        {"name": "Trial ID", "id": "trial_id"},
        {"name": "Upload Type", "id": "upload_type"},
        {"name": "Uploader", "id": "uploader_email"},
        {"name": "Date", "id": "_updated"},
    ]
    upload_jobs = UploadJobs.list(
        page_size=MAX_PAGINATION_PAGE_SIZE,
        sort_field="_updated",
        sort_direction="desc",
        filter_=lambda q: q.filter(
            UploadJobs.status == UploadJobStatus.MERGE_COMPLETED.value,
            not selected_trial_ids or UploadJobs.trial_id.in_(selected_trial_ids),
        ),
    )
    return (columns, upload_jobs_schema.dump(upload_jobs, many=True))
