import dash
import dash_table as dt
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
from flask import Flask, request
from sqlalchemy.sql.selectable import Select

from cidc_api.models import (
    UploadJobSchema,
    UploadJobStatus,
    UploadJobs,
    CIDCRole,
    TrialMetadata,
)
from cidc_api.config.settings import MAX_PAGINATION_PAGE_SIZE
from cidc_api.shared.auth import requires_auth

# This HTML template overrides Dash's default to enable passing values
# (e.g., a user's identity token) in the JSON body of all Dash ajax requests
# via URL query params. For example, loading a Dash dashboard like so:
#
#   <iframe src="https://api.cimac-network.org/dashboard-url?id_token=foo&some_param=bar" />
#
# will automatically insert {"id_token": "foo", "some_param": "bar"} into the
# JSON body of every ajax request the dashboard makes after loading into the iframe.
# This is useful for a) performing user authentication and authorization and
# b) parameterizing dashboards (i.e., having a generic trial dashboard that takes
# a protocol identifier as a parameter to determine what to render).
#
# The operative code here is in the <script id="_dash-renderer">...</script> tag.
index_string = """
    <!DOCTYPE html>
    <html>
        <head>
            {%metas%}
            <title>{%title%}</title>
            {%favicon%}
            {%css%}
        </head>
        <body>
            {%app_entry%}
            <footer>
                {%config%}
                {%scripts%}
                <script id="_dash-renderer">
                    const urlParams = new URLSearchParams(window.location.search);
                    const renderer = new DashRenderer({
                        request_pre: (req) => {
                            for (const [param, value] of urlParams.entries()) {
                                req[param] = value;
                            }
                        },
                        request_post: (req, res) => {}
                    })
                </script>
            </footer>
        </body>
    </html>
"""

UPLOAD_JOB_TABLE_ID = "upload-job-table"
TRIAL_ID_DROPDOWN_ID = "trial-id-dropdown"
NO_INPUT_ID = "force-callback-no-inputs"

layout = html.Div(
    [
        dcc.Dropdown(
            id=TRIAL_ID_DROPDOWN_ID,
            placeholder="Select protocol identifiers",
            multi=True,
        ),
        dt.DataTable(id=UPLOAD_JOB_TABLE_ID),
        # This input remains hidden from the user and is used
        # to allow adding component callbacks
        dcc.Input(id=NO_INPUT_ID, type="hidden"),
    ]
)

# Add this to a dash callback without inputs to ensure
# the callback gets called once when the dashboard first loads.
NoInputs = Input(NO_INPUT_ID, "value")

upload_jobs_schema = UploadJobSchema(exclude=["metadata_patch"])


def init_dash(app: Flask):
    dash_app = dash.Dash(
        server=app,
        url_base_pathname="/dashboards/upload_jobs/",
        index_string=index_string,
    )
    dash_app.layout = layout

    @dash_app.callback(Output(TRIAL_ID_DROPDOWN_ID, "options"), NoInputs)
    @requires_auth("dash.trial_ids", [CIDCRole.ADMIN.value])
    def populate_trial_ids(_):
        """Load available trial_ids into the trial id dropdown."""
        return [
            {"label": t, "value": t} for t in TrialMetadata.get_distinct("trial_id")
        ]

    @dash_app.callback(
        [Output(UPLOAD_JOB_TABLE_ID, "columns"), Output(UPLOAD_JOB_TABLE_ID, "data")],
        [Input(TRIAL_ID_DROPDOWN_ID, "value")],
    )
    @requires_auth("dash.upload_jobs", [CIDCRole.ADMIN.value])
    def upload_jobs_table(selected_trial_ids):
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
