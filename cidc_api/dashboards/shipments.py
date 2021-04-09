from typing import List, Optional

import pandas as pd
import dash_table as dt
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input
from sqlalchemy.sql.expression import column

from .dash_utils import create_new_dashboard
from ..models import UploadJobStatus, UploadJobs, CIDCRole, TrialMetadata
from ..shared.auth import requires_auth


def get_manifest_samples(trial_id: str, manifest_id: str) -> Optional[pd.DataFrame]:
    """Return a list of sample metadata associated with the given manifest upload."""
    # Get the most recent manifest upload with the given trial and manifest ids.
    # Although we're only querying for a single record, the UploadJobs.list method
    # makes it easier to build our query.
    upload_records: List[UploadJobs] = UploadJobs.list(
        page_size=1,
        sort_field="id",
        sort_direction="desc",
        filter_=lambda q: q.filter(
            UploadJobs.status == UploadJobStatus.MERGE_COMPLETED.value,
            UploadJobs.trial_id == trial_id,
            # This relies on the fact any manifest upload's metadata_patch will have exactly
            # one entry in its 'shipments' array corresponding to the manifest that was uploaded.
            UploadJobs.metadata_patch[("shipments", "0", "manifest_id")].astext
            == manifest_id,
        ),
    )

    # Return an empty list if no matching manifests were found
    if len(upload_records) == 0:
        return None

    upload_record = upload_records[0]

    # Normalize the participant-sample metadata to get a dataframe of samples
    metadata_patch = upload_record.metadata_patch
    samples_df = pd.io.json.json_normalize(
        metadata_patch["participants"], "samples", meta=["cohort_name"]
    )
    samples_df["assay_type"] = metadata_patch["shipments"][0]["assay_type"]
    samples_df["cimac_participant_id"] = samples_df.cimac_id.apply(lambda x: x[0:7])
    samples_df["manifest_id"] = manifest_id
    samples_df["cidc_received"] = upload_record._created

    return samples_df


def get_trial_shipments(trial_id: str) -> Optional[List[dict]]:
    """Return a list of distinct shipments associated with the given trial."""
    trial_record = TrialMetadata.find_by_trial_id(trial_id)

    # Return an empty list of shipments if the trial doesn't exist
    if trial_record is None:
        return None

    # Extract and deduplicate shipments
    seen = set()
    shipments = []
    for shipment in trial_record.metadata_json.get("shipments", []):
        manifest_id = shipment["manifest_id"]

        # Skip duplicates
        if manifest_id in seen:
            continue
        seen.add(manifest_id)

        # Get related samples
        manifest_samples = get_manifest_samples(trial_id, manifest_id)

        # Add or reformat fields
        shipment["cidc_received"] = manifest_samples.cidc_received[0]
        shipment["participant_count"] = manifest_samples.cimac_participant_id.nunique()
        shipment["sample_count"] = manifest_samples.shape[0]

        shipments.append(shipment)

    return shipments


shipments_dashboard = create_new_dashboard("upload_jobs")

TRIAL_DROPDOWN = "trial-dropdown"
SHIPMENTS_TABLE_ID = "shipments-table"
MANIFEST_DROPDOWN = "manifest-dropdown"
SAMPLES_TABLE_ID = "samples-table"

shipments_dashboard.layout = html.Div(
    [
        dcc.Dropdown(
            id=TRIAL_DROPDOWN, placeholder="Select trial to view shipment manifests"
        ),
        dt.DataTable(id=SHIPMENTS_TABLE_ID),
        dcc.Dropdown(
            id=MANIFEST_DROPDOWN, placeholder="Select manifest to view ingested samples"
        ),
        dt.DataTable(id=SAMPLES_TABLE_ID),
    ]
)


@shipments_dashboard.callback(
    Output(TRIAL_DROPDOWN, "options"), Input(TRIAL_DROPDOWN, "children")
)
@requires_auth("dash.shipments.trial_ids", [CIDCRole.ADMIN.value])
def populate_trial_ids(trial_ids):
    """Load available trial_ids into the trial id dropdown."""
    if not trial_ids:
        return [
            {"label": t, "value": t} for t in TrialMetadata.get_distinct("trial_id")
        ]
    return trial_ids


@shipments_dashboard.callback(
    Output(MANIFEST_DROPDOWN, "options"), Input(TRIAL_DROPDOWN, "value")
)
@requires_auth("dash.shipments.manifest_ids", [CIDCRole.ADMIN.value])
def populate_manifest_ids(trial_id):
    """Load available manifest IDs into dropdown."""
    # Don't populate the dropdown if no trial ID has been selected
    if trial_id is None:
        return []

    shipments = get_trial_shipments(trial_id)
    if shipments is None:
        return []

    return [
        {"label": shipment["manifest_id"], "value": shipment["manifest_id"]}
        for shipment in shipments
    ]


@shipments_dashboard.callback(
    [Output(SHIPMENTS_TABLE_ID, "columns"), Output(SHIPMENTS_TABLE_ID, "data")],
    [Input(TRIAL_DROPDOWN, "value")],
)
@requires_auth("dash.shipments.shipments", [CIDCRole.ADMIN.value])
def get_shipments_for_trial(trial_id):
    """Load successful upload jobs for the selected trials."""
    columns = [
        {"name": "Manifest Id", "id": "manifest_id"},
        {"name": "Assay type", "id": "assay_type"},
        {"name": "Date Shipped", "id": "date_shipped"},
        {"name": "Date Recieved", "id": "date_received"},
        {"name": "Date in CIDC", "id": "cidc_received"},
        {"name": "# participants", "id": "participant_count"},
        {"name": "# samples", "id": "sample_count"},
    ]

    if trial_id is None:
        return [], []

    shipments = get_trial_shipments(trial_id)
    if shipments is None:
        return columns, []

    return columns, shipments


@shipments_dashboard.callback(
    [Output(SAMPLES_TABLE_ID, "columns"), Output(SAMPLES_TABLE_ID, "data")],
    [Input(TRIAL_DROPDOWN, "value"), Input(MANIFEST_DROPDOWN, "value")],
)
@requires_auth("dash.shipments.samples", [CIDCRole.ADMIN.value])
def get_samples_for_manifest(trial_id, manifest_id):
    """Load successful upload jobs for the selected trials."""
    columns = [
        {"name": "Manifest Id", "id": "manifest_id"},
        {"name": "CIMAC Id", "id": "cimac_id"},
        {"name": "Cohort name", "id": "cohort_name"},
        {"name": "Assay type", "id": "assay_type"},
        {"name": "Sample Collection Procedure", "id": "sample_collection_procedure"},
        {"name": "Type Of Sample", "id": "type_of_sample"},
    ]

    if trial_id is None or manifest_id is None:
        return [], []

    samples = get_manifest_samples(trial_id, manifest_id)
    if samples is None:
        return columns, []
    return columns, samples.to_dict("records")
