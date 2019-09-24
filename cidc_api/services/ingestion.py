"""
Endpoints for validating and ingesting metadata and data.
"""
import os
import json
import datetime
from typing import BinaryIO, Tuple, List

from werkzeug.exceptions import (
    BadRequest,
    InternalServerError,
    NotFound,
    NotImplemented,
    Unauthorized,
)

from eve import Eve
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session
from flask import Blueprint, request, Request, Response, jsonify, _request_ctx_stack
from cidc_schemas import constants, validate_xlsx, prism, template

import gcloud_client
from models import (
    AssayUploads,
    AssayUploadStatus,
    TRIAL_ID_FIELD,
    TrialMetadata,
    DownloadableFiles,
    ManifestUploads,
    CIDCRole,
)
from auth import requires_auth
from config.settings import GOOGLE_UPLOAD_BUCKET

ingestion_api = Blueprint("ingestion", __name__, url_prefix="/ingestion")


def register_ingestion_hooks(app: Eve):
    """Set up ingestion-related hooks on an Eve app instance"""
    app.on_pre_PATCH_assay_uploads = validate_assay_upload_status_update
    app.on_post_PATCH_assay_uploads = on_post_PATCH_assay_uploads


def is_xlsx(filename: str) -> bool:
    """Checks if a filename suggests a file is an .xlsx file"""
    return filename.endswith(".xlsx")


def extract_schema_and_xlsx() -> Tuple[str, str, BinaryIO]:
    """
    Validate that a request has the required structure, then extract 
    the schema id and template file from the request. The request must
    have a multipart/form body with one field "schema" referencing a valid schema id
    and another field "template" with an attached .xlsx file.

    Raises:
        BadRequest: if the above requirements aren't satisfied

    Returns:
        Tuple[str, str, dict]: the schema identifier (aka template type), the schema path, and the open xlsx file
    """
    # If there is no form attribute on the request object,
    # then either one wasn't supplied, or it was malformed
    if not request.form:
        raise BadRequest(
            "Expected form content in request body, or failed to parse form content"
        )

    # If we have a form, check that the expected template file exists
    if "template" not in request.files:
        raise BadRequest("Expected a template file in request body")

    # Check that the template file appears to be a .xlsx file
    xlsx_file = request.files["template"]
    if xlsx_file.filename and not is_xlsx(xlsx_file.filename):
        raise BadRequest("Expected a .xlsx file")

    # Check that a schema id was provided and that a corresponding schema exists
    schema_id = request.form.get("schema")
    if not schema_id:
        raise BadRequest("Expected a form entry for 'schema'")
    schema_id = schema_id.lower()

    if not schema_id in template._TEMPLATE_PATH_MAP:
        raise BadRequest(f"Unknown template type {schema_id}")
    schema_path = template._TEMPLATE_PATH_MAP[schema_id]

    return schema_id, schema_path, xlsx_file


@ingestion_api.route("/validate", methods=["POST"])
@requires_auth(
    "ingestion/validate", [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]
)
def validate_endpoint():
    """
    Separated from `validate` function so that RBAC from requires_auth doesn't affect
    internal invocations of `validate` (in, e.g., the /ingestion/upload_assay endpoint).
    """
    return validate()


def validate():
    """
    Validate a .xlsx manifest or assay metadata template.

    TODO: add this endpoint to the OpenAPI docs
    """
    print(f"validate started")
    # Extract info from the request context
    template_type, _, template_file = extract_schema_and_xlsx()

    # Validate the .xlsx file with respect to the schema
    try:
        error_list = validate_xlsx(
            template_file, template_type, raise_validation_errors=False
        )
    except Exception as e:
        if "unknown template type" in str(e):
            raise BadRequest(str(e))
        raise InternalServerError(str(e))

    json = {"errors": []}
    if type(error_list) == bool and error_list is True:
        print(f"validate passed")
        # The spreadsheet is valid
        return jsonify(json)
    else:
        print(f"{len(error_list)} validation errors: [{error_list[0]!r}, ...]")
        # The spreadsheet is invalid
        json["errors"] = error_list
        return jsonify(json)


def validate_excel_payload(f):
    def wrapped(*args, **kwargs):
        print(f"validate_excel_payload started")
        # Run basic validations on the provided Excel file
        validations = validate()
        if len(validations.json["errors"]) > 0:
            raise BadRequest(validations.json)
        return f(*args, **kwargs)

    wrapped.__name__ = f.__name__
    return wrapped


@ingestion_api.route("/upload_manifest", methods=["POST"])
@requires_auth(
    "ingestion/upload_manifest", [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]
)
@validate_excel_payload
def upload_manifest():
    """
    Ingest manifest data from an excel spreadsheet.

    * API tries to load existing trial metadata blob (if fails, merge request fails; nothing saved).
    * API merges the merge request JSON into the trial metadata (if fails, merge request fails; nothing saved).
    * The manifest xlsx file is upload to the GCS uploads bucket and goes to Downloadable files.
    * The merge request parsed JSON is saved to `ManifestUploads`.
    * The updated trial metadata object is updated in the `TrialMetadata` table.

    Request: multipart/form
        schema: the schema identifier for this template
        template: the .xlsx file to process'
    Response:
        201 if the upload succeeds. Otherwise, some error status code and message.
    """
    upload_moment = datetime.datetime.now().isoformat()

    schema_hint, schema_path, xlsx_file = extract_schema_and_xlsx()

    md_patch, file_infos = prism.prismify(xlsx_file, schema_path, schema_hint)

    try:
        trial_id = md_patch[TRIAL_ID_FIELD]
    except KeyError:
        raise BadRequest(f"{TRIAL_ID_FIELD} field not found.")

    try:
        trial = TrialMetadata.patch_manifest(trial_id, md_patch, commit=False)
    except NoResultFound as e:
        raise BadRequest(f"Trial with {TRIAL_ID_FIELD}={trial_id} not found.")

    xlsx_file.seek(0)
    gcs_blob = gcloud_client.upload_xlsx_to_gcs(
        trial_id, "manifest", schema_hint, xlsx_file, upload_moment
    )
    # TODO maybe rely on default session
    session = Session.object_session(trial)
    # TODO move to prism
    DownloadableFiles.create_from_metadata(
        trial_id,
        assay_type=schema_hint,
        file_metadata={
            "artifact_category": "Manifest File",
            "object_url": gcs_blob.name,
            "file_name": gcs_blob.name,
            "file_size_bytes": gcs_blob.size,
            "md5_hash": gcs_blob.md5_hash,
            "uploaded_timestamp": upload_moment,
            "data_format": "XLSX",
        },
        session=session,
        commit=False,
    )

    user_email = _request_ctx_stack.top.current_user.email
    manifest_upload = ManifestUploads.create(
        manifest_type=schema_hint,
        uploader_email=user_email,
        metadata=md_patch,
        gcs_xlsx_uri=gcs_blob.name,
        session=session,
    )

    return jsonify({"metadata_json_patch": md_patch})


@ingestion_api.route("/upload_assay", methods=["POST"])
@requires_auth(
    "ingestion/upload_assay", [CIDCRole.ADMIN.value, CIDCRole.CIMAC_BIOFX_USER.value]
)
@validate_excel_payload
def upload_assay():
    """
    Initiate an assay metadata/data ingestion job.

    Request: multipart/form
        schema: the schema identifier for this template
        template: the .xlsx file to process
    Response: application/json
        url_mapping: a mapping from client's local filepaths to GCS object names
        to which they've been granted access.
        gcs_bucket: the bucket to upload objects to.
        job_id: the unique identifier for this upload job in the database
        job_etag: the job record's etag, required by Eve for safe updates
    
    # TODO: refactor this to be a pre-GET hook on the upload-jobs resource.
    """
    print(f"upload_assay started")
    schema_hint, schema_path, xlsx_file = extract_schema_and_xlsx()

    # Extract the clinical trial metadata blob contained in the .xlsx file,
    # along with information about the files the template references.
    md_patch, file_infos = prism.prismify(xlsx_file, schema_path, schema_hint)

    try:
        trial_id = md_patch[TRIAL_ID_FIELD]
    except KeyError:
        print(f"{TRIAL_ID_FIELD} field not found in patch {md_patch}.")
        raise BadRequest(f"{TRIAL_ID_FIELD} field not found.")

    trial = TrialMetadata.find_by_trial_id(trial_id)
    if trial is None:
        print(f"Trial with {TRIAL_ID_FIELD}={trial_id} not found.")
        raise BadRequest(f"Trial with {TRIAL_ID_FIELD}={trial_id} not found.")

    upload_moment = datetime.datetime.now().isoformat()
    uri2uuid = {}
    url_mapping = {}
    for file_info in file_infos:
        uuid = file_info.upload_placeholder

        # Build the path to the "directory" in GCS where the
        # local file should be uploaded. Attach a timestamp (upload_moment)
        # to prevent collisions with previous uploads of this file.
        gcs_uri = f"{file_info.gs_key}/{upload_moment}"

        uri2uuid[gcs_uri] = uuid

        if file_info.local_path in url_mapping:
            raise RuntimeError(
                f"File {file_info.local_path} came twice.\nEach local file should be used only once."
            )
        url_mapping[file_info.local_path] = gcs_uri

    # Upload the xlsx template file to GCS
    xlsx_file.seek(0)
    gcs_blob = gcloud_client.upload_xlsx_to_gcs(
        trial_id, "assays", schema_hint, xlsx_file, upload_moment
    )

    # Save the upload job to the database
    user_email = _request_ctx_stack.top.current_user.email
    job = AssayUploads.create(
        schema_hint, user_email, uri2uuid, md_patch, gcs_blob.name
    )

    # Grant the user upload access to the upload bucket
    gcloud_client.grant_upload_access(GOOGLE_UPLOAD_BUCKET, user_email)

    response = {
        "job_id": job.id,
        "job_etag": job._etag,
        "url_mapping": url_mapping,
        "gcs_bucket": GOOGLE_UPLOAD_BUCKET,
    }
    return jsonify(response)


@ingestion_api.route("/poll_upload_merge_status", methods=["GET"])
@requires_auth(
    "ingestion/poll_upload_merge_status",
    [CIDCRole.ADMIN.value, CIDCRole.CIMAC_BIOFX_USER.value],
)
def poll_upload_merge_status():
    """
    Check an assay upload's status, and supply the client with directions on when to retry the check.

    Request: no body
        query parameter "id": the id of the assay_upload of interest
    Response: application/json
        status {str or None}: the current status of the assay_upload (empty if not MERGE_FAILED or MERGE_COMPLETED)
        status_details {str or None}: information about `status` (e.g., error details). Only present if `status` is present.
        retry_in {str or None}: the time in seconds to wait before making another request to this endpoint (empty if `status` has a value)
    Raises:
        400: no "id" query parameter is supplied
        401: the requesting user did not create the requested upload job
        404: no upload job with id "id" is found
    """
    upload_id = request.args.get("id")
    if not upload_id:
        raise BadRequest("Missing expected query parameter 'id'")

    user = _request_ctx_stack.top.current_user
    upload = AssayUploads.find_by_id(upload_id, user.email)
    if not upload:
        raise NotFound(f"Could not find assay upload job with id {upload_id}")

    if upload.status in [
        AssayUploadStatus.MERGE_COMPLETED.value,
        AssayUploadStatus.MERGE_FAILED.value,
    ]:
        return jsonify(
            {"status": upload.status, "status_details": upload.status_details}
        )

    # TODO: get smarter about retry-scheduling
    return jsonify({"retry_in": 5})


def validate_assay_upload_status_update(request: Request, _: dict):
    """Event hook ensuring a user is requesting a valid upload job status transition"""
    # Extract the target status
    upload_patch = request.json
    target_status = upload_patch.get("status")
    if not target_status:
        # Let Eve's input validation handle this
        return

    # Look up the current status
    user = _request_ctx_stack.top.current_user
    upload_id = request.view_args["id"]
    upload = AssayUploads.find_by_id(upload_id, user.email)
    if not upload:
        raise NotFound(f"Could not find assay upload job with id {upload_id}")

    # Check that the requested status update is valid
    if not AssayUploadStatus.is_valid_transition(upload.status, target_status):
        raise BadRequest(
            f"Cannot set assay upload status to '{target_status}': "
            f"current status is '{upload.status}'"
        )


def on_post_PATCH_assay_uploads(request: Request, payload: Response):
    """Revoke the user's write access to the objects they've uploaded to."""
    if not payload.json or not "id" in payload.json:
        raise BadRequest("Unexpected payload while updating assay_uploads")

    # TODO: handle the case where the user has more than one upload running,
    # in which case we shouldn't revoke the user's write access until they
    # have no remaining jobs with status "started".

    job_id = payload.json["id"]
    status = request.json["status"]

    # If this is a successful upload job, publish this info to Pub/Sub
    if status == AssayUploadStatus.UPLOAD_COMPLETED.value:
        gcloud_client.publish_upload_success(job_id)

    # Revoke the user's write access
    user_email = _request_ctx_stack.top.current_user.email
    gcloud_client.revoke_upload_access(GOOGLE_UPLOAD_BUCKET, user_email)


# @ingestion_api.route("/signed-upload-urls", methods=["POST"])
# NOTE: this endpoint isn't used currently, so it's not added to the API.
def signed_upload_urls():
    """
    NOTE: We will use IAM for managing bucket access instead of signed URLs, 
    because this will allow us to leverage gsutil on the client side. This
    endpoint isn't currently in use.

    Given a request whose body contains a directory name and a list of object names,
    return a JSON object mapping object names to signed GCS upload URLs for those objects.

    Note: a signed URL gives time-restricted, method-restricted access to one of our GCS
    storage buckets

    TODO: In the long run, this endpoint *needs* user-level rate-limiting or similar. If we don't keep 
    track of how recently we've issued signed URLs to a certain user, then that user can
    keep acquiring signed URLs over and over, effectively circumventing the time-restrictions
    built into these URLs. For now, though, since only people on the development team are
    registered in the app, we don't need to worry about this.

    Sample request body:
    {
        "directory_name": "my-assay-run-id",
        "object_names": ["my-fastq-1.fastq.gz", "my-fastq-2.fastq.gz"]
    }

    Sample response body:
    {
        "my-fastq-1.fastq.gz": [a signed URL with PUT permissions],
        "my-fastq-2.fastq.gz": [a signed URL with PUT permissions]
    }
    """
    # Validate the request body
    if not request.json:
        raise BadRequest("expected JSON request body.")
    if not "directory_name" in request.json and "object_names" in request.json:
        raise BadRequest(
            "expected keys 'directory_name' and 'object_names' in request body."
        )

    directory_name = request.json["directory_name"]
    object_urls = {}
    # Build up the mapping of object names to buckets
    for object_name in request.json["object_names"]:
        # Prepend objects with the given directory name
        full_object_name = f"{directory_name}/{object_name}"
        object_url = gcloud_client.get_signed_url(full_object_name)
        object_urls[object_name] = object_url

    return jsonify(object_urls)
