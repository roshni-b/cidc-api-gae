"""
Endpoints for validating and ingesting metadata and data.
"""
import os
import json
import datetime
from typing import BinaryIO, Tuple, List

from werkzeug.exceptions import BadRequest, InternalServerError, NotImplemented

from eve import Eve
from eve.auth import requires_auth
from sqlalchemy.orm.exc import NoResultFound
from flask import Blueprint, request, Request, Response, jsonify, _request_ctx_stack
from cidc_schemas import constants, validate_xlsx, prism, template

import gcloud_client
from models import AssayUploads, STATUSES, TRIAL_ID_FIELD, TrialMetadata
from config.settings import GOOGLE_UPLOAD_BUCKET

ingestion_api = Blueprint("ingestion", __name__, url_prefix="/ingestion")


def register_ingestion_hooks(app: Eve):
    """Set up ingestion-related hooks on an Eve app instance"""
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
@requires_auth("ingestion.validate")
def validate():
    """
    Validate a .xlsx manifest or assay metadata template.

    TODO: add this endpoint to the OpenAPI docs
    """
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
        # The spreadsheet is valid
        return jsonify(json)
    else:
        # The spreadsheet is invalid
        json["errors"] = error_list
        return jsonify(json)


def validate_excel_payload(f):
    def wrapped(*args, **kwargs):
        # Run basic validations on the provided Excel file
        validations = validate()
        if len(validations.json["errors"]) > 0:
            return BadRequest(validations)
        return f(*args, **kwargs)

    wrapped.__name__ = f.__name__
    return wrapped


@ingestion_api.route("/upload_manifest", methods=["POST"])
@requires_auth("ingestion.upload_manifest")
@validate_excel_payload
def upload_manifest():
    """
    Ingest manifest data from an excel spreadsheet.

    * API tries to load existing trial metadata blob (if fails, merge request fails; nothing saved).
    * API merges the merge request JSON into the trial metadata (if fails, merge request fails; nothing saved).
    - The manifest xlsx file is upload to the GCS uploads bucket and goes to Downloadable files.
    - The merge request parsed JSON is saved to `ManifestUploads`.
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

    if len(file_infos) > 0:
        raise BadRequest(f"Shipping manifests shouldn't reference any local files.")

    try:
        trial_id = md_patch[TRIAL_ID_FIELD]
    except KeyError:
        raise BadRequest(f"No {TRIAL_ID_FIELD} parsed from template.")

    xlsx_file.seek(0)
    gcs_xlsx_uri = gcloud_client.upload_xlsx_to_gcs(
        trial_id, "manifest", schema_hint, xlsx_file, upload_moment
    )

    try:
        TrialMetadata.patch_manifest(trial_id, md_patch)
    except NoResultFound as e:
        raise BadRequest(f"Trial with {TRIAL_ID_FIELD} {trial_id} not found.")

    return jsonify({"metadata_json_patch": md_patch})


@ingestion_api.route("/upload_assay", methods=["POST"])
@requires_auth("ingestion.upload_assay")
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
    schema_hint, schema_path, xlsx_file = extract_schema_and_xlsx()

    # Extract the clinical trial metadata blob contained in the .xlsx file,
    # along with information about the files the template references.
    metadata_json, file_infos = prism.prismify(xlsx_file, schema_path, schema_hint)

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
    gcs_xlsx_uri = gcloud_client.upload_xlsx_to_gcs(
        metadata_json[TRIAL_ID_FIELD], "assays", schema_hint, xlsx_file, upload_moment
    )

    # Save the upload job to the database
    user_email = _request_ctx_stack.top.current_user.email
    job = AssayUploads.create(
        schema_hint, user_email, uri2uuid, metadata_json, gcs_xlsx_uri
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
    if status == "completed":
        gcloud_client.publish_upload_success(job_id)

    # Revoke the user's write access
    user_email = _request_ctx_stack.top.current_user.email
    gcloud_client.revoke_upload_access(GOOGLE_UPLOAD_BUCKET, user_email)


@ingestion_api.route("/signed-upload-urls", methods=["POST"])
@requires_auth("ingestion.signed-upload-urls")
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
