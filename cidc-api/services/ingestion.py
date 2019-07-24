"""
Endpoints for validating and ingesting metadata and data.
"""
import os
import json
import datetime
from typing import BinaryIO, Tuple, List

from werkzeug.exceptions import BadRequest, InternalServerError, NotImplemented

from google.cloud import storage
from eve import Eve
from eve.auth import requires_auth
from flask import Blueprint, request, Request, Response, jsonify, _request_ctx_stack
from cidc_schemas import constants, validate_xlsx, prism

import gcs_iam
from models import UploadJobs, STATUSES
from settings import GOOGLE_UPLOAD_BUCKET, HINT_TO_SCHEMA, SCHEMA_TO_HINT

ingestion_api = Blueprint("ingestion", __name__, url_prefix="/ingestion")


def register_ingestion_hooks(app: Eve):
    """Set up ingestion-related hooks on an Eve app instance"""
    app.on_post_PATCH_upload_jobs = on_post_PATCH_upload_jobs


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
        Tuple[str, str, dict]: the schema hint, the schema path, and the open xlsx file
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
    if schema_id in HINT_TO_SCHEMA:
        schema_hint = schema_id
        schema_path = HINT_TO_SCHEMA[schema_hint]
    elif schema_id in SCHEMA_TO_HINT:
        schema_hint = SCHEMA_TO_HINT[schema_id]
        schema_path = schema_id
    else:
        raise BadRequest(f"No known schema with id {schema_id}")

    return schema_hint, schema_path, xlsx_file


@ingestion_api.route("/validate", methods=["POST"])
@requires_auth("ingestion.validate")
def validate():
    """
    Validate a .xlsx manifest or assay metadata template.

    TODO: add this endpoint to the OpenAPI docs
    """
    # Extract info from the request context
    _, schema_path, template_file = extract_schema_and_xlsx()

    # Validate the .xlsx file with respect to the schema
    try:
        error_list = validate_xlsx(template_file, schema_path, False)
    except Exception as e:
        # TODO: log the traceback for this error
        raise InternalServerError(str(e))

    json = {"errors": []}
    if type(error_list) == bool:
        # The spreadsheet is valid
        return jsonify(json)
    else:
        # The spreadsheet is invalid
        json["errors"] = error_list
        return jsonify(json)


@ingestion_api.route("/upload", methods=["POST"])
@requires_auth("ingestion.upload")
def upload():
    """
    Initiate a metadata/data ingestion job.

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
    # Run basic validations on the provided Excel file
    validations = validate()
    if len(validations.json["errors"]) > 0:
        return validations

    schema_hint, schema_path, xlsx_file = extract_schema_and_xlsx()

    # TODO: this path-resolution should happen internally in prism
    full_schema_path = os.path.join(constants.SCHEMA_DIR, schema_path)

    # Extract the clinical trial metadata blob contained in the .xlsx file,
    # along with information about the files the template references.
    metadata_json, file_infos = prism.prismify(xlsx_file, full_schema_path, schema_hint)

    upload_moment = str(datetime.datetime.now()).replace(" ", "_")
    url_mapping = {}
    for file_info in file_infos:
        gcs_uri_dir, local_path = file_info["gs_key"], file_info["local_path"]

        # Build the path to the "directory" in GCS where the
        # local file should be uploaded. Attach a timestamp (upload_moment)
        # to prevent collisions with previous uploads of this file.
        gcs_uri_prefix = f"{gcs_uri_dir}/{upload_moment}"

        # Store the full path to GCS object for this file
        # in the url mapping to be sent back to the user.
        gcs_uri = f"{gcs_uri_prefix}/{local_path}"
        url_mapping[local_path] = gcs_uri

    # Save the upload job to the database
    xlsx_bytes = xlsx_file.read()
    gcs_uris = url_mapping.values()
    user_email = _request_ctx_stack.top.current_user.email
    job = UploadJobs.create(user_email, gcs_uris, metadata_json)

    # Grant the user upload access to the upload bucket
    gcs_iam.grant_upload_access(GOOGLE_UPLOAD_BUCKET, user_email)

    response = {
        "job_id": job.id,
        "job_etag": job._etag,
        "url_mapping": url_mapping,
        "gcs_bucket": GOOGLE_UPLOAD_BUCKET,
    }
    return jsonify(response)


def on_post_PATCH_upload_jobs(request: Request, payload: Response):
    """Revoke the user's write access to the objects they've uploaded to."""
    if not payload.json and not "id" in payload.json:
        raise BadRequest("Unexpected payload while updating upload_jobs")

    # TODO: handle the case where the user has more than one upload running,
    # in which case we shouldn't revoke the user's write access until they
    # have no remaining jobs with status "started". This will require
    # adding a "created_by" field or similar to the upload_jobs object.

    # Revoke the user's write access
    user_email = _request_ctx_stack.top.current_user.email
    gcs_iam.revoke_upload_access(GOOGLE_UPLOAD_BUCKET, user_email)


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
        object_url = get_signed_url(full_object_name)
        object_urls[object_name] = object_url

    return jsonify(object_urls)


def get_signed_url(object_name: str, method: str = "PUT", expiry_mins: int = 5) -> str:
    """
    Generate a signed URL for `object_name` to give a client temporary access.

    See: https://cloud.google.com/storage/docs/access-control/signing-urls-with-helpers
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(GOOGLE_UPLOAD_BUCKET)
    blob = bucket.blob(object_name)

    # Generate the signed URL, allowing a client to use `method` for `expiry_mins` minutes
    expiration = datetime.timedelta(minutes=expiry_mins)
    url = blob.generate_signed_url(version="v4", expiration=expiration, method=method)

    return url
