"""
Endpoints for validating and ingesting metadata and data.
"""
import os, sys
import json
import datetime
from typing import BinaryIO, Tuple, List, NamedTuple
from functools import wraps

from werkzeug.exceptions import BadRequest, InternalServerError, NotFound, Unauthorized

from eve import Eve
from sqlalchemy.orm.exc import NoResultFound
from jsonschema.exceptions import ValidationError
from sqlalchemy.orm.session import Session
from flask import Blueprint, request, Request, Response, jsonify, _request_ctx_stack

from cidc_schemas import constants, prism
from cidc_schemas.template import Template
from cidc_schemas.template_reader import XlTemplateReader

import gcloud_client
from models import (
    AssayUploads,
    AssayUploadStatus,
    TrialMetadata,
    DownloadableFiles,
    ManifestUploads,
    Permissions,
    CIDCRole,
    Users,
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
        Tuple[Template, BinaryIO]: template, and the open xlsx file
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

    try:
        template = Template.from_type(schema_id)
    except Exception:
        raise BadRequest(f"Unknown template type {schema_id!r}")

    return template, xlsx_file


def validate(template, xlsx):
    """
    Validate a .xlsx manifest or assay metadata template.

    TODO: add this endpoint to the OpenAPI docs
    """
    print(f"validate started")
    # Validate the .xlsx file with respect to the schema
    error_list = list(xlsx.iter_errors(template))
    json = {"errors": []}
    if not error_list:
        print(f"validate passed")
        return jsonify(json)
    else:
        print(f"{len(error_list)} validation errors: [{error_list[0]!r}, ...]")
        # The spreadsheet is invalid
        json["errors"] = error_list
        return jsonify(json)


def check_permissions(user, trial_id, template_type):
    """
    Check that the given user has permissions to access the given trial / template type.

    If no trial exists with this ID, raise a 404.
    If no permission exists for this user-trial-template_type trio, raise a 401.
    """
    perm = Permissions.find_for_user_trial_type(user, trial_id, template_type)
    if not perm and user.role != CIDCRole.ADMIN.value:
        print(f"Unauthorized attempt to access trial {trial_id} by {user.email!r}")
        raise Unauthorized(
            f"{user.email} is not authorized to upload {template_type} data to {trial_id}. "
            f"Please contact a CIDC administrator if you believe this is a mistake."
        )


def upload_handler(f):
    """
    Extracts and validates the xlsx file from the request form body,
    prismifies the xlsx file, checks that the current user has
    permission to complete this upload, then passes relevant data
    along to `f` as positional arguments.

    This decorator factors out common code from `upload_manifest` and `upload_assay`.
    """

    @wraps(f)
    def wrapped(*args, **kwargs):
        print(f"upload_handler({f.__name__}) started")
        template, xlsx_file = extract_schema_and_xlsx()

        errors_so_far = []

        xlsx, errors = XlTemplateReader.from_excel(xlsx_file)
        print(f"xlsx parsed: {len(errors)} errors")
        if errors:
            errors_so_far.extend(errors)

        # Run basic validations on the provided Excel file
        validations = validate(template, xlsx)
        if len(validations.json["errors"]) > 0:
            errors_so_far.extend(validations.json["errors"])
        print(f"xlsx validated: {len(validations.json['errors'])} errors")

        md_patch, file_infos, errors = prism.prismify(xlsx, template)
        if errors:
            errors_so_far.extend(errors)
        print(f"prismified: {len(errors)} errors, {len(file_infos)} file_infos")

        try:
            trial_id = md_patch[prism.PROTOCOL_ID_FIELD_NAME]
        except KeyError:
            errors_so_far.append(f"{prism.PROTOCOL_ID_FIELD_NAME} field not found.")
            # we can't find trial id so we can't proceed
            raise BadRequest({"errors": [str(e) for e in errors_so_far]})

        trial = TrialMetadata.find_by_trial_id(trial_id)
        if not trial:
            errors_so_far.insert(
                0, f"Trial with {prism.PROTOCOL_ID_FIELD_NAME}={trial_id} not found."
            )
            # we can't find trial so we can't proceed trying to check_perm or merge
            raise BadRequest({"errors": [str(e) for e in errors_so_far]})

        user = _request_ctx_stack.top.current_user
        try:
            check_permissions(user, trial_id, template.type)
        except Unauthorized as e:
            errors_so_far.insert(0, e.description)
            # unauthorized to pull trial so we can't proceed trying to merge
            raise Unauthorized({"errors": [str(e) for e in errors_so_far]})

        # Try to merge assay metadata into the existing clinical trial metadata
        # Ignoring result as we inly want to check there's no validation errors
        try:
            merged_md, errors = prism.merge_clinical_trial_metadata(
                md_patch, trial.metadata_json
            )
        except ValidationError as e:
            errors_so_far.append(f"{e.message} in {e.instance}")
        except prism.MergeCollisionException as e:
            errors_so_far.append(str(e))
        except prism.InvalidMergeTargetException as e:
            # we have an invalid MD stored in db - users can't do anything about it.
            # So we log it
            print(f"Internal error with trial {trial_id!r}", file=sys.stderr)
            print(e, file=sys.stderr)
            # and return an error. Though it's not BadRequest but rather an
            # Internal Server error we report it like that, so it will be displayed
            raise BadRequest(
                f"Internal error with {trial_id!r}. Please contact a CIDC Administrator."
            ) from e
        print(f"merged: {len(errors)} errors")
        if errors:
            errors_so_far.extend(errors)

        if errors_so_far:
            raise BadRequest({"errors": [str(e) for e in errors_so_far]})

        return f(
            user, trial, template.type, xlsx_file, md_patch, file_infos, *args, **kwargs
        )

    return wrapped


@ingestion_api.route("/validate", methods=["POST"])
@requires_auth(
    "ingestion/validate", [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]
)
@upload_handler
def validate_endpoint(*args, **kwargs):
    # Validation is done within `upload_handler`
    # so we just return ok here
    return jsonify({"errors": []})


@ingestion_api.route("/upload_manifest", methods=["POST"])
@requires_auth(
    "ingestion/upload_manifest", [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]
)
@upload_handler
def upload_manifest(
    user: Users,
    trial: TrialMetadata,
    template_type: str,
    xlsx_file: BinaryIO,
    md_patch: dict,
    file_infos: List[prism.LocalFileUploadEntry],
):
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

    try:
        trial = TrialMetadata.patch_manifest(trial.trial_id, md_patch, commit=False)
    except ValidationError as e:
        raise BadRequest(f"{e.message} in {e.instance}")
    except ValidationMultiError as e:
        raise BadRequest({"errors": e.args[0]})

    gcs_blob = gcloud_client.upload_xlsx_to_gcs(
        trial.trial_id, "manifest", template_type, xlsx_file, upload_moment
    )
    # TODO maybe rely on default session
    session = Session.object_session(trial)

    DownloadableFiles.create_from_blob(
        trial.trial_id,
        template_type,
        "Shipping Manifest",
        gcs_blob,
        session=session,
        commit=False,
    )

    manifest_upload = ManifestUploads.create(
        manifest_type=template_type,
        uploader_email=user.email,
        metadata=md_patch,
        gcs_xlsx_uri=gcs_blob.name,
        session=session,
    )

    # Publish that this trial's metadata has been updated
    gcloud_client.publish_patient_sample_update(trial.trial_id)

    return jsonify({"metadata_json_patch": md_patch})


@ingestion_api.route("/upload_assay", methods=["POST"])
@requires_auth(
    "ingestion/upload_assay", [CIDCRole.ADMIN.value, CIDCRole.CIMAC_BIOFX_USER.value]
)
@upload_handler
def upload_assay(
    user: Users,
    trial: TrialMetadata,
    template_type: str,
    xlsx_file: BinaryIO,
    md_patch: dict,
    file_infos: List[prism.LocalFileUploadEntry],
):
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
        extra_metadata: files with extra metadata information (only applicable to few assays), else None
    
    # TODO: refactor this to be a pre-GET hook on the upload-jobs resource.
    """
    print(f"upload_assay started")
    upload_moment = datetime.datetime.now().isoformat()
    uri2uuid = {}
    url_mapping = {}
    files_with_extra_md = {}
    for file_info in file_infos:
        uuid = file_info.upload_placeholder

        # Build the path to the "directory" in GCS where the
        # local file should be uploaded. Attach a timestamp (upload_moment)
        # to prevent collisions with previous uploads of this file.
        gcs_uri = f"{file_info.gs_key}/{upload_moment}"

        uri2uuid[gcs_uri] = uuid

        if file_info.local_path in url_mapping:
            raise BadRequest(
                f"File {file_info.local_path} came twice.\nEach local file should be used only once."
            )
        url_mapping[file_info.local_path] = gcs_uri

        if file_info.metadata_availability:
            files_with_extra_md[file_info.local_path] = file_info.upload_placeholder

    gcs_blob = gcloud_client.upload_xlsx_to_gcs(
        trial.trial_id, "assays", template_type, xlsx_file, upload_moment
    )

    # Save the upload job to the database
    job = AssayUploads.create(
        template_type, user.email, uri2uuid, md_patch, gcs_blob.name
    )

    # Grant the user upload access to the upload bucket
    gcloud_client.grant_upload_access(GOOGLE_UPLOAD_BUCKET, user.email)

    response = {
        "job_id": job.id,
        "job_etag": job._etag,
        "url_mapping": url_mapping,
        "gcs_bucket": GOOGLE_UPLOAD_BUCKET,
        "extra_metadata": None,
    }
    if bool(files_with_extra_md):
        response["extra_metadata"] = files_with_extra_md

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
    upload = AssayUploads.find_by_id_and_email(upload_id, user.email)
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
    upload = AssayUploads.find_by_id_and_email(upload_id, user.email)
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


@ingestion_api.route("/extra-assay-metadata", methods=["POST"])
def extra_assay_metadata():
    """

    Extracts:
        job_id, and extra_metadata_file from request body
    Raises:
        BadRequest: if the request requirements aren't satisfied

    request.form = {
        'job_id': the job_id to update the patch for,
    }

    request.files = {
        [artifact_uuid_1]: [open extra metadata file 1],
        [artifact_uuid_2]: [open extra metadata file 2]
    }
    """

    if not request.form:
        raise BadRequest(
            "Expected form content in request body, or failed to parse form content"
        )

    if "job_id" not in request.form:
        raise BadRequest("Expected job_id in form")

    if not request.files:
        raise BadRequest(
            "Expected files in request (mapping from artifact uuids to open files)"
        )

    job_id = request.form["job_id"]

    files = request.files.to_dict()

    try:
        AssayUploads.merge_extra_metadata(job_id, files)
    except Exception as e:
        # TODO see if it's validation sort of error and return BadRequest
        raise e

    # TODO: return something here?
    return jsonify({})
