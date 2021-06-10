import datetime
from typing import BinaryIO, Tuple, List
from functools import wraps

from marshmallow import Schema, INCLUDE
from webargs import fields
from webargs.flaskparser import use_args
from flask import Blueprint, request, jsonify
from jsonschema.exceptions import ValidationError
from sqlalchemy.orm.session import Session
from werkzeug.exceptions import BadRequest, NotFound, Unauthorized, PreconditionRequired

from cidc_schemas import prism, json_validation
from cidc_schemas.template import Template
from cidc_schemas.template_reader import (
    XlTemplateReader,
    ValidationError as SchemasValidationError,
)

from ..shared import gcloud_client, emails
from ..shared.auth import requires_auth, get_current_user, authenticate_and_get_user
from ..shared.rest_utils import (
    with_lookup,
    lookup,
    marshal_response,
    unmarshal_request,
    use_args_with_pagination,
)
from ..config.settings import GOOGLE_UPLOAD_BUCKET, PRISM_ENCRYPT_KEY
from ..models import (
    UploadJobs,
    UploadJobSchema,
    UploadJobListSchema,
    UploadJobStatus,
    TrialMetadata,
    UploadJobs,
    Permissions,
    CIDCRole,
    Users,
    ValidationMultiError,
)
from ..config.logging import get_logger

logger = get_logger(__name__)

prism.set_prism_encrypt_key(PRISM_ENCRYPT_KEY)

# TODO: consolidate ingestion blueprint into upload_jobs blueprint
ingestion_bp = Blueprint("ingestion", __name__)
upload_jobs_bp = Blueprint("upload_jobs", __name__)

upload_job_schema = UploadJobSchema()
upload_job_list_schema = UploadJobListSchema()

### UploadJobs REST methods ###
upload_job_roles = [
    CIDCRole.ADMIN.value,
    CIDCRole.CIMAC_BIOFX_USER.value,
    CIDCRole.NCI_BIOBANK_USER.value,
    CIDCRole.CIDC_BIOFX_USER.value,
]


@upload_jobs_bp.route("/", methods=["GET"])
@requires_auth("upload_jobs", upload_job_roles)
@use_args_with_pagination({}, upload_job_schema)
@marshal_response(upload_job_list_schema)
def list_upload_jobs(args, pagination_args):
    """List visible upload_job records."""
    user = get_current_user()

    def filter_jobs(q):
        if not user.is_admin():
            return q.filter(UploadJobs.uploader_email == user.email)
        return q

    jobs = UploadJobs.list(filter_=filter_jobs, **pagination_args)
    count = UploadJobs.count(filter_=filter_jobs)

    return {"_items": jobs, "_meta": {"total": count}}


@upload_jobs_bp.route("/<int:upload_job>", methods=["GET"])
@requires_auth("upload_jobs", upload_job_roles)
@with_lookup(UploadJobs, "upload_job")
@marshal_response(upload_job_schema)
def get_upload_job(upload_job: UploadJobs):
    """Get an upload_job by ID. Non-admins can only view their own upload_jobs."""
    user = get_current_user()
    if not user.is_admin() and upload_job.uploader_email != user.email:
        raise NotFound()

    # this is not user-input due to @with_lookup, so safe to return
    return upload_job


def requires_upload_token_auth(endpoint):
    """
    Decorator that adds "upload token" authentication to an endpoint.
    The provided endpoint must include the upload job id as a URL param, i.e.,
    `<int:upload_job>`. This upload job ID is used to look up the relevant upload_job
    and check its `token` field against the user-provided `token` query parameter.
    If authentication and upload job record lookup succeeds, pass the upload job record
    in the `upload_job` kwarg to `endpoint`.
    """
    # Flag this endpoint as authenticated
    endpoint.is_protected = True

    token_schema = Schema.from_dict({"token": fields.Str(required=True)})(
        # Don't throw an error if there are unknown query params in addition to "token"
        unknown=INCLUDE
    )

    @wraps(endpoint)
    @use_args(token_schema, location="query")
    def wrapped(args, *pos_args, **kwargs):
        # Attempt identity token authentication to get user info
        user = authenticate_and_get_user()

        try:
            upload_job = lookup(
                UploadJobs, kwargs["upload_job"], check_etag=request.method == "PATCH"
            )
        except (PreconditionRequired, NotFound) as e:
            # If there's an authenticated user associated with this request,
            # raise errors thrown by `lookup`. Otherwise, just report that auth failed.
            if user:
                raise e
            else:
                raise Unauthorized("upload_job token authentication failed")

        # Check that the user-provided upload token matches the saved upload token
        token = args["token"]
        if str(token) != str(upload_job.token):
            raise Unauthorized("upload_job token authentication failed")

        # Pass the looked-up upload_job record to `endpoint` via the `upload_job` keyword argument
        kwargs["upload_job"] = upload_job

        return endpoint(*pos_args, **kwargs)

    return wrapped


@upload_jobs_bp.route("/<int:upload_job>", methods=["PATCH"])
@requires_upload_token_auth
@unmarshal_request(
    UploadJobSchema(only=["status", "gcs_file_map", "token"]),
    "upload_job_updates",
    load_sqla=False,
)
@marshal_response(upload_job_schema, 200)
def update_upload_job(upload_job: UploadJobs, upload_job_updates: dict):
    """Update an upload_job."""
    try:
        if "gcs_file_map" in upload_job_updates and upload_job.gcs_file_map is not None:
            upload_job_updates["metadata_patch"] = upload_job.metadata_patch.copy()
            for uri, uuid in upload_job.gcs_file_map.items():
                if uri not in upload_job_updates["gcs_file_map"]:
                    upload_job_updates[
                        "metadata_patch"
                    ] = _remove_optional_uuid_recursive(
                        upload_job_updates["metadata_patch"], uuid
                    )

        upload_job.update(changes=upload_job_updates)
    except ValueError as e:
        raise BadRequest(str(e))

    # If this is a successful upload job, publish this info to Pub/Sub
    if upload_job.status == UploadJobStatus.UPLOAD_COMPLETED.value:
        gcloud_client.publish_upload_success(upload_job.id)

    # Revoke the uploading user's bucket access, since their querying
    # this endpoint indicates a completed / failed upload attempt.
    gcloud_client.revoke_upload_access(upload_job.uploader_email)

    # this is not user-input due to @with_lookup, so safe to return
    return upload_job


def _remove_optional_uuid_recursive(target: dict, uuid: str):
    """
    If target contains an item : dict with {"upload_placeholder":uuid}, removes that item and returns the modified target
    If no such item is found, continues recursively as depth first search
    If the uuid is never found, returns the target unchanged
    """
    if isinstance(target, dict):
        if target.get("upload_placeholder") == uuid:
            return {}

        for k, v in target.items():
            if (
                isinstance(v, dict)
                and "upload_placeholder" in v
                and v["upload_placeholder"] == uuid
            ):
                target.pop(k)
                return target
            elif isinstance(v, (dict, list)):
                temp = _remove_optional_uuid_recursive(v, uuid)
                if len(temp):
                    target[k] = temp
                else:
                    # drop completely if empty
                    target.pop(k)
                    return target

    elif isinstance(target, list):
        temp = [_remove_optional_uuid_recursive(i, uuid) for i in target]
        target = [t for t in temp if t]  # remove None or empty

    return target


### Ingestion endpoints ###


def is_xlsx(filename: str) -> bool:
    """Checks if a filename suggests a file is an .xlsx file"""
    return filename.endswith(".xlsx")


def extract_schema_and_xlsx(allowed_types: List[str]) -> Tuple[str, BinaryIO]:
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

    # Check that the schema type is allowed
    if schema_id not in allowed_types:
        raise BadRequest(
            f"Schema type '{schema_id}' is not supported for this endpoint. Available options: {allowed_types}"
        )

    template = Template.from_type(schema_id)

    return template, xlsx_file


def validate(template, xlsx):
    """
    Validate a .xlsx manifest or assay metadata template.

    TODO: add this endpoint to the OpenAPI docs
    """
    logger.info(f"validate started")
    # Validate the .xlsx file with respect to the schema
    error_list = list(xlsx.iter_errors(template))
    json = {"errors": []}
    if not error_list:
        logger.info(f"validate passed")
        return jsonify(json)
    else:
        logger.error(f"{len(error_list)} validation errors: [{error_list[0]!r}, ...]")
        # The spreadsheet is invalid
        json["errors"] = error_list
        return jsonify(json)


def check_permissions(user, trial_id, template_type):
    """
    Check that the given user has permissions to upload to the given trial / template type.

    If no trial exists with this ID, raise a 404.
    If no permission exists for this user-trial-template_type trio, raise a 401.
    """
    perm = Permissions.find_for_user_trial_type(user.id, trial_id, template_type)
    # Admins don't need permissions
    if user.is_admin():
        return
    # NCI users don't need permissions on manifest uploads
    if user.is_nci_user() and template_type in prism.SUPPORTED_MANIFESTS:
        return
    if not perm:
        logger.error(
            f"Unauthorized attempt to access trial {trial_id} by {user.email!r}"
        )
        raise Unauthorized(
            f"{user.email} is not authorized to upload {template_type} data to {trial_id}. "
            f"Please contact a CIDC administrator if you believe this is a mistake."
        )


def log_multiple_errors(errors: list):
    if isinstance(errors, list):
        if errors != []:
            logger.error("\n".join(str(e) for e in errors))
    else:
        logger.error(errors)


def upload_handler(allowed_types: List[str]):
    """
    Extracts and validates the xlsx file from the request form body,
    prismifies the xlsx file, checks that the current user has
    permission to complete this upload, then passes relevant data
    along to `f` as positional arguments.

    If the request's schema type isn't in `allowed_types`, the request is rejected.

    This decorator factors out common code from `upload_manifest` and `upload_assay`.
    """

    def inner(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            logger.info(f"upload_handler({f.__name__}) started")
            template, xlsx_file = extract_schema_and_xlsx(allowed_types)

            errors_so_far = []

            try:
                xlsx, errors = XlTemplateReader.from_excel(xlsx_file)
            except SchemasValidationError as e:
                raise BadRequest({"errors": [str(e)]})
            logger.info(f"xlsx parsed: {len(errors)} errors")
            log_multiple_errors(errors)
            errors_so_far.extend(errors)

            # Run basic validations on the provided Excel file
            validations = validate(template, xlsx)
            logger.info(f"xlsx validated: {len(validations.json['errors'])} errors")
            log_multiple_errors(validations.json["errors"])
            errors_so_far.extend(validations.json["errors"])

            md_patch, file_infos, errors = prism.prismify(xlsx, template)
            logger.info(
                f"prismified: {len(errors)} errors, {len(file_infos)} file_infos"
            )
            log_multiple_errors(errors)
            errors_so_far.extend(errors)

            try:
                trial_id = md_patch[prism.PROTOCOL_ID_FIELD_NAME]
            except KeyError:
                errors_so_far.append(f"{prism.PROTOCOL_ID_FIELD_NAME} field not found.")
                # we can't find trial id so we can't proceed
                raise BadRequest({"errors": [str(e) for e in errors_so_far]})

            trial = TrialMetadata.find_by_trial_id(trial_id)
            if not trial:
                errors_so_far.insert(
                    0,
                    f"Trial with {prism.PROTOCOL_ID_FIELD_NAME}={trial_id!r} not found.",
                )
                # we can't find trial so we can't proceed trying to check_perm or merge
                raise BadRequest({"errors": [str(e) for e in errors_so_far]})

            user = get_current_user()
            try:
                check_permissions(user, trial_id, template.type)
            except Unauthorized as e:
                errors_so_far.insert(0, e.description)
                # unauthorized to pull trial so we can't proceed trying to merge
                raise Unauthorized({"errors": [str(e) for e in errors_so_far]})

            # Try to merge assay metadata into the existing clinical trial metadata
            # Ignoring result as we only want to check there's no validation errors
            try:
                merged_md, errors = prism.merge_clinical_trial_metadata(
                    md_patch, trial.metadata_json
                )
            except ValidationError as e:
                errors_so_far.append(json_validation.format_validation_error(e))
            except prism.MergeCollisionException as e:
                errors_so_far.append(str(e))
            except prism.InvalidMergeTargetException as e:
                # we have an invalid MD stored in db - users can't do anything about it.
                # So we log it
                logger.error(f"Internal error with trial {trial_id!r}\n{e}")
                # and return an error. Though it's not BadRequest but rather an
                # Internal Server error we report it like that, so it will be displayed
                raise BadRequest(
                    f"Internal error with {trial_id!r}. Please contact a CIDC Administrator."
                ) from e
            logger.info(f"merged: {len(errors)} errors")
            log_multiple_errors(errors)
            errors_so_far.extend(errors)

            if errors_so_far:
                raise BadRequest({"errors": [str(e) for e in errors_so_far]})

            return f(
                user,
                trial,
                template.type,
                xlsx_file,
                md_patch,
                file_infos,
                *args,
                **kwargs,
            )

        return wrapped

    return inner


@ingestion_bp.route("/validate", methods=["POST"])
@requires_auth(
    "ingestion/validate", [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]
)
@upload_handler(prism.SUPPORTED_TEMPLATES)
def validate_endpoint(*args, **kwargs):
    # Validation is done within `upload_handler`
    # so we just return ok here
    return jsonify({"errors": []})


@ingestion_bp.route("/upload_manifest", methods=["POST"])
@requires_auth(
    "ingestion/upload_manifest", [CIDCRole.ADMIN.value, CIDCRole.NCI_BIOBANK_USER.value]
)
@upload_handler(prism.SUPPORTED_MANIFESTS)
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
    * The merge request parsed JSON is saved to `UploadJobs`.
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
        raise BadRequest(json_validation.format_validation_error(e))
    except ValidationMultiError as e:
        raise BadRequest({"errors": e.args[0]})

    # TODO maybe rely on default session
    session = Session.object_session(trial)

    manifest_upload = UploadJobs.create(
        upload_type=template_type,
        uploader_email=user.email,
        metadata=md_patch,
        gcs_xlsx_uri="",  # not saving xlsx so we won't have phi-ish stuff in it
        gcs_file_map=None,
        session=session,
        send_email=True,
        status=UploadJobStatus.MERGE_COMPLETED.value,
    )

    # Publish that a manifest upload has been received
    gcloud_client.publish_patient_sample_update(manifest_upload.id)

    return jsonify({"metadata_json_patch": md_patch})


@ingestion_bp.route("/upload_assay", methods=["POST"])
@requires_auth(
    "ingestion/upload_assay", [CIDCRole.ADMIN.value, CIDCRole.CIMAC_BIOFX_USER.value]
)
@upload_handler(prism.SUPPORTED_ASSAYS)
def upload_assay(*args, **kwargs):
    """Handle assay metadata / file uploads."""
    return upload_data_files(*args, **kwargs)


@ingestion_bp.route("/upload_analysis", methods=["POST"])
@requires_auth(
    "ingestion/upload_analysis", [CIDCRole.ADMIN.value, CIDCRole.CIDC_BIOFX_USER.value]
)
@upload_handler(prism.SUPPORTED_ANALYSES)
def upload_analysis(*args, **kwargs):
    """Handle analysis metadata / file uploads."""
    return upload_data_files(*args, **kwargs)


def upload_data_files(
    user: Users,
    trial: TrialMetadata,
    template_type: str,
    xlsx_file: BinaryIO,
    md_patch: dict,
    file_infos: List[prism.LocalFileUploadEntry],
):
    """
    Initiate a data ingestion job.

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
        token: the unique token identifier for this upload job - possession of this token
            gives a user the right to update the corresponding upload job (no other authentication required).

    # TODO: refactor this to be a pre-GET hook on the upload-jobs resource.
    """
    logger.info(f"upload_assay started")
    upload_moment = datetime.datetime.now().isoformat()
    uri2uuid = {}
    url_mapping = {}
    optional_files = []
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

        if file_info.allow_empty:
            optional_files.append(file_info.local_path)

    gcs_blob = gcloud_client.upload_xlsx_to_gcs(
        trial.trial_id, "assays", template_type, xlsx_file, upload_moment
    )

    # Save the upload job to the database
    job = UploadJobs.create(
        template_type, user.email, uri2uuid, md_patch, gcs_blob.name
    )

    # Grant the user upload access to the upload bucket
    gcloud_client.grant_upload_access(user.email)

    response = {
        "job_id": job.id,
        "job_etag": job._etag,
        "url_mapping": url_mapping,
        "gcs_bucket": GOOGLE_UPLOAD_BUCKET,
        "extra_metadata": None,
        "gcs_file_map": uri2uuid,
        "optional_files": optional_files,
        "token": job.token,
    }
    if bool(files_with_extra_md):
        response["extra_metadata"] = files_with_extra_md

    return jsonify(response)


@ingestion_bp.route("/poll_upload_merge_status/<int:upload_job>", methods=["GET"])
@requires_upload_token_auth
def poll_upload_merge_status(upload_job: UploadJobs):
    """
    Check an assay upload's status, and supply the client with directions on when to retry the check.

    Response: application/json
        status {str or None}: the current status of the assay_upload (empty if not MERGE_FAILED or MERGE_COMPLETED)
        status_details {str or None}: information about `status` (e.g., error details). Only present if `status` is present.
        retry_in {str or None}: the time in seconds to wait before making another request to this endpoint (empty if `status` has a value)
    Raises:
        400: no "id" query parameter is supplied
        401: the requesting user did not create the requested upload job
        404: no upload job with id "id" is found
    """
    if upload_job.status in [
        UploadJobStatus.MERGE_COMPLETED.value,
        UploadJobStatus.MERGE_FAILED.value,
    ]:
        return jsonify(
            {"status": upload_job.status, "status_details": upload_job.status_details}
        )

    # TODO: get smarter about retry-scheduling
    return jsonify({"retry_in": 5})


@ingestion_bp.route("/extra-assay-metadata", methods=["POST"])
@requires_auth("extra_assay_metadata")
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
        UploadJobs.merge_extra_metadata(job_id, files)
    except ValueError as e:
        # thrown by parser itself if file cannot be parsed, e.g. wrong file uploaded
        # wrapped by merger to include uuid / assay_hint information, just use that message
        # thrown by UploadJobs.merge_extra_metadata if job_id doesn't exist or is already merged
        # thrown by getting artifact if uuid doesn't exist in the trial
        raise BadRequest(f"{e!s}")

    # Uncaught i.e. internal errors
    # TypeError thrown by parser itself if file is not the right type

    # TODO: return something here?
    return jsonify({})


INTAKE_ROLES = [
    CIDCRole.ADMIN.value,
    CIDCRole.CIDC_BIOFX_USER.value,
    CIDCRole.CIMAC_BIOFX_USER.value,
]


@ingestion_bp.route("/intake_bucket", methods=["POST"])
@requires_auth("intake_bucket", INTAKE_ROLES)
@use_args(
    {"trial_id": fields.Str(required=True), "upload_type": fields.Str(required=True)}
)
def create_intake_bucket(args):
    """
    Create an intake bucket for the current user if one doesn't exist yet, and return
    both the `gs://...` and web console URLs to the subdirectory in this bucket that we
    want the user to upload to (for the given trial / upload type combination).
    """
    user = get_current_user()
    intake_bucket = gcloud_client.create_intake_bucket(user.email)
    bucket_subdir = f'{intake_bucket.name}/{args["trial_id"]}/{args["upload_type"]}'
    gs_url = f"gs://{bucket_subdir}"
    console_url = f"https://console.cloud.google.com/storage/browser/{bucket_subdir}"

    return jsonify({"gs_url": gs_url, "console_url": console_url})


@ingestion_bp.route("/intake_metadata", methods=["POST"])
@requires_auth("intake_metadata", INTAKE_ROLES)
@use_args(
    {
        "trial_id": fields.Str(required=True),
        "assay_type": fields.Str(required=True),
        "description": fields.Str(required=True),
    },
    location="form",
)
@use_args(
    {
        "xlsx": fields.Field(
            required=True,
            # Check that this is an XLSX file
            validate=lambda file: file.mimetype
            == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            error_messages={"validator_failed": "must be a .xlsx file"},
        )
    },
    location="files",
)
def send_intake_metadata(form_args, file_args):
    """
    Send an email to the CIDC Admin mailing list with the provided metadata attached.
    """
    user = get_current_user()
    xlsx_gcp_url = gcloud_client.upload_xlsx_to_intake_bucket(
        user.email, form_args["trial_id"], form_args["assay_type"], file_args["xlsx"]
    )
    emails.intake_metadata(
        user, **form_args, xlsx_gcp_url=xlsx_gcp_url, send_email=True
    )
    return jsonify("ok")
