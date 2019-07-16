"""
Endpoints for validating and ingesting metadata and data.
"""
import json
import datetime
from typing import BinaryIO, Tuple

from werkzeug.exceptions import BadRequest, InternalServerError, NotImplemented

from google.cloud import storage
from eve.auth import requires_auth
from flask import Blueprint, request, jsonify
from cidc_schemas import constants, validate_xlsx

from settings import GOOGLE_UPLOAD_BUCKET

ingestion_api = Blueprint("ingestion", __name__, url_prefix="/ingestion")


def is_xlsx(filename: str) -> bool:
    """Checks if a filename suggests a file is an .xlsx file"""
    return filename.endswith(".xlsx")


def extract_schema_and_xlsx() -> Tuple[str, BinaryIO]:
    """
    Validate that a request has the required structure, then extract 
    the schema id and template file from the request. The request URL
    must have a query parameter "schema" referencing a valid schema id
    and request body with content-type multipart/form containing a data entry
    "template" with an attached .xlsx file.

    Raises:
        BadRequest: if the above requirements aren't satisfied

    Returns:
        Tuple[str, str]: the requested schema identifier and a path to a tempfile containing the xlsx template
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
    if not is_xlsx(xlsx_file.filename):
        raise BadRequest("Expected a .xlsx file")

    # Check that a schema id was provided and that a corresponding schema exists
    schema_id = request.args.get("schema")
    if not schema_id:
        raise BadRequest("Expected a value for URL query param 'schema'")
    if schema_id not in constants.SCHEMA_LIST:
        raise BadRequest(f"No known schema with id {schema_id}")

    return schema_id, xlsx_file


@ingestion_api.route("/validate", methods=["POST"])
@requires_auth("ingestion.validate")
def validate():
    """
    Validate a .xlsx manifest or assay metadata template.

    TODO: add this endpoint to the OpenAPI docs
    """
    # Extract info from the request context
    schema_path, template_file = extract_schema_and_xlsx()

    # Validate the .xlsx file with respect to the schema
    try:
        error_list = validate_xlsx(template_file, schema_path, False)
    except Exception as e:
        # TODO: log the traceback for this error
        raise InternalServerError(str(e))

    return jsonify({"errors": [] if type(error_list) == bool else error_list})


@ingestion_api.route("/upload", methods=["POST"])
@requires_auth("ingestion.upload")
def upload():
    """Ingest the metadata associated with a completed upload."""
    raise NotImplemented()


@ingestion_api.route("/signed-upload-urls", methods=["POST"])
@requires_auth("ingestion.signed-upload-urls")
def signed_upload_urls():
    """
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
