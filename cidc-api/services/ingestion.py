"""
Endpoints for validating and ingesting metadata and data.
"""
import json
from typing import BinaryIO, Tuple

from werkzeug.exceptions import BadRequest, InternalServerError, NotImplemented

from eve.auth import requires_auth
from flask import Blueprint, request, jsonify
from cidc_schemas import constants, validate_xlsx

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
    raise NotImplemented()
