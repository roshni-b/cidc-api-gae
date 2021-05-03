"""Endpoints providing info related to this API"""
import os
import re

from flask import Blueprint, jsonify, current_app as app, send_file
from werkzeug.exceptions import NotFound, BadRequest

from cidc_schemas import prism, template

from ..shared.auth import public
from ..models import TrialMetadata, DownloadableFiles, EXTRA_DATA_TYPES

info_bp = Blueprint("info", __name__)


@info_bp.route("assays", methods=["GET"])
@public
def assays():
    """List all supported assays"""
    return jsonify(prism.SUPPORTED_ASSAYS)


@info_bp.route("analyses", methods=["GET"])
@public
def analyses():
    """List all supported analyses"""
    return jsonify(prism.SUPPORTED_ANALYSES)


@info_bp.route("manifests", methods=["GET"])
@public
def manifests():
    """List all supported manifests"""
    return jsonify(prism.SUPPORTED_MANIFESTS)


@info_bp.route("extra_data_types", methods=["GET"])
@public
def extra_data_types():
    """List all extra data types on which permissions can be granted"""
    return jsonify(EXTRA_DATA_TYPES)


@info_bp.route("data_overview", methods=["GET"])
@public
def data_overview():
    """Return an overview of data ingested into the system"""
    metadata_counts = TrialMetadata.get_metadata_counts()
    num_files = DownloadableFiles.count()
    num_bytes = DownloadableFiles.get_total_bytes()
    return jsonify(
        {
            **metadata_counts,
            "num_files": num_files,
            "num_bytes": num_bytes,
            "num_assays": len(prism.SUPPORTED_ASSAYS),
        }
    )


_al_under = re.compile("^\w+$")  # alpha or underscore


@info_bp.route("templates/<template_family>/<template_type>", methods=["GET"])
@public
def templates(template_family, template_type):
    """
    Return the empty Excel template file for the given 
    `template_family` (e.g., manifests, metadata) and 
    `template_type` (e.g., pbmc, olink).
    """
    # Check that both strings are alphabetic
    if not re.match(_al_under, template_family):
        raise BadRequest(f"Invalid template family: {template_family}")
    elif not re.match(_al_under, template_type):
        raise BadRequest(f"Invalid template type: {template_type}")

    schema_path = os.path.join(
        "templates", template_family, f"{template_type}_template.json"
    )
    template_filename = f"{template_type}_template.xlsx"
    template_path = os.path.join(
        app.config["TEMPLATES_DIR"], template_family, template_filename
    )

    # Generate the empty template if it doesn't exist yet
    if not os.path.exists(template_path):
        try:
            template.generate_empty_template(schema_path, template_path)
        except FileNotFoundError:
            raise NotFound(
                f"No template found for the given template family and template type"
            )

    return send_file(
        template_path, as_attachment=True, attachment_filename=template_filename
    )
