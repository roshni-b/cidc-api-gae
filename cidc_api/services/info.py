"""Endpoints providing info related to this API"""
import os

from flask import Blueprint, jsonify, current_app as app, send_file
from werkzeug.exceptions import NotFound, BadRequest

from cidc_schemas import prism

from models import EXTRA_DATA_TYPES

info_api = Blueprint("info", __name__, url_prefix="/info")


@info_api.route("assays", methods=["GET"])
def assays():
    """List all supported assays"""
    return jsonify(prism.SUPPORTED_ASSAYS)


@info_api.route("analyses", methods=["GET"])
def analyses():
    """List all supported analyses"""
    return jsonify(prism.SUPPORTED_ANALYSES)


@info_api.route("manifests", methods=["GET"])
def manifests():
    """List all supported manifests"""
    return jsonify(prism.SUPPORTED_MANIFESTS)


@info_api.route("extra_data_types", methods=["GET"])
def extra_data_types():
    """List all extra data types on which permissions can be granted"""
    return jsonify(EXTRA_DATA_TYPES)


@info_api.route("templates/<template_family>/<template_type>", methods=["GET"])
def templates(template_family, template_type):
    """
    Return the empty Excel template file for the given 
    `template_family` (e.g., manifests, metadata) and 
    `template_type` (e.g., pbmc, olink).
    """
    # Check that both strings are alphabetic
    if not template_family.isalpha():
        raise BadRequest(f"Invalid template family: {template_family}")
    elif not template_type.isalpha():
        raise BadRequest(f"Invalid template type: {template_type}")

    filename = f"{template_type}_template.xlsx"
    path = os.path.join(app.config["TEMPLATES_DIR"], template_family, filename)

    # Check that the template exists
    if not os.path.exists(path):
        raise NotFound(
            f"No template found for the given template family and template type"
        )

    return send_file(path, as_attachment=True, attachment_filename=filename)
