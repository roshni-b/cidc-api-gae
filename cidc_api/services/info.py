"""Endpoints providing info related to this API"""
from flask import Blueprint, jsonify

from cidc_schemas.template import _TEMPLATE_PATH_MAP
from cidc_schemas import prism


info_api = Blueprint("info", __name__, url_prefix="/info")


@info_api.route("assays", methods=["GET"])
def assays():
    """List all supported assays"""
    return jsonify(SUPPORTED_ASSAYS)


@info_api.route("manifests", methods=["GET"])
def manifests():
    """List all supported manifests"""
    return jsonify(SUPPORTED_MANIFESTS)
