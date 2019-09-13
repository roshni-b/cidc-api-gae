"""Endpoints providing info related to this API"""
from flask import Blueprint, jsonify

from cidc_schemas.template import _TEMPLATE_PATH_MAP
from cidc_schemas import prism


info_api = Blueprint("info", __name__, url_prefix="/info")

assay_list = [
    k
    for k, v in _TEMPLATE_PATH_MAP.items()
    if "metadata" in v and k in prism.SUPPORTED_ASSAYS
]
manifest_list = [
    k
    for k, v in _TEMPLATE_PATH_MAP.items()
    if "manifests" in v and k in prism.SUPPORTED_MANIFESTS
]


@info_api.route("assays", methods=["GET"])
def assays():
    """List all supported assays"""
    return jsonify(assay_list)


@info_api.route("manifests", methods=["GET"])
def manifests():
    """List all supported manifests"""
    return jsonify(manifest_list)
