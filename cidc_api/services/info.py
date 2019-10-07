"""Endpoints providing info related to this API"""
from flask import Blueprint, jsonify

from cidc_schemas import prism

from models import EXTRA_DATA_TYPES

info_api = Blueprint("info", __name__, url_prefix="/info")


@info_api.route("assays", methods=["GET"])
def assays():
    """List all supported assays"""
    return jsonify(prism.SUPPORTED_ASSAYS)


@info_api.route("manifests", methods=["GET"])
def manifests():
    """List all supported manifests"""
    return jsonify(prism.SUPPORTED_MANIFESTS)


@info_api.route("extra_data_types", methods=["GET"])
def extra_data_types():
    """List all extra data types on which permissions can be granted"""
    return jsonify(EXTRA_DATA_TYPES)
