"""Endpoints providing info related to this API"""
from flask import Blueprint, jsonify

from settings import SUPPORTED_ASSAYS

info_api = Blueprint("info", __name__, url_prefix="/info")


@info_api.route("assays", methods=["GET"])
def assays():
    """List all supported assays"""
    return jsonify(SUPPORTED_ASSAYS)
