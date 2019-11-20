import os

import pytest
from werkzeug.exceptions import BadRequest, NotFound

INFO_ENDPOINT = "/info"


def test_info_assays(app_no_auth):
    """Check that the /info/assays endpoint returns a list of assays"""
    client = app_no_auth.test_client()
    res = client.get(f"{INFO_ENDPOINT}/assays")
    assert type(res.json) == list
    assert "wes" in res.json


def test_info_analyses(app_no_auth):
    """Check that the /info/analyses endpoint returns a list of assays"""
    client = app_no_auth.test_client()
    res = client.get(f"{INFO_ENDPOINT}/analyses")
    assert type(res.json) == list
    assert "cytof_analysis" in res.json


def test_info_manifests(app_no_auth):
    """Check that the /info/manifests endpoint returns a list of manifests"""
    client = app_no_auth.test_client()
    res = client.get(f"{INFO_ENDPOINT}/manifests")
    assert type(res.json) == list
    assert "pbmc" in res.json


def test_info_extra_types(app_no_auth):
    """Check that the /info/manifests endpoint returns a list of manifests"""
    client = app_no_auth.test_client()
    res = client.get(f"{INFO_ENDPOINT}/extra_data_types")
    assert type(res.json) == list
    assert "participants info" in res.json


def test_templates(app_no_auth):
    """Check that the /info/templates endpoint behaves as expected"""
    client = app_no_auth.test_client()

    # Invalid URLs
    res = client.get(f"{INFO_ENDPOINT}/templates/../pbmc")
    assert res.status_code == 400
    assert res.json["_error"]["message"] == "Invalid template family: .."

    res = client.get(f"{INFO_ENDPOINT}/templates/manifests/pbmc123")
    assert res.status_code == 404
    assert res.json["_error"]["message"] == "No template found for the given template family and template type"

    res = client.get(f"{INFO_ENDPOINT}/templates/manifests/pbmc123!")
    assert res.status_code == 400
    assert res.json["_error"]["message"] == "Invalid template type: pbmc123!"

    # Non-existent template
    res = client.get(f"{INFO_ENDPOINT}/templates/foo/bar")
    assert res.status_code == 404

    # Existing manifest
    pbmc_path = os.path.join(
        app_no_auth.config["TEMPLATES_DIR"], "manifests", "pbmc_template.xlsx"
    )
    with open(pbmc_path, "rb") as f:
        real_pbmc_file = f.read()
    res = client.get(f"{INFO_ENDPOINT}/templates/manifests/pbmc")
    assert res.status_code == 200
    assert res.data == real_pbmc_file

    # Existing assay
    olink_path = os.path.join(
        app_no_auth.config["TEMPLATES_DIR"], "metadata", "olink_template.xlsx"
    )
    with open(olink_path, "rb") as f:
        real_olink_file = f.read()
    res = client.get(f"{INFO_ENDPOINT}/templates/metadata/olink")
    assert res.status_code == 200
    assert res.data == real_olink_file
