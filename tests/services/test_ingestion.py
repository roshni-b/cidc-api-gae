import io
from unittest.mock import MagicMock

import pytest
from werkzeug.exceptions import (
    HTTPException,
    InternalServerError,
    BadRequest,
    NotImplemented,
)

from services.ingestion import extract_schema_and_xlsx

from . import open_data_file
from ..util import assert_same_elements


@pytest.fixture
def valid_xlsx():
    with open_data_file("pbmc_valid.xlsx") as xlsx:
        yield xlsx


@pytest.fixture
def invalid_xlsx():
    yield open_data_file("pbmc_invalid.xlsx")


def form_data(filename=None, fp=None):
    """
    If no filename is provided, return some text form data.
    If a filename is provided but no opened file (`fp`) is provided,
    return form data with a mock file included.
    If a filename and an opened file is provided, return
    form data with the provided file included.
    """
    data = {"foo": "bar"}
    if filename:
        fp = fp or io.BytesIO(b"blah blah")
        data["template"] = (fp, filename)
    return data


VALIDATE = "/ingestion/validate"


def test_validate_valid_template(app_no_auth, valid_xlsx):
    """Ensure that the validation endpoint returns no errors for a known-valid .xlsx file"""
    client = app_no_auth.test_client()
    data = form_data("pbmc.xlsx", valid_xlsx)
    res = client.post(f"{VALIDATE}?schema=templates/pbmc_template.json", data=data)
    assert res.status_code == 200
    assert res.json["errors"] == []


def test_validate_invalid_template(app_no_auth, invalid_xlsx):
    """Ensure that the validation endpoint returns errors for a known-invalid .xlsx file"""
    client = app_no_auth.test_client()
    data = form_data("pbmc.xlsx", invalid_xlsx)
    res = client.post(f"{VALIDATE}?schema=templates/pbmc_template.json", data=data)
    assert res.status_code == 200
    assert len(res.json["errors"]) > 0


@pytest.mark.parametrize(
    "url,data,error,message",
    [
        # Missing form content
        [VALIDATE, None, BadRequest, "form content"],
        # Form missing template file
        [VALIDATE, form_data(), BadRequest, "template file"],
        # Template file is non-.xlsx
        [VALIDATE, form_data("text.txt"), BadRequest, ".xlsx file"],
        # URL is missing "schema" query param
        [VALIDATE, form_data("text.xlsx"), BadRequest, "query param 'schema'"],
        # "schema" query param references non-existent schema
        [
            f"{VALIDATE}?schema=foo/bar",
            form_data("test.xlsx"),
            BadRequest,
            "schema with id foo/bar",
        ],
    ],
)
def test_extract_schema_and_xlsx_failures(app, url, data, error, message):
    """
    Test that we get the expected errors when trying to extract 
    schema/template from a malformed request.
    """
    with app.test_request_context(url, data=data):
        with pytest.raises(error, match=message):
            extract_schema_and_xlsx()


def test_upload_not_implemented(app_no_auth):
    """Ensure the upload endpoint returns a not implemented error"""
    client = app_no_auth.test_client()
    res = client.post("/ingestion/upload")
    assert res.status_code == NotImplemented.code


def test_signed_upload_urls(app_no_auth, monkeypatch):
    """
    Ensure the signed upload urls endpoint responds with the expected structure
    
    TODO: an integration test that actually calls out to GCS
    """
    client = app_no_auth.test_client()
    data = {
        "directory_name": "my-assay-run-id",
        "object_names": ["my-fastq-1.fastq.gz", "my-fastq-2.fastq.gz"],
    }

    monkeypatch.setattr("google.cloud.storage.Client", MagicMock)
    res = client.post("/ingestion/signed-upload-urls", json=data)

    assert_same_elements(res.json.keys(), data["object_names"])
