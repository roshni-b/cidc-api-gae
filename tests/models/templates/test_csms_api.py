from cidc_api.models.templates.csms_api import (
    insert_manifest_from_json,
    insert_manifest_into_blob,
)

from tests.csms.data import samples, manifests


def test_insert_manifest_into_blob():
    """test that insertion of manifest into blob works as expected"""


def test_insert_manifest_from_json():
    """test that insertion of manifest from json works as expected"""


def end_to_end_csms_api():
    """end to end integration test that checks if
    1) creation, 2) updates, 3) getting summaries, and 4) adding new manifests
    works as expected"""
