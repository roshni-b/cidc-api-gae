from unittest.mock import MagicMock
from cidc_api.services.files import insert_download_url, insert_download_urls

FILE = {"object_url": "1"}
URL = "foo"


def test_insert_download_url(monkeypatch):
    """
    Test that we try to generate a signed download URL for the file in the payload.
    """
    get_signed_url = lambda url: URL
    monkeypatch.setattr("gcloud_client.get_signed_url", get_signed_url)

    f = FILE.copy()

    insert_download_url(f)
    assert f["download_link"] == URL


def test_insert_download_urls(monkeypatch):
    """
    Test that we try to generate a signed download URL for every file in the payload.
    """

    def insert_signed_url(f):
        f["download_link"] = URL

    monkeypatch.setattr(
        "cidc_api.services.files.insert_download_url", insert_signed_url
    )

    f1 = FILE.copy()
    f2 = FILE.copy()
    f2["object_url"] = "2"
    payload = {"_items": [f1, f2]}

    insert_download_urls(payload)
    for f in payload["_items"]:
        assert f["download_link"] == URL
